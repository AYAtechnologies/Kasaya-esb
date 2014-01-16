#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.conf import settings
from kasaya.core.protocol import messages
from kasaya.core import exceptions
#from kasaya.core.lib.binder import get_bind_address
from kasaya.core.protocol.comm import MessageLoop, send_and_receive_response
from kasaya.core.lib.control_tasks import ControlTasks, RedirectRequiredToAddr
from kasaya.core.lib import LOG, servicesctl
from kasaya.core.events import add_event_handler, emit
#from kasaya.workers.kasayad.pong import PingDB
from datetime import datetime, timedelta
import gevent

from signal import SIGKILL, SIGTERM
from os import kill
import random


__all__=("SyncWorker",)


def _worker_addr( wrkr ):
    return "tcp://%s:%i" % (wrkr['ip'],wrkr['port'])

class RedirectRequiredEx(RedirectRequiredToAddr):
    def __init__(self, host_id):
        self.remote_id = host_id



class SyncWorker(object):

    def __init__(self, server, database):
        self.DAEMON = server
        self.DB = database
        #self.BC = broadcaster
        #self.pinger = PingDB()

        # bind events
        add_event_handler("worker-local-start", self.worker_start_local )
        add_event_handler("worker-local-stop", self.worker_stop_local )
        add_event_handler("worker-local-wait", self.worker_prepare )
        add_event_handler("worker-remote-join", self.worker_start_remote )
        add_event_handler("worker-remote-leave", self.worker_stop_remote )
        add_event_handler("connection-end", self.handle_connection_end )

        # cache
        self.__services = None

        # kasayad <--> kasayad communication
        self.intersync = MessageLoop( 'tcp://0.0.0.0:'+str(settings.KASAYAD_CONTROL_PORT) )
        self.intersync.register_message(messages.CTL_CALL, self.handle_global_control_request)
        # local worker <-> kasayad dialog on public port
        self.intersync.register_message(messages.WORKER_LIVE, self.handle_worker_live)
        self.intersync.register_message(messages.WORKER_LEAVE, self.handle_worker_leave)
        self.intersync.register_message(messages.QUERY, self.handle_name_query, raw_msg_response=True)
        self.intersync.register_message(messages.QUERY_MULTI, self.handle_name_query_multi, raw_msg_response=True)
        # service control tasks
        self.ctl = ControlTasks(allow_redirect=True)
        self.ctl.register_task("svbus.status",  self.CTL_global_services)
        self.ctl.register_task("worker.stop",   self.CTL_worker_stop)
        self.ctl.register_task("worker.stats",  self.CTL_worker_stats)
        self.ctl.register_task("worker.exists",  self.CTL_worker_exists)
        self.ctl.register_task("service.start", self.CTL_service_start)
        self.ctl.register_task("service.stop",  self.CTL_service_stop)
        self.ctl.register_task("host.rescan",   self.CTL_host_rescan)



    @property
    def replaces_broadcast(self):
        return self.DB.replaces_broadcast

    @property
    def own_ip(self):
        return self.intersync.ip

    @property
    def own_addr(self):
        """
        Own network address
        """
        return self.intersync.address


    # closing and quitting

    def stop(self):
        #self.local_input.stop()
        #self.queries.stop()
        pass

    def close(self):
        #self.local_input.close()
        #self.queries.close()
        pass

    # all message loops used in kasayad
    def get_loops(self):
        return [
            #self.pinger.loop,
            self.intersync.loop,
        ]


    # local services management
    # -------------------------


    #def local_services_scan(self):



    def local_services_list(self, rescan=False):
        """
        List of available local services.
        If rescan bring changes, then database and broadcast will be triggered.
        """
        scan = rescan or (self.__services is None)
        if scan:
            self.__services = servicesctl.local_services()
        lst = self.__services.keys()
        if rescan:
            ID = self.DAEMON.ID
            changes = self.DB.service_update_list(ID, lst)
            if changes:
                self.DAEMON.notify_kasayad_refresh(ID, lst, local=True)
        return lst


    def get_service_ctl(self, name):
        """
        Return ServiceCtl object for given service name
        """
        return self.__services[name]


    # local message handlers
    # -----------------------------------

    def handle_worker_live(self, msg):
        """
        Receive worker's ping singnal.
        This function is triggered only by local worker.
        """
        {
            u'status': 1,
            u'addr': u'tcp://0.0.0.0:5000',
            u'service': u'locka',
            u'pid': 6222,
            u'id': u'WLUKJGE5AOKF4E'
        }

        wrkr = self.DB.worker_get(msg['id'])

        if wrkr is None:
            # new local worker just started
            emit("worker-local-start", msg['id'], msg['addr'], msg['service'], msg['pid'] )
        return

        if (msg['addr']!=wrkr['addr']) or (msg['service']!=wrkr['service']):
            # worker properties are different, assume that
            # old worker died silently and new appears under same ID
            # (it's just impossible!)
            emit("worker-local-stop", worker_id )
            return

    def handle_connection_end(self, addr, ssid):
        """
        This is event handler for connection-end.
        """
        if ssid==None: return
        # unexpected connection lost with local worker
        wrkr = self.DB.worker_get(ssid)
        if wrkr is not None:
            emit("worker-local-stop", ssid )

    def handle_worker_leave(self, msg):
        """
        Local worker is going down,
        generate event worker-local-stop
        """
        emit("worker-local-stop", msg['id'] )

    def handle_name_query(self, msg):
        """
        Odpowiedź na pytanie o adres workera
        """
        name = msg['service']
        addr = self.DB.choose_worker_for_service(name)
        if not addr is None:
            addr = addr['addr']
        return {
            'message':messages.WORKER_ADDR,
            'service':name,
            'addr':addr,
        }

    def handle_name_query_multi(self, msg):
        """
        Send all workers for given service
        """
        name = msg['service']
        addrlst = self.DB.list_workers_for_service(name)
        if not addrlst is None:
            addrlst = [ a['addr'] for a in addrlst ]
        return {
            'message':messages.WORKER_ADDR,
            'service':name,
            'addrlst': addrlst,
            'timeout':10,
        }

    def handle_local_control_request(self, msg):
        """
        control requests from localhost
        """
        result = self.ctl.handle_request(msg)
        return result



    # worker state changes, high level functions
    # ------------------------------------------

    def worker_prepare(self, worker_id):
        """
        After start, worker is in offline state.
        It need to be configured and after then it can be activated to be online.
        This function make all required things and when worker is online it broadcast new worker in network.
        """
        wrknfo = self.DB.worker_get(worker_id)

        # all configuration of worker should be there
        pass
        # send information to worker to start processing tasks
        msg = {
            'message':messages.CTL_CALL,
            'method':'start'
        }
        res = send_and_receive_response(wrknfo['addr'], msg)
        LOG.debug("Local worker [%s] on [%s] is now online" % (wrknfo['service'], wrknfo['addr']) )
        # broadcast new worker state
        self.DB.worker_set_state( worker_id, True )
        #self.BC.broadcast_worker_live(self.DAEMON.ID, worker_id, wrknfo['addr'], wrknfo['service'])


    def worker_start_local(self, worker_id, address, service, pid):
        """
        Local worker started
        """
        self.DB.worker_register(self.DAEMON.ID, worker_id, service, address, pid, False)
        LOG.info("Local worker [%s] started, address [%s] [id:%s]" % (service, address, worker_id) )
        # emit signal
        emit("worker-local-wait", worker_id)

    def worker_start_remote(self, worker_id, host_id, address, service):
        """
        Remote worker started
        """
        self.DB.worker_register(host_id, worker_id, service, address)
        LOG.info("Remote worker [%s] started, address [%s] [id:%s]" % (service, address, worker_id) )

    def worker_stop_local(self, worker_id):
        """
        Local worker stopped
        """
        self.DB.worker_unregister(ID=worker_id)
        LOG.info("Local worker stopped [id:%s]" % worker_id )
        #self.BC.broadcast_worker_stop(worker_id)

    def worker_stop_remote(self, worker_id):
        """
        Remote worker stopped
        """
        self.DB.worker_unregister(ID=worker_id)
        LOG.info("Remote worker stopped [id:%s]" % worker_id )



    # heartbeat
    def hearbeat_loop(self):
        """
        Periodically check all locally registered workers ping time. Unregister dead workers
        """
        maxpinglife = timedelta( seconds = settings.HEARTBEAT_TIMEOUT + settings.WORKER_HEARTBEAT )
        unreglist = []
        while True:

            now = datetime.now()
            for ID, nfo in self.__pingdb.iteritems():
                # find outdated timeouts
                to = nfo['t'] + maxpinglife
                if to<now:
                    LOG.warning("Worker [%s] with id [%s] died. Unregistering." % (nfo['s'], ID) )
                    unreglist.append(ID)

            # unregister all dead workers
            while len(unreglist)>0:
                ID = unreglist.pop()
                self.worker_stop( ID )

            gevent.sleep(settings.WORKER_HEARTBEAT)



    # inter sync communication and global management
    # -------------------------------------------------


    def redirect_or_pass_by_id(self, host_id):
        """
        Check if given host id is own host, if not then raise exception
        to redirect message to proper destination.
        """
        if host_id is None:
            raise exceptions.ServiceBusException("Unknown address")
        ownip = host_id==self.DAEMON.ID
        if not ownip:
            raise RedirectRequiredEx(host_id)


    def handle_global_control_request(self, msg):
        """
        Control requests from remote hosts
        """
        result = self.ctl.handle_request(msg)
        return result
        #return {"message":messages.RESULT, "result":result }



    # kasayad host tasks


    def CTL_global_services(self):
        """
        List of all working hosts and services in network
        """
        lst = []
        # all kasayad hosts
        for hst in self.DB.host_list():
            # workers on host
            hst ['services'] = self.CTL_services_on_host( hst['id'] )
            lst.append( hst )
        return lst


    # this command is not currently exposed via control interface
    def CTL_services_on_host(self, host_id):
        """
        List of all services on host
        """
        lst = []

        # managed services set
        managed = set()
        for s in self.DB.service_list(host_id):
            managed.add(s['service'])

        # currently running services
        running = set()
        for wnfo in self.DB.worker_list(host_id):
            running.add(wnfo['service'])
            wnfo['running'] = True
            wnfo['managed'] = wnfo['service'] in managed
            lst.append(wnfo)

        # offline services
        for sv in managed:
            if not sv in running:
                lst.append( {'service':sv,'running':False, 'managed':True} )
        return lst


    def CTL_worker_exists(self, worker_id):
        """
        Check if worker with given id is existing
        """
        wrkr = self.DB.worker_get(worker_id)
        return not wrkr is None


    def CTL_worker_stop(self, ID, terminate=False, sigkill=False):
        """
        Send stop signal to worker
        """
        wrkr = self.DB.worker_get(ID)
        self.redirect_or_pass_by_id( wrkr['ip'] )

        if terminate:
            signal = SIGTERM
            if sigkill:
                signal = SIGKILL

            kill(pid, signal)
            return True
        else:
            addr = _worker_addr(wrkr)
            msg = {
                'message':messages.CTL_CALL,
                'method':'stop'
            }
            res = send_and_receive_response(self.context, addr, msg)
            return res


    def CTL_worker_stats(self, ID):
        """
        Return full stats of worker
        """
        wrkr = self.DB.worker_get(ID)
        self.redirect_or_pass_by_id( wrkr['ip'] )
        # call worker for stats
        addr = _worker_addr(wrkr)
        msg = {
            'message':messages.CTL_CALL,
            'method':'stats'
        }
        res = send_and_receive_response(self.context, addr, msg)
        return res


    def CTL_service_start(self, name, ip=None):
        """
        Start service on host, or locally if host is not given.
        name - name of service to start
        ip - ip address of host on which service shoult be started,
             if not given then worker will be started on localhost.
        """
        if ip is None:
            ip = self.own_ip
        self.redirect_or_pass_by_id(ip)

        try:
            svc = self.get_service_ctl(name)
        except KeyError:
            raise exceptions.ServiceBusException("There is no service [%s] on this host" % name)

        svc.start_service()
        return True


    def CTL_service_stop(self, name, ip=None):
        """
        Stop all workers serving given service.
        name - name of service to stop
        """
        if ip is None:
            ip = self.own_ip
        self.redirect_or_pass_by_id(ip)

        services = []
        for wrk in self.DB.worker_list_local():
            if wrk['service']==name:
                services.append(wrk['ID'])
        if len(services)==0:
            raise exceptions.ServiceBusException("There is no [%s] service running" % name)
        for u in services:
            self.CTL_worker_stop(u)


    def CTL_host_rescan(self, ip=None):
        """
        Rescan services available on local host
        """
        if ip is None:
            ip = self.own_ip
        self.redirect_or_pass_by_id(ip)
        svlist = self.local_services_list(rescan=True)
        # send nwe list of services to kasaya daemon instance
        #self.DAEMON.notify_kasayad_refresh(self.DAEMON.ID, svlist, True)
