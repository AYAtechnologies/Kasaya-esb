#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.conf import settings
from kasaya.core.protocol import messages
from kasaya.core import exceptions
from kasaya.core.lib.binder import get_bind_address
from kasaya.core.lib.comm import RepLoop, send_and_receive_response
from kasaya.core.lib.control_tasks import ControlTasks, RedirectRequiredToAddr
from kasaya.core.lib import LOG, servicesctl
from datetime import datetime, timedelta
import zmq.green as zmq
import gevent

from signal import SIGKILL, SIGTERM
from os import kill
import random


__all__=("SyncWorker",)


def _ip_to_sync_addr(ip):
    return "tcp://%s:%i" % (ip, settings.KASAYAD_CONTROL_PORT)

def _worker_addr( wrkr ):
    return "tcp://%s:%i" % (wrkr['ip'],wrkr['port'])

class RedirectRequiredEx(RedirectRequiredToAddr):
    def __init__(self, ip):
        self.remote_addr = _ip_to_sync_addr(ip)



class SyncWorker(object):

    def __init__(self, server, database, broadcaster):
        self.DAEMON = server
        self.DB = database
        self.BC = broadcaster
        self.context = zmq.Context()
        self.__pingdb = {}
        # cache
        self.__services = None
        # local workers <--> local kasayad communication
        self.queries = RepLoop(self._connect_queries_loop, context=self.context)
        self.queries.register_message(messages.WORKER_LIVE, self.handle_worker_live)
        self.queries.register_message(messages.WORKER_LEAVE, self.handle_worker_leave)
        self.queries.register_message(messages.QUERY, self.handle_name_query, raw_msg_response=True)
        self.queries.register_message(messages.CTL_CALL, self.handle_local_control_request)
        #self.queries.register_message(messages.HOST_REFRESH, self.handle_host)
        # kasayad <--> kasayad communication
        self.intersync = RepLoop(self._connect_inter_sync_loop, context=self.context)
        self.intersync.register_message(messages.CTL_CALL, self.handle_global_control_request)
        # service control tasks
        self.ctl = ControlTasks(self.context, allow_redirect=True)
        self.ctl.register_task("svbus.status",  self.CTL_global_services)
        self.ctl.register_task("worker.stop",   self.CTL_worker_stop)
        self.ctl.register_task("worker.stats",  self.CTL_worker_stats)
        self.ctl.register_task("service.start", self.CTL_service_start)
        self.ctl.register_task("service.stop",  self.CTL_service_stop)
        self.ctl.register_task("host.rescan",   self.CTL_host_rescan)


    @property
    def replaces_broadcast(self):
        return self.DB.replaces_broadcast

    @property
    def own_ip(self):
        return self.intersync.ip

    def get_sync_address(self, ip):
        return "tcp://%s:%i" % (ip, settings.SYNCD_CONTROL_PORT)


    # socket connectors

    def _connect_queries_loop(self, context):
        """
        connect local queries loop
        """
        sock = context.socket(zmq.REP)
        addr = 'ipc://'+settings.SOCK_QUERIES
        sock.bind(addr)
        LOG.debug("Connected query socket %s" % addr)
        return sock, addr

    def _connect_inter_sync_loop(self, context):
        """
        connext global network syncd dialog
        """
        sock = context.socket(zmq.REP)
        addr = _ip_to_sync_addr( get_bind_address(settings.SYNCD_CONTROL_BIND) )
        sock.bind(addr)
        LOG.debug("Connected inter-kasaya dialog socket %s" % addr)
        return sock, addr


    # closing and quitting

    def stop(self):
        #self.local_input.stop()
        self.queries.stop()

    def close(self):
        #self.local_input.close()
        self.queries.close()

    # all message loops used in syncd
    def get_loops(self):
        return [
            self.queries.loop,
            self.hearbeat_loop,
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
            uuid = self.DAEMON.uuid
            changes = self.DB.service_update_list(uuid, lst)
            if changes:
                self.DAEMON.notify_kasayad_refresh(uuid, lst, local=True)
        return lst


#    def local_services_stats(self):
#        """
#        List of all services on localhost including inactive.
#        result is dict, key is service name, value is number
#        of workers currently running for this service.
#        """
#        svlist = self.local_services_list()
#        print ("svlist",svlist)
#        result = {}
#        # list of running workers
#        for wrk in self.DB.get_local_workers():
#            name = wrk[1]
#            if not name in result:
#                result[name] = {'count':0,'uuid':[]}
#            result[name]['count'] += 1
#            result[name]['uuid'].append(wrk[0])
#        # add inactive services
#        for svc in self.__services:
#            if svc in result:
#                continue
#            result[svc] = {'count':0,'uuid':[]}
#        return result


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
        now = datetime.now()
        uuid = msg['uuid']
        ip = msg['addr']
        port = msg['port']
        svce = msg['service']
        pid = msg['pid']
        addr = ip+"."+str(port)
        try:
            png = self.__pingdb[addr]
        except KeyError:
            # worker is unknown
            self.worker_start(uuid, svce, ip, port, pid)
            return

        if png['s']!=svce:
            # different service under same address!
            LOG.error("Service [%s] expected on address [%s], but [%s] appeared. Removing old, registering new." % (png['s'], addr, svce))
            self.worker_stop(ip, port, True)
            self.worker_start(uuid, svce, ip, port, pid)
            return

        # update heartbeats
        png['t'] = now
        self.__pingdb[addr] = png


    def handle_worker_leave(self, msg):
        """
        Worker is going down
        """
        self.worker_stop( msg['ip'], msg['port'] )

    def handle_name_query(self, msg):
        """
        Odpowiedź na pytanie o adres workera
        """
        name = msg['service']
        res = self.DB.choose_worker_for_service(name)
        if res is None:
            a = None
            p = None
        else:
            a = res['ip']
            p = res['port']
        return {
            'message':messages.WORKER_ADDR,
            'service':name,
            'ip':a,
            'port':p
        }

    def handle_local_control_request(self, msg):
        """
        control requests from localhost
        """
        result = self.ctl.handle_request(msg)
        return result



    # worker state changes
    # ------------------------------

    def worker_start(self, uuid, service, ip, port, pid=0):
        """
        Handle joining to network by worker (local or blobal)
        """
        succ = self.DB.worker_register(uuid, service, ip, port,pid)
        addr = ip+":"+str(port)
        local = ip==self.own_ip
        if succ:
            if local:
                LOG.info("Local worker [%s] started, address [%s]" % (service, addr) )
            else:
                LOG.info("Remote worker [%s] started, address [%s]" % (service, addr) )
        if local:
            # dodanie serwisu do bazy z czasami pingów
            self.__pingdb[addr] =  {
                's' : service,
                't' : datetime.now(),
            }

        # rozesłanie informacji w sieci jeśli nastąpi lokalna zmiana stanu
        if succ and local:
            self.BC.send_worker_live(uuid, service, ip,port)


    def worker_stop(self, ip, port):
        """
        Handle worker leaving network (local or global)
        """
        self.DB.worker_unregister(ip,port)
        addr = ip+":"+str(port)
        local = ip==self.own_ip
        if local:
            LOG.info("Local worker stopped, address [%s]" % addr )
            # usunięcie wpisu z listy pingów
            try:
                del self.__pingdb[addr]
            except KeyError:
                pass
            # send information to all network about stopping worker
            self.BC.send_worker_stop(ip,port)
        else:
            LOG.info("Remote worker stopped, address [%s]" % addr )


    def request_workers_broadcast(self):
        """
        Send to all local workers request for registering in network.
        Its used after new host start.
        """
        for w in self.DB.worker_list_local():
            gevent.sleep(0.4)
            self.BC.send_worker_live(w['uuid'], w['service'], w['ip'], w['port'])


    # heartbeat
    def hearbeat_loop(self):
        maxpinglife = timedelta( seconds = settings.HEARTBEAT_TIMEOUT + settings.WORKER_HEARTBEAT )
        unreglist = []
        while True:
            now = datetime.now()
            for addr, nfo in self.__pingdb.iteritems():
                # find outdated timeouts
                to = nfo['t'] + maxpinglife
                if to<now:
                    LOG.warning("Worker [%s] died on address [%s]. Unregistering." % (nfo['s'], addr) )
                    unreglist.append(addr)
            # unregister all dead workers
            while len(unreglist)>0:
                ip,port = unreglist.pop().split(":")
                self.worker_stop(ip,int(port))

            gevent.sleep(settings.WORKER_HEARTBEAT)



    # inter sync communication and global management
    # -------------------------------------------------


    def redirect_or_pass_by_ip(self, ip):
        """
        Check if given IP is own host ip, if not then raise exception
        to redirect message to proper destination
        """
        if ip is None:
            raise exceptions.ServiceBusException("Unknown address")
        ownip = ip==self.own_ip
        if not ownip:
            raise RedirectRequiredEx(ip)


    def handle_global_control_request(self, msg):
        """
        Control requests from remote hosts
        """
        result = self.ctl.handle_request(msg)
        return result
        #return {"message":messages.RESULT, "result":result }



    # syncd host tasks


    def CTL_global_services(self):
        """
        List of all working hosts and services in network
        """
        lst = []
        # all syncd hosts
        for hst in self.DB.host_list():
            # workers on host
            hst ['services'] = self.CTL_services_on_host( hst['uuid'] )
            lst.append( hst )
        return lst


    # this command is not currently exposed via control interface
    def CTL_services_on_host(self, uuid):
        """
        List of all services on host
        """
        lst = []
        managed = set()
        for s in self.DB.service_list(uuid):
            managed.add(s['service'])
        running = set()
        for wnfo in self.DB.worker_list(uuid):
            running.add(wnfo['service'])
            wnfo['running'] = True
            wnfo['managed'] = wnfo['service'] in managed
            lst.append(wnfo)
        # offline services
        for sv in managed:
            if not sv in running:
                lst.append( {'service':sv,'running':False, 'managed':True} )
        return lst


    def CTL_worker_stop(self, uuid, terminate=False, sigkill=False):
        """
        Send stop signal to worker
        """
        wrkr = self.DB.worker_get(uuid)
        self.redirect_or_pass_by_ip( wrkr['ip'] )

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
                'method':['stop']
            }
            res = send_and_receive_response(self.context, addr, msg)
            return res


    def CTL_worker_stats(self, uuid):
        """
        Return full stats of worker
        """
        wrkr = self.DB.worker_get(uuid)
        self.redirect_or_pass_by_ip( wrkr['ip'] )
        # call worker for stats
        addr = _worker_addr(wrkr)
        msg = {
            'message':messages.CTL_CALL,
            'method':['stats']
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
        self.redirect_or_pass_by_ip(ip)

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
        self.redirect_or_pass_by_ip(ip)

        services = []
        for wrk in self.DB.worker_list_local():
            if wrk['service']==name:
                services.append(wrk['uuid'])
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
        self.redirect_or_pass_by_ip(ip)
        svlist = self.local_services_list(rescan=True)
        # send nwe list of services to kasaya daemon instance
        #self.DAEMON.notify_kasayad_refresh(self.DAEMON.uuid, svlist, True)


