#!/usr/bin/env python
#coding: utf-8
from distutils.core import setup
import os


def package_finder(dirname, onlypackgages=True, destdir=None):
    global packages, package_dir
    for dirpath, dirnames, filenames in os.walk(dirname):
        dirnames[:] = [d for d in dirnames if not d.startswith('.') and d != '__pycache__']
        packname = ".".join( dirpath.split("/") )

        is_package = "__init__.py" in filenames
        if onlypackgages:
            if not is_package:
                continue
        packages.append( packname )
        if not destdir is None:
            #print packname
            package_dir[packname] = os.path.join(destdir, packname)


packages = []
package_dir = {}

package_finder("kasaya")
#package_finder("examples", False, "kasaya")
#package_finder("tests", False, "kasaya")

#from pprint import pprint
#pprint (packages)
#pprint (package_dir)


setup(
    name = 'Kasaya ESB',
    version = '0.0.1',
    packages = packages,
    package_dir = package_dir,
    data_files = [
        ("config", ["examples/kasaya.conf"]),
        #("")
    ],
    # tools
    scripts=['bin/svbus'],
    # requirments
    install_requires = [
        'gevent',
        'zerorpc',
        'gevent-zeromq',
        'msgpack-python',
        'netifaces',
        'plac',
        'pycrypto',
    ],

    #home_page = "http://github.com/AYAtechnologies/Kasaya-esb",
    license = file('LICENSE').read(),
    long_description = file('README.md').read(),
    author = "AYA Technologies",
    author_email = "kb@ayatechnologies.net",
)



#setup(...,
#      data_files=[('/sbin', ['rootfill']),
#                  ('/etc/init.d', ['init-script'])]
#     )