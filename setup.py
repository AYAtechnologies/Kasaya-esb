#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
from distutils.core import setup
import os


packages, package_data = [], {}
EXCLUDE_FROM_PACKAGES = []


def fullsplit(path, result=None):
    """
    Split a pathname into components (the opposite of os.path.join)
    in a platform-neutral way.
    """
    if result is None:
        result = []
    head, tail = os.path.split(path)
    if head == '':
        return [tail] + result
    if head == path:
        return result
    return fullsplit(head, [tail] + result)


def is_package(package_name):
    for pkg in EXCLUDE_FROM_PACKAGES:
        if package_name.startswith(pkg):
            return False
    return True


def package_finder_old(root_dir):
    global packages, package_data
    #os.chdir(root_dir)
    for dirpath, dirnames, filenames in os.walk(root_dir):
        # Ignore PEP 3147 cache dirs and those whose names start with '.'
        dirnames[:] = [d for d in dirnames if not d.startswith('.') and d != '__pycache__']
        parts = fullsplit(dirpath)
        package_name = '.'.join(parts)
        if '__init__.py' in filenames and is_package(package_name):
            packages.append(package_name)
        elif filenames:
            relative_path = []
            while '.'.join(parts) not in packages:
                relative_path.append(parts.pop())
            relative_path.reverse()
            path = os.path.join(*relative_path)
            package_files = package_data.setdefault('.'.join(parts), [])
            package_files.extend([os.path.join(path, f) for f in filenames])


def package_finder(dirname):
    global packages, package_data
    for dirpath, dirnames, filenames in os.walk(dirname):
        dirnames[:] = [d for d in dirnames if not d.startswith('.') and d != '__pycache__']
        packname = ".".join( dirpath.split("/") )
        if "__init__.py" in filenames:
            packages.append( packname )
        else:
            continue
        print packname
        #print filenames


#root_dir = os.path.dirname( os.path.abspath(__file__) )
package_finder("kasaya")
#package_finder("examples")
#package_finder("tests")

from pprint import pprint
pprint (packages)
#pprint (package_data)


setup(
    name = 'Kasaya ESB',
    version = '0.0.1',
    packages = packages,#['servicebus','middleware','examples'],
    package_data = package_data,

    #packages=find_packages(),

    data_files = [
        ("config", ["examples/kasaya.conf"])
    ],

    home_page = "http://github.com/AYAtechnologies/Kasaya-esb",

    license = file('LICENSE').read(),
    long_description = file('README.md').read(),
    author = "AYA Technologies",
    author_email = "kb@ayatechnologies.net",
)



