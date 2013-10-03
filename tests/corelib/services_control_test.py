#!/home/moozg/venvs/kasatest/bin/python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
import unittest, os

from kasaya.conf import set_value, settings, load_defaults, load_config_from_file
from decimal import Decimal


class ConfigTest(unittest.TestCase):

    def setUp(self):
        """
        wipeout all settings
        """
        settings.clear()
        #set_value("PASSWORD","absolute_secret_password")
        #set_value("COMPRESSION","no")
        #set_value("ENCRYPTION","yes")
        #
        self.test_root = os.path.dirname(__file__)
        self.test_etc = os.path.join(self.test_root, "etc")

    def tearDown(self):
        """
        Restore kasaya original config
        """
        from kasaya import conf
        settings.clear()
        load_defaults()
        load_config_from_file (conf.SYSTEM_KASAYA_CONFIG, optional=True)


    def test_default_config_loader(self):
        settings.clear()
        load_defaults()

        # force float data
        settings['FLOAT_DATA'] = 1.0
        settings['DECIMAL_DATA'] = Decimal("1")
        # load prepared config
        load_config_from_file( os.path.join(self.test_etc, "kasaya.conf"), optional=False)

        # default data types conversion
        self.assertEqual(settings.WORKER_POOL_SIZE, 5)
        self.assertEqual( type(settings.WORKER_POOL_SIZE), int )
        self.assertEqual( settings.ENCRYPTION, True )
        self.assertEqual( type(settings.ENCRYPTION), bool )
        self.assertEqual( settings.BIND_WORKER_TO, "1.2.3.4" )
        self.assertEqual( type(settings.BIND_WORKER_TO), type("") )
        self.assertEqual( settings.FLOAT_DATA, 3.0 )
        self.assertEqual( type(settings.FLOAT_DATA), float )
        self.assertEqual( settings.DECIMAL_DATA, Decimal("10.000123") )
        self.assertEqual( type(settings.DECIMAL_DATA), Decimal )


if __name__ == '__main__':
    unittest.main()
