#!/home/moozg/venvs/kasatest/bin/python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
import unittest, os

from kasaya.conf import set_value, settings, load_defaults, load_config_from_file
from decimal import Decimal
#load_settings_from_config_file


class ConfigTest(unittest.TestCase):

    def setUp(self):
        """
        wipeout all settings
        """
        settings.clear()
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
        load_defaults()
        # force float data
        settings['FLOAT_DATA'] = 1.0
        settings['DECIMAL_DATA'] = Decimal("1")
        # load prepared config
        load_config_from_file( os.path.join(self.test_etc, "test.conf"), optional=False)

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

        set_value("FLOAT_DATA", "5")
        self.assertEqual( settings.FLOAT_DATA, 5.0 )
        set_value("DECIMAL_DATA", "9.002")
        self.assertEqual( settings.DECIMAL_DATA, Decimal("9.002") )

        for y in ("yes","Yes","TAK","1","True","true"):
            set_value("ENCRYPTION", y)
            self.assertEqual( settings.ENCRYPTION, True )

        for n in ("no","No","","żółw"):
            set_value("ENCRYPTION", n)
            self.assertEqual( settings.ENCRYPTION, False )



    def test_multiple_config(self):
        pass




if __name__ == '__main__':
    unittest.main()
