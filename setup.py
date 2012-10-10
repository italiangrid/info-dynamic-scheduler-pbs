#!/usr/bin/env python

from distutils.core import setup
import ConfigParser

pkg_version = '0.0.0'
try:
    parser = ConfigParser.ConfigParser()
    parser.read('setup.cfg')
    pkg_version = parser.get('global','pkgversion')
except:
    pass

libexec_list = [
                "src/lrmsinfo-pbs",
                "src/vomaxjobs-maui"
               ]

setup(
      name='lcg-info-dynamic-scheduler-pbs',
      version=pkg_version,
      description='Plugins for the lcg-info-dynamic-scheduler GIP plugin',
      long_description='''Plugins for the lcg-info-dynamic-scheduler GIP plugin.  The two 
plugins here are for Maui (scheduler) and PBS/Torque (LRMS).''',
      license='Apache Software License',
      author='CREAM group, Jeff Templon',
      author_email='CREAM group <cream-support@lists.infn.it>',
      py_modules=['pbsServer', 'torque_utils', 'lrms.py'],
      package_dir = {'': 'src'},
      data_files=[
                  ('usr/libexec', libexec_list)
                 ]
     )


