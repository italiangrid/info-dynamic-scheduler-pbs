#!/usr/bin/env python

import sys, os, os.path, shlex, subprocess
from subprocess import Popen as execScript
from distutils.core import setup
from distutils.command.bdist_rpm import bdist_rpm as _bdist_rpm

pkg_name = 'lcg-info-dynamic-scheduler-pbs'
pkg_version = '2.4.5'
pkg_release = '1'

source_items = "config setup.py src"

class bdist_rpm(_bdist_rpm):

    def run(self):

        topdir = os.path.join(os.getcwd(), self.bdist_base, 'rpmbuild')
        builddir = os.path.join(topdir, 'BUILD')
        srcdir = os.path.join(topdir, 'SOURCES')
        specdir = os.path.join(topdir, 'SPECS')
        rpmdir = os.path.join(topdir, 'RPMS')
        srpmdir = os.path.join(topdir, 'SRPMS')
        
        cmdline = "mkdir -p %s %s %s %s %s" % (builddir, srcdir, specdir, rpmdir, srpmdir)
        execScript(shlex.split(cmdline)).communicate()
        
        cmdline = "tar -zcf %s %s" % (os.path.join(srcdir, pkg_name + '.tar.gz'), source_items)
        execScript(shlex.split(cmdline)).communicate()
        
        specOut = open(os.path.join(specdir, pkg_name + '.spec'),'w')
        cmdline = "sed -e 's|@PKGVERSION@|%s|g' -e 's|@PKGRELEASE@|%s|g' project/%s.spec.in" % (pkg_version, pkg_release, pkg_name)
        execScript(shlex.split(cmdline), stdout=specOut, stderr=sys.stderr).communicate()
        specOut.close()
        
        cmdline = "rpmbuild -ba --define '_topdir %s' %s.spec" % (topdir, os.path.join(specdir, pkg_name))
        execScript(shlex.split(cmdline)).communicate()


libexec_list = [
                "src/info-dynamic-pbs",
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
      author_email='CREAM group <cream-support@lists.infn.it>',
      packages=['TorqueInfoUtils'],
      package_dir = {'': 'src'},
      data_files=[
                  ('usr/libexec', libexec_list)
                 ],
      cmdclass={'bdist_rpm': bdist_rpm}
     )


