# Copyright (c) Members of the EGEE Collaboration. 2004. 
# See http://www.eu-egee.org/partners/ for details on the copyright
# holders.  
#
# Licensed under the Apache License, Version 2.0 (the "License"); 
# you may not use this file except in compliance with the License. 
# You may obtain a copy of the License at 
#
#     http://www.apache.org/licenses/LICENSE-2.0 
#
# Unless required by applicable law or agreed to in writing, software 
# distributed under the License is distributed on an "AS IS" BASIS, 
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
# See the License for the specific language governing permissions and 
# limitations under the License.

import sys
import unittest

from TorqueInfoUtils import PBSNodesHandler
from TestUtils import Workspace


class PBSNodesTestCase(unittest.TestCase):

    def setUp(self):
        self.workspace = Workspace()
        
        self.pbsnodesPattern = '''# item from pbsnodes -a
%(host)s.pn.pd.infn.it
     state = %(state)s
     np = %(np)s
     properties = lcgpro
     ntype = cluster
     status = rectime=1376989178,varattr=,jobs=,state=free,netload=23614350258,gres=,loadave=0.00
     gpus = 0

'''
        
    def test_parse_all_free(self):

        pattern_args = {'host' : 'cert-wn64-01', 'state' : 'free', 'np' : '2'}
        tmpfile = self.workspace.createFile(self.pbsnodesPattern % pattern_args)
        
        pattern_args = {'host' : 'cert-wn64-02', 'state' : 'free', 'np' : '2'}
        self.workspace.appendToFile(self.pbsnodesPattern % pattern_args, tmpfile)
        
        container = PBSNodesHandler.parseCPUInfo(None, tmpfile)
        self.assertTrue(container.totalCPU == 4 and container.freeCPU == 4)
        
    def test_parse_all_free_with_job(self):

        pattern_args = {'host' : 'cert-wn64-01', 'state' : 'free', 'np' : '2'}
        tmpfile = self.workspace.createFile(self.pbsnodesPattern % pattern_args)
        
        pattern_args = {'host' : 'cert-wn64-02', 'state' : 'free', 'np' : '4'}
        tmps = self.pbsnodesPattern % pattern_args
        tmps += '     jobs = 0/15.cert-34.pd.infn.it, 1/16.cert-34.pd.infn.it\n'
        self.workspace.appendToFile(tmps, tmpfile)
        
        container = PBSNodesHandler.parseCPUInfo(None, tmpfile)
        self.assertTrue(container.totalCPU == 6 and container.freeCPU == 4)

    def test_parse_half_busy(self):
    
        pattern_args = {'host' : 'cert-wn64-01', 'state' : 'busy', 'np' : '2'}
        tmpfile = self.workspace.createFile(self.pbsnodesPattern % pattern_args)
        
        pattern_args = {'host' : 'cert-wn64-02', 'state' : 'free', 'np' : '2'}
        self.workspace.appendToFile(self.pbsnodesPattern % pattern_args, tmpfile)

        container = PBSNodesHandler.parseCPUInfo(None, tmpfile)
        self.assertTrue(container.totalCPU == 4 and container.freeCPU == 2)
    
    def test_parse_multi_state_down(self):

        pattern_args = {'host' : 'cert-wn64-01', 'state' : 'offline,down', 'np' : '2'}
        tmpfile = self.workspace.createFile(self.pbsnodesPattern % pattern_args)
        
        pattern_args = {'host' : 'cert-wn64-02', 'state' : 'free', 'np' : '2'}
        self.workspace.appendToFile(self.pbsnodesPattern % pattern_args, tmpfile)

        container = PBSNodesHandler.parseCPUInfo(None, tmpfile)
        self.assertTrue(container.totalCPU == 2 and container.freeCPU == 2)

    def test_parse_wrong_cpunum(self):
        
        try:
            pattern_args = {'host' : 'cert-wn64-01', 'state' : 'free', 'np' : '2a'}
            tmpfile = self.workspace.createFile(self.pbsnodesPattern % pattern_args)
        
            pattern_args = {'host' : 'cert-wn64-02', 'state' : 'free', 'np' : '2'}
            self.workspace.appendToFile(self.pbsnodesPattern % pattern_args, tmpfile)

            container = PBSNodesHandler.parseCPUInfo(None, tmpfile)
            self.fail("No exception detected")
        except Exception, ex:
            msg = str(ex)
            self.assertTrue(msg.startswith("invalid literal for int"))
    


if __name__ == '__main__':
    unittest.main()

