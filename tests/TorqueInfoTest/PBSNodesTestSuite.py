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
        
        self.pbsnodesPattern = '''
# pbsnodes -a
cert-wn64-01.pn.pd.infn.it
     state = %s
     np = %s
     properties = lcgpro
     ntype = cluster
     status = rectime=1376989178,varattr=,jobs=,state=free,netload=23614350258,gres=,loadave=0.00,ncpus=2,physmem=1922720kb,availmem=3814724kb,totmem=4019864kb,idletime=10626905,nusers=1,nsessions=1,sessions=1508,uname=Linux cert-wn64-01.pn.pd.infn.it 2.6.32-279.9.1.el6.x86_64 #1 SMP Tue Sep 25 14:55:44 CDT 2012 x86_64,opsys=linux
     gpus = 0

cert-wn64-03.pn.pd.infn.it
     state = %s
     np = %s
     properties = lcgpro
     ntype = cluster
     status = rectime=1376989189,varattr=,jobs=,state=free,netload=6140965090,gres=,loadave=0.00,ncpus=2,physmem=2058752kb,availmem=3931244kb,totmem=4155224kb,idletime=10626883,nusers=1,nsessions=1,sessions=2299,uname=Linux cert-wn64-03.pn.pd.infn.it 2.6.18-308.16.1.el5 #1 SMP Wed Oct 3 00:53:20 EDT 2012 x86_64,opsys=linux
     gpus = 0
'''
        
    def test_parse_all_free(self):
        pbsnodes_pattern = self.pbsnodesPattern % ('free', '2', 'free', '2')

        tmpfile = self.workspace.createFile(pbsnodes_pattern)
        container = PBSNodesHandler.parseCPUInfo(None, tmpfile)
        self.assertTrue(container.totalCPU == 4 and container.freeCPU == 4)
        
    def test_parse_all_free_with_job(self):
        pbsnodes_pattern = self.pbsnodesPattern % ('free', '2', 'free', '4')
        pbsnodes_pattern += '     jobs = 0/15.cert-34.pd.infn.it, 1/16.cert-34.pd.infn.it\n'

        tmpfile = self.workspace.createFile(pbsnodes_pattern)
        container = PBSNodesHandler.parseCPUInfo(None, tmpfile)
        self.assertTrue(container.totalCPU == 6 and container.freeCPU == 4)

    def test_parse_half_busy(self):
        pbsnodes_pattern = self.pbsnodesPattern % ('busy', '2', 'free', '2')

        tmpfile = self.workspace.createFile(pbsnodes_pattern)
        container = PBSNodesHandler.parseCPUInfo(None, tmpfile)
        self.assertTrue(container.totalCPU == 4 and container.freeCPU == 2)
    
    def test_parse_multi_state_down(self):
        pbsnodes_pattern = self.pbsnodesPattern % ('offline,down', '2', 'free', '2')

        tmpfile = self.workspace.createFile(pbsnodes_pattern)
        container = PBSNodesHandler.parseCPUInfo(None, tmpfile)
        self.assertTrue(container.totalCPU == 2 and container.freeCPU == 2)

    def test_parse_wrong_cpunum(self):
        pbsnodes_pattern = self.pbsnodesPattern % ('free', '2a', 'free', '2')
        
        try:
            tmpfile = self.workspace.createFile(pbsnodes_pattern)
            container = PBSNodesHandler.parseCPUInfo(None, tmpfile)
            self.fail("No exception detected")
        except Exception, ex:
            msg = str(ex)
            self.assertTrue(msg.startswith("invalid literal for int"))
    


if __name__ == '__main__':
    unittest.main()

