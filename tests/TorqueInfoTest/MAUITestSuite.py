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

from TorqueInfoUtils import MAUIHandler
from TestUtils import Workspace


class MAUITestCase(unittest.TestCase):

    def setUp(self):
        self.workspace = Workspace()

        self.diagPattern = '''Displaying group information...
Name         Priority        Flags         QDef      QOSList*        PartitionList Target  Limits

%(group)s               0       [NONE]       [NONE]       [NONE]                [NONE]   0.00  %(limit)s
DEFAULT             0       [NONE]       [NONE]       [NONE]                [NONE]   0.00  [NONE]

'''

    def test_diagnose_limited_ok(self):
        
        pattern_args = {'group' : 'dteam', 'limit' : 'MAXPROC=50'}
        tmpfile = self.workspace.createFile(self.diagPattern % pattern_args)
        
        container = MAUIHandler.parseJobLimit(None, None, tmpfile)
        self.assertTrue('dteam' in container.limitTable and container.limitTable['dteam'] == 50)
        
    def test_diagnose_unlimited_ok(self):
        
        pattern_args = {'group' : 'dteam', 'limit' : '[NONE]'}
        tmpfile = self.workspace.createFile(self.diagPattern % pattern_args)
        
        container = MAUIHandler.parseJobLimit(None, None, tmpfile)
        self.assertTrue(len(container.limitTable) == 0)

if __name__ == '__main__':
    unittest.main()

