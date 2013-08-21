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

from TorqueInfoUtils import QStatHandler
from TestUtils import Workspace


class QStatTestCase(unittest.TestCase):

    def setUp(self):
        self.workspace = Workspace()
        
        self.srvPattern = '''# qstat -B -f
Server: cert-34.pd.infn.it
    server_state = Active
    scheduling = True
    total_jobs = 0
    state_count = Transit:0 Queued:3 Held:0 Waiting:-3 Running:0 Exiting:0 
    acl_host_enable = False
    acl_hosts = cert-34.pd.infn.it
    managers = root@cert-34.pd.infn.it
    operators = root@cert-34.pd.infn.it
    default_queue = dteam
    log_events = 511
    mail_from = adm
    query_other_jobs = True
    resources_assigned.nodect = 0
    scheduler_iteration = 600
    node_check_rate = 150
    tcp_timeout = 6
    default_node = lcgpro
    node_pack = False
    mail_domain = never
    pbs_version = %s
    kill_delay = 10
    next_job_number = 11
    net_counter = 3 0 0
    authorized_users = *@cert-34.pd.infn.it
'''

        self.jobPattern = '''# qstat -f
Job Id: %s.cert-34.pd.infn.it
    Job_Name = %s
    Job_Owner = dteam013@cert-34.pd.infn.it
    job_state = %s
    queue = %s
    server = cert-34.pd.infn.it
    Checkpoint = u
    ctime = Wed Aug 21 11:37:25 2013
    Error_Path = cert-34.pd.infn.it:/dev/null
    Hold_Types = n
    Join_Path = n
    Keep_Files = n
    Mail_Points = n
    mtime = Wed Aug 21 11:37:25 2013
    Output_Path = cert-34.pd.infn.it:/dev/null
    Priority = 0
    qtime = Wed Aug 21 11:37:25 2013
    Rerunable = True
    Resource_List.neednodes = 1
    Resource_List.nodect = 1
    Resource_List.nodes = 1
    Shell_Path_List = /bin/bash
    stagein = CREAM921657923_jobWrapper.sh.18190.15697.1377077844@cert-34.pd.infn.it
    stageout = out_cream_921657923_StandardOutput@cert-34.pd.infn.it
    substate = 11
    Variable_List = PBS_O_QUEUE=cert,PBS_O_HOME=/home/dteam013,
	PBS_O_LANG=en_US.UTF-8,PBS_O_LOGNAME=dteam013,
	PBS_O_PATH=/usr/kerberos/bin:/bin:/usr/bin:/home/dteam013/bin,
	PBS_O_MAIL=/var/spool/mail/dteam013,PBS_O_SHELL=/bin/sh,
	PBS_O_HOST=cert-34.pd.infn.it,PBS_SERVER=cert-34.pd.infn.it,
	PBS_O_WORKDIR=/var/tmp
    euser = dteam013
    egroup = dteam
    queue_rank = 23
    queue_type = E
    etime = Wed Aug 21 11:37:25 2013
    submit_args = /tmp/cream_921657923
    fault_tolerant = False
    submit_host = cert-34.pd.infn.it
    init_work_dir = /var/tmp
'''


    def test_lrmsver_ok(self):
        pattern = self.srvPattern % '2.5.7'
        
        tmpfile = self.workspace.createFile(pattern)
        self.assertTrue(QStatHandler.parseLRMSVersion(None, tmpfile) == '2.5.7') 

    def test_lrmsver_missing(self):
        pattern = self.srvPattern.replace('pbs_version', 'no_version')
        
        tmpfile = self.workspace.createFile(pattern)
        self.assertTrue(QStatHandler.parseLRMSVersion(None, tmpfile) == None)
    
    def test_parse_job_ok(self):
    
        pattern = self.jobPattern % ('01', 'cream_921657923', 'R', 'cert')
        tmpfile = self.workspace.createFile(pattern)
        
        pattern = self.jobPattern % ('02', 'cream_921657924', 'R', 'cert')
        self.workspace.appendToFile(pattern, tmpfile)
        
        outList = list()
        QStatHandler.parse(outList, tmpfile)
        self.assertTrue(len(outList) == 2) 
        
         


if __name__ == '__main__':
    unittest.main()

