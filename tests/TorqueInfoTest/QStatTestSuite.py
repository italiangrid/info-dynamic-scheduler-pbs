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
    pbs_version = %(lrmsver)s
    kill_delay = 10
    next_job_number = 11
    net_counter = 3 0 0
    authorized_users = *@cert-34.pd.infn.it
'''

        self.jobPattern = '''# item from qstat -f
Job Id: %(jserial)s.cert-34.pd.infn.it
    Job_Name = %(jname)s
    Job_Owner = dteam013@cert-34.pd.infn.it
    job_state = %(jstate)s
    queue = %(queue)s
    euser = dteam013
    egroup = dteam
    qtime = %(qtime)s
    Resource_List.walltime = 36:00:00
    %(pair1)s
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
    queue_rank = 23
    queue_type = E
    etime = Wed Aug 21 11:37:25 2013
    submit_args = /tmp/cream_921657923
    fault_tolerant = False
    submit_host = cert-34.pd.infn.it
    init_work_dir = /var/tmp

'''

        self.queuePattern = '''# item from qstat -Q -f
Queue: %(queue)s
    queue_type = Execution
    total_jobs = 0
    state_count = Transit:0 Queued:0 Held:0 Waiting:0 Running:0 Exiting:0 
    resources_max.cput = %(maxcpu)s
    resources_max.walltime = %(maxwt)s
    acl_group_enable = True
    acl_groups = dteam,infngrid,testers
    mtime = 1375189536
    resources_assigned.nodect = 0
    enabled = True
    started = True

'''


    def test_lrmsver_ok(self):
        pattern = self.srvPattern % {'lrmsver' : '2.5.7'}
        
        tmpfile = self.workspace.createFile(pattern)
        self.assertTrue(QStatHandler.parseLRMSVersion(None, tmpfile) == '2.5.7') 

    def test_lrmsver_missing(self):
        pattern = self.srvPattern.replace('pbs_version', 'no_version')
        
        tmpfile = self.workspace.createFile(pattern)
        self.assertTrue(QStatHandler.parseLRMSVersion(None, tmpfile) == None)
    
    def test_parse_job_qtime_ok(self):
    
        pattern_args = {'jserial' : '01', 
                        'jname' : 'cream_921657923', 
                        'jstate' : 'Q',
                        'qtime' : 'Wed Aug 21 11:37:25 2013',
                        'queue' : 'cert',
                        'pair1' : 'dummy1 = None'}
        tmpfile = self.workspace.createFile(self.jobPattern % pattern_args)
        
        pattern_args = {'jserial' : '02', 
                        'jname' : 'cream_921657924', 
                        'jstate' : 'Q',
                        'qtime' : 'Wed Aug 21 11:37:30 2013',
                        'queue' : 'cert',
                        'pair1' : 'dummy1 = None'}
        self.workspace.appendToFile(self.jobPattern % pattern_args, tmpfile)
        
        outList = list()
        QStatHandler.parse(outList, None, tmpfile)
        qtimeCount = 0
        for jtable in outList:
            if jtable['qtime'] == 1377074245 or jtable['qtime'] == 1377074250:
                qtimeCount += 1
        self.assertTrue(qtimeCount == 2) 
        
    def test_parse_job_stime_ok(self):
    
        pattern_args = {'jserial' : '01', 
                        'jname' : 'cream_921657923', 
                        'jstate' : 'R',
                        'qtime' : 'Wed Aug 21 11:37:25 2013',
                        'queue' : 'cert',
                        'pair1' : 'start_time = Wed Aug 21 11:37:26 2013'}
        tmpfile = self.workspace.createFile(self.jobPattern % pattern_args)
        
        pattern_args = {'jserial' : '02', 
                        'jname' : 'cream_921657924', 
                        'jstate' : 'Q',
                        'qtime' : 'Wed Aug 21 11:37:30 2013',
                        'queue' : 'cert',
                        'pair1' : 'dummy1 = None'}
        self.workspace.appendToFile(self.jobPattern % pattern_args, tmpfile)
        
        outList = list()
        QStatHandler.parse(outList, None, tmpfile)
        stimeCount = 0
        for jtable in outList:
            try:
                if jtable['start'] == 1377074246 and jtable['startAnchor'] == 'start_time':
                    stimeCount += 1
            except:
                pass
        self.assertTrue(stimeCount == 1) 
         
    def test_parse_queue_ok(self):
        
        pattern_args = {'queue' : 'cert', 'maxcpu' : '24:00:00', 'maxwt' : '36:00:00'}
        tmpfile = self.workspace.createFile(self.queuePattern % pattern_args)
        
        container = QStatHandler.parseQueueInfo('cert', None, tmpfile)
        result = container.maxCPUtime == 86400
        result = result and container.maxWallTime == 129600
        self.assertTrue(result)

if __name__ == '__main__':
    unittest.main()

