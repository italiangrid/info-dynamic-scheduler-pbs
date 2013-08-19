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
import re
import time
import shlex
from threading import Thread
import pwd
import grp

from TorqueInfoUtils import CommonUtils

class PBSJobHandler(Thread):

    def __init__(self, container):
        Thread.__init__(self)
        self.container = container
        self.errList = list()
        self.jRegex = re.compile('^\s*Job Id:([^$]+)$')
        self.pRegex = re.compile('^\s*([^=\s]+)\s*=([^$]+)$')

    def setStream(self, stream):
        self.stream = stream

    def _convertState(self, state):
        if state == 'Q' or state == 'W':
            return 'queued'
        if state == 'R' or state == 'E':
            return 'running'
        if state == 'H' or state == 'T':
            return 'pending'
        if state == 'C':
            return 'done'
        return 'unknown'
    
    def _convertTimeStr(self, tStr):
        #
        # TODO verify conversion to UTC
        #      any timestamp MUST refer to UTC
        #
        timetuple = time.strptime(tStr,"%c")
        return  int(time.mktime(timetuple)) + time.timezone
    
    
    
    
    def run(self):
        line = self.stream.readline()
        currTable = None
        now = int(time.time())
        
        while line:
            parsed = self.pRegex.match(line)
            if parsed and currTable <> None:
                key = parsed.group(1)
                value = parsed.group(2).strip()
                
                if key == 'euser':
                
                    currTable['user'] = value
                
                elif key == 'egroup':
                
                    currTable['group'] = value
                
                elif key == 'Job_Owner':
                    tmpt = value.split('@')
                    if len(tmpt) == 2:
                        currTable['user'] = tmpt[0]
                        try:
                            thisgroup=pwd.getpwnam(tmpt[0])[3]
                            currTable['group'] = grp.getgrgid(thisgroup)[0]
                        except:
                            etype, evalue, etraceback = sys.exc_info()
                
                elif key == 'job_state':
                
                    currTable['state'] = self._convertState(value)
                
                elif key == 'queue':
                
                    currTable['queue'] = value
                
                elif key == 'qtime':
                
                    currTable['qtime'] = self._convertTimeStr(value)
                    
                elif key == 'Resource_List.walltime':
                    
                    tmpt = value.split(':')
                    if len(tmpt) == 3:
                        currTable['maxwalltime'] = int(tmpt[0]) * 3600 + int(tmpt[1]) * 60 + int(tmpt[2])
                
                elif key == 'start_time':
                
                    currTable['start'] = self._convertTimeStr(value)
                    currTable['startAnchor'] = 'start_time'
                
                elif key == 'resources_used.walltime':
                
                    tmpt = value.split(':')
                    if len(tmpt) == 3:
                        currTable['walltime'] = int(tmpt[0]) * 3600 + int(tmpt[1]) * 60 + int(tmpt[2])
                
                elif key == 'Job_Name':
                
                    currTable['name'] = value
                
                elif key == 'exec_host':
                
                    currTable['cpucount'] = value.count('+') + 1
                
            else:
                parsed = self.jRegex.match(line)
                if parsed:
                
                    if currTable <> None:
                    
                        if not 'user' in currTable:
                            self.errList.append("Cannot find user for " + currTable['jobid'])
                        if not 'group' in currTable:
                            self.errList.append("Cannot find user for " + currTable['jobid'])
                        if 'walltime' in currTable:
                            if not 'start' in currTable:
                                currTable['start'] = now - currTable['walltime']
                                currTable['startAnchor'] = 'resources_used.walltime'
                        else:
                            if 'start' in currTable:
                                currTable['walltime'] = now - currTable['start']
                        
                        self.container.append(currTable)
                    
                    currTable = dict()
                    currTable['jobid'] = parsed.group(1).strip()
                    currTable['state'] = 'unknown'
            
            line = self.stream.readline()

        if currTable <> None:
            if not 'user' in currTable:
                self.errList.append("Cannot find user for " + currTable['jobid'])
            if not 'group' in currTable:
                self.errList.append("Cannot find user for " + currTable['jobid'])
            if 'walltime' in currTable:
                if not 'start' in currTable:
                    currTable['start'] = now - currTable['walltime']
                    currTable['startAnchor'] = 'resources_used.walltime'
            else:
                if 'start' in currTable:
                    currTable['walltime'] = now - currTable['start']
        
            self.container.append(currTable)

    # end of thread



def parse(resultContainer, filename=None):

    if filename:
        cmd = shlex.split('cat ' + filename)
    else:
        cmd = shlex.split('qstat -f')
        
    container = PBSJobHandler(resultContainer)
    CommonUtils.parseStream(cmd, container)


