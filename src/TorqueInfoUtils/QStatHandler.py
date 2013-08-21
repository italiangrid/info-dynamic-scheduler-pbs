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




class LRMSVersionHandler(Thread):

    def __init__(self):
        Thread.__init__(self)
        self.version = None
        self.errList = list()
        self.pRegex = re.compile('^\s*pbs_version\s*=([^$]+)$')
    
    def setStream(self, stream):
        self.stream = stream
      
    def run(self):
        line = self.stream.readline()
        while line:
            parsed = self.pRegex.match(line)
            if parsed:
                self.version = parsed.group(1).strip()
            line = self.stream.readline()

def parseLRMSVersion(pbsHost=None, filename=None):
    if filename:
        cmd = shlex.split('cat ' + filename)
    else:
        if pbsHost:
            cmd = shlex.split('qstat -B -f %s' % pbsHost)
        else:
            cmd = shlex.split('qstat -B -f')

    container = LRMSVersionHandler()
    CommonUtils.parseStream(cmd, container)
    return container.version



class QueueInfoHandler(Thread):

    def __init__(self):
        Thread.__init__(self)
        self.errList = list()
        self.pRegex = re.compile('^\s*([^=\s]+)\s*=([^$]+)$')
        self.cputRegex = re.compile('(\d+):(\d+):(\d+)')
        self.memRegex = re.compile('(\d+)([tgmkbw]+)')
        self.maxCPUtime = -1
        self.defaultCPUtime = -1
        self.maxPCPUtime = -1
        self.defaultPCPUtime = -1
        self.maxTotJobs = -1
        self.maxRunJobs = -1
        self.policyPriority = None
        self.maxWallTime = -1
        self.defaultWallTime = -1
        self.obtWallTime = -1
        self.maxProcCount = -1
        self.defaultProcCount = -1
        self.defaultMem = -1
        self.defaultVMem = -1
        self.maxMem = -1
        self.maxVMem = -1
        self.enabled = False
        self.started = False
        self.state = 'Closed'
        
    def setStream(self, stream):
        self.stream = stream
        
    def conv(self, strtime):
        parsed = self.cputRegex.match(strtime.strip())
        if parsed:
            return int(parsed.group(1)) * 3600 + int(parsed.group(2)) * 60 + int(parsed.group(3))
        if strtime.strip() <> '-':
            return int(strtime.strip())
        return 0
        
    def convMem(self, msize):
        parsed = self.memRegex.match(msize.strip())
        if parsed:
            suffix = parsed.group(2)
            if suffix == 'b' or suffix == 'w':
                return int(parsed.group(1)) / 1048576
            if suffix == 'kb' or suffix == 'kw':
                return int(parsed.group(1)) / 1024
            if suffix == 'mb' or suffix == 'mw':
                return int(parsed.group(1))
            if suffix == 'gb' or suffix == 'gw':
                return int(parsed.group(1)) * 1024
            if suffix == 'tb' or suffix == 'tw':
                return int(parsed.group(1)) * 1048576
        return 0

    def run(self):
        line = self.stream.readline()
        while line:
            parsed = self.pRegex.match(line)
            if parsed:
                if parsed.group(1) == 'resources_max.cput':
                
                    self.maxCPUtime = self.conv(parsed.group(2))
                    
                elif parsed.group(1) == 'resources_default.cput':
                
                    self.defaultCPUtime = self.conv(parsed.group(2))
                    
                elif parsed.group(1) == 'resources_max.pcput':
                
                    self.maxPCPUtime = self.conv(parsed.group(2))
                    
                elif parsed.group(1) == 'resources_default.pcput':
                
                    self.defaultPCPUtime = self.conv(parsed.group(2))
                    
                elif parsed.group(1) == 'max_queuable':
                
                    self.maxTotJobs = int(parsed.group(2).strip())
                    
                elif parsed.group(1) == 'Priority':
                    
                    self.policyPriority = parsed.group(2).strip()
                    
                elif parsed.group(1) == 'max_running':
                
                    self.maxRunJobs = int(parsed.group(2).strip())
                    
                elif parsed.group(1) == 'resources_max.walltime':
                
                    self.maxWallTime = self.conv(parsed.group(2))
                    
                elif parsed.group(1) == 'resources_default.walltime':
                
                    self.defaultWallTime = self.conv(parsed.group(2))
                    
                elif parsed.group(1) == 'resources_max.procct':
                
                    self.maxProcCount = int(parsed.group(2).strip())
                    
                elif parsed.group(1) == 'resources_default.procct':
                
                    self.defaultProcCount = int(parsed.group(2).strip())
                    
                elif parsed.group(1) == 'resources_default.mem':
                
                    self.defaultMem = self.convMem(parsed.group(2))
                    
                elif parsed.group(1) == 'resources_default.vmem':
                
                    self.defaultVMem = self.convMem(parsed.group(2))
                    
                elif parsed.group(1) == 'resources_max.mem':
                
                    self.maxMem = self.convMem(parsed.group(2))
                    
                elif parsed.group(1) == 'resources_max.vmem':
                
                    self.maxVMem = self.convMem(parsed.group(2))
                    
                elif parsed.group(1) == 'enabled' and parsed.group(2).strip() == 'True':
                    
                    self.enabled = True
                    
                elif parsed.group(1) == 'started' and parsed.group(2).strip() == 'True':
                
                    self.started = True



            line = self.stream.readline()

        if self.enabled:
            if self.started:
                self.state = 'Production'
            else:
                self.state = 'Queueing'
        else:
            if self.started:
                self.state = 'Draining'
            
        if self.defaultCPUtime <> -1 and self.defaultPCPUtime <> -1:
            self.defaultCPUtime = min(self.defaultCPUtime, self.defaultPCPUtime)
        if self.defaultPCPUtime <> -1:
            self.defaultCPUtime = self.defaultPCPUtime
            
        if self.maxCPUtime <> -1 and self.maxPCPUtime <> -1:
            self.maxCPUtime = min(self.maxCPUtime, self.maxPCPUtime)
        if self.maxPCPUtime <> -1:
            self.maxCPUtime = self.maxPCPUtime

        if self.maxCPUtime == -1 and self.defaultCPUtime <> -1:
            self.maxCPUtime = self.defaultCPUtime
        
        if self.maxWallTime <> -1:
            self.obtWallTime = self.maxWallTime
                
        if self.maxWallTime == -1 and self.defaultWallTime <> -1:
            self.maxWallTime = self.defaultWallTime

        if self.maxProcCount == -1 and self.defaultProcCount <> -1:
            self.maxProcCount = self.defaultProcCount
            
        if self.maxMem == -1 and self.defaultMem <> -1:
            self.maxMem = self.defaultMem

        if self.maxVMem == -1 and self.defaultVMem <> -1:
            self.maxVMem = self.defaultVMem


def parseQueueInfo(queue, pbsHost=None, filename=None):
    if filename:
        cmd = shlex.split('cat ' + filename)
    else:
        if pbsHost:
            cmd = shlex.split('qstat -Q -f %s\@%s' % (queue, pbsHost))
        else:
            cmd = shlex.split('qstat -Q -f %s' % queue)

    container = QueueInfoHandler()
    CommonUtils.parseStream(cmd, container)
    return container


