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
import logging

from TorqueInfoUtils import CommonUtils

logger = logging.getLogger("QStatHandler")

class PBSJobHandler(Thread):

    def __init__(self, container):
        Thread.__init__(self)
        self.container = container
        self.errList = list()
        self.jRegex = re.compile('^\s*Job Id:(.+)$')
        self.pRegex = re.compile('^\s*([^=\s]+)\s*=(.+)$')

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
    
    
    def _registerJobItem(self, jTable, now):
        if jTable == None:
            return
                    
        if not 'user' in jTable:
            self.errList.append("Cannot find user for " + jTable['jobid'])
            
        if not 'group' in jTable:
            # fix for bug CREAM-130
            # it should be possible to extract
            # and cache the real local group
            # using maui-client (checkjob)
            jTable['group'] = '__localgroup__'
        
        if 'walltime' in jTable:
            if not 'start' in jTable:
                jTable['start'] = now - jTable['walltime']
                jTable['startAnchor'] = 'resources_used.walltime'
        else:
            if 'start' in jTable:
                jTable['walltime'] = now - jTable['start']
                        
        self.container.append(jTable)
        
    
    def run(self):
        line = self.stream.readline()
        currTable = None
        now = int(time.time()) + time.timezone
        
        while line:
            parsed = self.pRegex.match(line)
            if parsed and currTable <> None:
            
                logger.debug("(Attribute info) Detected item: %s" % line.strip())
            
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
                            logger.debug("Error parsing job info output", exc_info=True)
                
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
                
                    logger.debug("(Job info) Detected item: %s" % line.strip())
                
                    self._registerJobItem(currTable, now)
                    
                    currTable = dict()
                    currTable['jobid'] = parsed.group(1).strip()
                    currTable['state'] = 'unknown'
            
            line = self.stream.readline()

        self._registerJobItem(currTable, now)

    # end of thread



def parse(resultContainer, pbsHost=None, filename=None):

    if filename:
        cmd = shlex.split('cat ' + filename)
    else:
        if pbsHost:
            cmd = shlex.split('qstat -f @%s' % pbsHost)
        else:
            cmd = shlex.split('qstat -f')
        
    container = PBSJobHandler(resultContainer)
    CommonUtils.parseStream(cmd, container)




class LRMSVersionHandler(Thread):

    def __init__(self):
        Thread.__init__(self)
        self.version = None
        self.errList = list()
        self.pRegex = re.compile('^\s*pbs_version\s*=(.+)$')
    
    def setStream(self, stream):
        self.stream = stream
      
    def run(self):
        line = self.stream.readline()
        while line:
            parsed = self.pRegex.match(line)
            if parsed:
                self.version = parsed.group(1).strip()
                logger.debug('Found version ' + self.version)
            line = self.stream.readline()

def parseLRMSVersion(pbsHost=None, filename=None):
    if filename:
        cmd = shlex.split('cat ' + filename)
    else:
        if pbsHost:
            cmd = shlex.split('qstat -B -f %s' % pbsHost)
        else:
            cmd = shlex.split('qstat -B -f')

    logger.debug("Calling executable: " + repr(cmd))

    container = LRMSVersionHandler()
    CommonUtils.parseStream(cmd, container)
    return container.version



RES_UNDEF = -1

class QueueInfoHandler(Thread):

    def __init__(self, def_values=None):
        Thread.__init__(self)
        self.errList = list()
        self.pRegex = re.compile('^\s*([^=\s]+)\s*=(.+)$')
        self.cputRegex = re.compile('(\d+):(\d+):(\d+)')
        self.memRegex = re.compile('(\d+)([tgmkbw]+)')
        if def_values:
            self.maxCPUtime = def_values.maxCPUtime
            self.defaultCPUtime = def_values.defaultCPUtime
            self.maxPCPUtime = def_values.maxPCPUtime
            self.defaultPCPUtime = def_values.defaultPCPUtime
            self.maxTotJobs = def_values.maxTotJobs
            self.maxRunJobs = def_values.maxRunJobs
            self.maxWallTime = def_values.maxWallTime
            self.defaultWallTime = def_values.defaultWallTime
            self.maxProcCount = def_values.maxProcCount
            self.defaultProcCount = def_values.defaultProcCount
            self.defaultMem = def_values.defaultMem
            self.defaultVMem = def_values.defaultVMem
            self.maxMem = def_values.maxMem
            self.maxVMem = def_values.maxVMem
        else:
            self.maxCPUtime = RES_UNDEF
            self.defaultCPUtime = RES_UNDEF
            self.maxPCPUtime = RES_UNDEF
            self.defaultPCPUtime = RES_UNDEF
            self.maxTotJobs = RES_UNDEF
            self.maxRunJobs = RES_UNDEF
            self.maxWallTime = RES_UNDEF
            self.defaultWallTime = RES_UNDEF
            self.maxProcCount = RES_UNDEF
            self.defaultProcCount = RES_UNDEF
            self.defaultMem = RES_UNDEF
            self.defaultVMem = RES_UNDEF
            self.maxMem = RES_UNDEF
            self.maxVMem = RES_UNDEF
        self.policyPriority = None
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

            logger.debug("(Queue info) Detected item: %s" % line.strip())
                    
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
        
        #
        # following from discussion in https://savannah.cern.ch/bugs/?49653
        #
        if self.defaultCPUtime <> RES_UNDEF and self.defaultPCPUtime <> RES_UNDEF:
            self.defaultCPUtime = min(self.defaultCPUtime, self.defaultPCPUtime)
        if self.defaultCPUtime == RES_UNDEF and self.defaultPCPUtime <> RES_UNDEF:
            self.defaultCPUtime = self.defaultPCPUtime
            
        if self.maxCPUtime <> RES_UNDEF and self.maxPCPUtime <> RES_UNDEF:
            self.maxCPUtime = min(self.maxCPUtime, self.maxPCPUtime)
        if self.maxCPUtime == RES_UNDEF and self.maxPCPUtime <> RES_UNDEF:
            self.maxCPUtime = self.maxPCPUtime

        if self.maxCPUtime <> RES_UNDEF and self.defaultCPUtime == RES_UNDEF:
            self.defaultCPUtime = self.maxCPUtime
        
        if self.maxWallTime <> RES_UNDEF and self.defaultWallTime == RES_UNDEF:
            self.defaultWallTime = self.maxWallTime

        if self.maxProcCount == RES_UNDEF and self.defaultProcCount <> RES_UNDEF:
            self.maxProcCount = self.defaultProcCount
            
        if self.maxMem == RES_UNDEF and self.defaultMem <> RES_UNDEF:
            self.maxMem = self.defaultMem

        if self.maxVMem == RES_UNDEF and self.defaultVMem <> RES_UNDEF:
            self.maxVMem = self.defaultVMem


def parseQueueInfo(queue, pbsHost=None, filename=None):
    if filename:
        cmd = shlex.split('cat ' + filename)
    else:
        if pbsHost:
            cmd = shlex.split('qstat -Q -f %s\@%s' % (queue, pbsHost))
        else:
            cmd = shlex.split('qstat -Q -f %s' % queue)

    logger.debug("Calling executable: " + repr(cmd))

    container = QueueInfoHandler()
    CommonUtils.parseStream(cmd, container)
    return container

def parseAllQueuesInfo(queues, pbsHost=None):
    
    handlers = dict()
    
    #Reading server-level attributes
    if pbsHost:
        cmd = shlex.split('qstat -B -f @%s' % pbsHost)
    else:
        cmd = shlex.split('qstat -B -f')
    slh = QueueInfoHandler()
    CommonUtils.parseStream(cmd, slh)
    
    for queue in queues:
    
        if pbsHost:
            cmd = shlex.split('qstat -Q -f %s\@%s' % (queue, pbsHost))
        else:
            cmd = shlex.split('qstat -Q -f %s' % queue)

        logger.debug("Calling executable: " + repr(cmd))
        
        handlers[queue] = QueueInfoHandler(slh)
        CommonUtils.parseStream(cmd, handlers[queue])
    
    return handlers
        







