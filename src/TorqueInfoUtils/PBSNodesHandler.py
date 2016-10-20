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
import subprocess
import logging
from threading import Thread

from TorqueInfoUtils import CommonUtils

logger = logging.getLogger("PBSNodesHandler")

class CPUInfoHandler(Thread):

    def __init__(self):
        Thread.__init__(self)
        self.errList = list()
        self.totalCPU = 0
        self.freeCPU = 0
        self.pRegex = re.compile('^\s*([^=\s]+)\s*=(.+)$')
        self.gpuRegex = re.compile('gpu\[\d*\]\s*=\s*')
        self.gpuTable = dict()
    
    def setStream(self, stream):
        self.stream = stream
      
    def run(self):
    
        currState = None
        currNode = None
        
        try:
            line = self.stream.readline()
            while line:
                parsed = self.pRegex.match(line)
                if parsed:
                
                    logger.debug("Detected item: %s" % line.strip())
                    
                    if parsed.group(1) == 'state':
                
                        currState = parsed.group(2).strip()
                
                    elif parsed.group(1) == 'np':
                
                        procNum = int(parsed.group(2).strip())
                        if not ('down' in currState or 'offline' in currState or 'unknown' in currState):
                            self.totalCPU += procNum
                        if currState == 'free':
                            self.freeCPU += procNum
                
                    elif parsed.group(1) == 'jobs':
                    
                        jobList = parsed.group(2).strip()
                        if currState == 'free' and len(jobList) > 0:
                            self.freeCPU -= jobList.count(',') + 1

                    elif parsed.group(1) == 'gpu_status':

                        gpuNodeInfo = dict()
                        gpuNodeInfo['node_state'] = currState
                        gpuNodeInfo['total_gpus'] = 0
                        gpuNodeInfo['free_gpus'] = 0

                        for gpuStats in self.gpuRegex.split(parsed.group(2).strip()):

                            gpuStats = gpuStats.strip()
                            if len(gpuStats) == 0:
                                continue

                            curr_gpu_use = 100
                            curr_mem_use = 100
                            for pStr in gpuStats.split(';'):
                                res = self.pRegex.match(pStr)
                                if res.group(1) == 'gpu_utilization':
                                    curr_gpu_use = int(re.match('\d+', res.group(2)).group(0))
                                elif res.group(1) == 'gpu_memory_utilization':
                                    curr_mem_use = int(re.match('\d+', res.group(2)).group(0))
                            
                            if curr_gpu_use == 0 and curr_mem_use ==0:
                                gpuNodeInfo['free_gpus'] += 1
                            gpuNodeInfo['total_gpus'] += 1

                        self.gpuTable[currNode] = gpuNodeInfo

                else:
                    tmps = line.strip()
                    if len(tmps):
                        currNode = tmps

                line = self.stream.readline()
        
        except:
            logger.debug("Error parsing pbsnodes output", exc_info=True)
            self.errList.append(CommonUtils.errorMsgFromTrace())


def parseCPUInfo(pbsHost=None, filename=None):

    if filename:
        cmd = shlex.split('cat ' + filename)
    else:
        if pbsHost:
            cmd = shlex.split('pbsnodes -a -s %s' % pbsHost)
        else:
            cmd = shlex.split('pbsnodes -a')
            
    logger.debug("Calling executable: " + repr(cmd))

    container = CPUInfoHandler()
    CommonUtils.parseStream(cmd, container)
    return container
    





