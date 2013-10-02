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
    
    def setStream(self, stream):
        self.stream = stream
      
    def run(self):
    
        currState = None
        
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
    





