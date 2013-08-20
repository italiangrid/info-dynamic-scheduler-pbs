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
from threading import Thread

from TorqueInfoUtils import CommonUtils

class PBSNodesHandler(Thread):

    def __init__(self):
        Thread.__init__(self)
        self.errList = list()
        self.nodeTables = list()
        self.pRegex = re.compile('^\s*([^=\s]+)\s*=([^$]+)$')
        
    def setStream(self, stream):
        self.stream = stream

    def run(self):
        line = self.stream.readline()
        currTable = None
        
        while line:
            parsed = self.pRegex.match(line)
            if parsed and currTable <> None:
            
                key = parsed.group(1)
                value = parsed.group(2).strip()
                
                if key == 'state':
                
                    currTable['state'] = value
                    currTable['up'] = value <> 'down' and value <> 'offline'
                
                elif key == 'np':
                    
                    try:
                        currTable['ncpu'] = int(value)
                    except:
                        currTable['ncpu'] = 0
                
                elif key == 'jobs':
                
                    if len(value) > 0:
                        currTable['njob'] = value.count(',') + 1
                                        
            else:
            
                nodeName = line.strip()
                if len(nodeName) > 0:
                    
                    if currTable <> None:                            
                        self.nodeTables.append(currTable)
                        
                    currTable = dict()
                    currTable['name'] = nodeName
                    currTable['up'] = False
                    currTable['njob'] = 0
                    currTable['ncpu'] = 0
            
            line = self.stream.readline()

        if currTable <> None:                            
            self.nodeTables.append(currTable)
        
    #end of thread

def parseForNodelist(nodeList=[""], filename=None):

    ncpu = 0
    njob = 0
    
    for nodeId in nodeList:
        if filename:
            cmd = shlex.split('cat ' + filename)
        else:
            cmd = shlex.split('pbsnodes -a ' + nodeId)
        
        container = PBSNodesHandler()
        CommonUtils.parseStream(cmd, container)
                    
        for nodeTable in container.nodeTables:
            if nodeTable['up']:
                ncpu += nodeTable['ncpu']
                njob += nodeTable['njob']
            
    return (ncpu, max(ncpu - njob, 0))

def parse(filename=None):

    ncpu = 0
    njob = 0
    
    if filename:
        cmd = shlex.split('cat ' + filename)
    else:
        cmd = shlex.split('pbsnodes -a')
    
    container = PBSNodesHandler()
    CommonUtils.parseStream(cmd, container)
                    
    for nodeTable in container.nodeTables:
        if nodeTable['up']:
            ncpu += nodeTable['ncpu']
            njob += nodeTable['njob']
            
    return (ncpu, max(ncpu - njob, 0))





class CPUInfoHandler(Thread):

    def __init__(self):
        Thread.__init__(self)
        self.errList = list()
        self.totalCPU = 0
        self.freeCPU = 0
        self.pRegex = re.compile('^\s*([^=\s]+)\s*=([^$]+)$')
    
    def setStream(self, stream):
        self.stream = stream
      
    def run(self):
        currState = None
        line = self.stream.readline()
        while line:
            parsed = self.pRegex.match(line)
            if parsed:
                if parsed.group(1) == 'state':
                
                    currState = parsed.group(2).strip()
                
                elif parsed.group(1) == 'np':
                
                    procNum = int(parsed.group(2).strip())
                    if not ('down' in currState or 'offline' in currState or 'unknown' in currState):
                        self.totalCPU += procNum
                    if currState == 'free':
                        self.freeCPU += procNum
                
                elif parsed.group(1) == 'jobs':
                    
                    jobs = parsed.group(2).strip().split(', ')
                    if currState == 'free':
                        self.freeCPU -= len(jobs)
                
            line = self.stream.readline()


def parseCPUInfo(pbsHost, filename=None):

    if filename:
        cmd = shlex.split('cat ' + filename)
    else:
        cmd = shlex.split('pbsnodes -a -s %s' % pbsHost)

    container = CPUInfoHandler()
    CommonUtils.parseStream(cmd, container)
    return container
    





