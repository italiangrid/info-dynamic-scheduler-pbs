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


class ErrorHandler(Thread):

    def __init__(self, err_stream):
        Thread.__init__(self)
        self.stream = err_stream
        self.message = ""
    
    def run(self):
        line = self.stream.readline()
        while line:
            self.message = self.message + line
            line = self.stream.readline()



class PBSNodesHandler(Thread):

    def __init__(self, stream):
        Thread.__init__(self)
        self.stream = stream
        self.errList = list()
        self.container = list()
        self.pRegex = re.compile('^\s*([^=\s]+)\s*=([^$]+)$')
        
        
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
                        self.container.append(currTable)
                        
                    currTable = dict()
                    currTable['name'] = nodeName
                    currTable['up'] = False
                    currTable['njob'] = 0
                    currTable['ncpu'] = 0
            
            line = self.stream.readline()

        if currTable <> None:                            
            self.container.append(currTable)
        
    #end of thread

def parse(nodeList=[""], filename=None):

    ncpu = 0
    njob = 0
    
    for nodeId in nodeList:
        if filename:
            cmd = shlex.split('cat ' + filename)
        else:
            cmd = shlex.split('pbsnodes -a ' + nodeId)
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        stdout_thread = PBSNodesHandler(process.stdout)
        stderr_thread = ErrorHandler(process.stderr)
        
        stdout_thread.start()
        stderr_thread.start()
        
        ret_code = process.wait()
        
        stdout_thread.join()
        stderr_thread.join()
            
        if ret_code <> 0:
            raise Exception(stderr_thread.message)
                
        if len(stdout_thread.errList) > 0:
            raise Exception(stdout_thread.errList[0])
            
        for nodeTable in stdout_thread.container:
            if nodeTable['up']:
                ncpu += nodeTable['ncpu']
                njob += nodeTable['njob']
            
    return (ncpu, max(ncpu - njob, 0))


if __name__ == "__main__":
    
    print parse([''], sys.argv[1])
    
    
