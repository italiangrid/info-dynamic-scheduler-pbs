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
import os, os.path
from threading import Thread

from TorqueInfoUtils import CommonUtils



class DiagnoseHandler(Thread):

    def __init__(self):
        Thread.__init__(self)
        self.errList = list()
        self.limitTable = dict()
        self.limRegex = re.compile("MAXJOB=([0-9,]+)")

    def setStream(self, stream):
        self.stream = stream

    def run(self):
        gpos = -1
        lpos = -1
        
        line = self.stream.readline()
        while line:
            
            try:
                fields = line.split()
            
                # find table header
                if gpos < 0 and lpos < 0:
                    try:
                        gpos = fields.index('Name')
                        lpos = fields.index('Limits')
                        tmppos = fields.index('Priority')
                    except:
                        gpos = -1
                        lpos = -1
                    continue
            
                if gpos <> -1 and lpos <> -1 and len(fields) > max(gpos, lpos):
                    
                    # expect either [NONE] or MAXJOB=N or MAXJOB=N,M
                    
                    group = fields[gpos]
                    limit = fields[lpos]
                    parsed = self.limRegex.search(limit)
                    if parsed:
                        tmpl = parsed.group(1).split(',')
                        if len(tmpl) > 1:
                            self.limitTable[group] = int(tmpl[1])
                        elif len(tmpl) > 0:
                            self.limitTable[group] = int(tmpl[0])
            
            finally:
                line = self.stream.readline()



def parseJobLimit(pbsHost=None, keyfile=None, filename=None):
    if filename:
        cmd = shlex.split('cat ' + filename)
    else:
        tmps = 'diagnose -g'
        if pbsHost:
            tmps += ' --host=%s' % pbsHost
        if keyfile:
            tmps += ' --keyfile=%s' % keyfile
        cmd = shlex.split(tmps)

    container = DiagnoseHandler()
    CommonUtils.parseStream(cmd, container)
    return container


def available():

    for pDir in os.environ['PATH'].split(':'):
        if os.path.exists(os.path.join(pDir, 'diagnose')):
            return True
    return False




