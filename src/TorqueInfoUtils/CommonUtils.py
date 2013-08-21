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
import subprocess
import traceback
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


def parseStream(cmd, container):

    processErr = None
    
    try:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
        container.setStream(process.stdout)
        stderr_thread = ErrorHandler(process.stderr)
    
        container.start()
        stderr_thread.start()
    
        ret_code = process.wait()
    
        container.join()
        stderr_thread.join()
        
        if ret_code <> 0:
            processErr = stderr_thread.message
            
        if len(container.errList) > 0:
            processErr = container.errList[0]

    except:
        raise Exception(errorMsgFromTrace())

    if processErr:
        raise Exception(processErr)



def readDNsAndAttr(filename, dnRE, attrRE):
    result = dict()
    ldifFile = None
    try:
    
        ldifFile = open(filename)
        
        for line in ldifFile.readlines():
            
            tmpm = dnRE.match(line)
            if tmpm <> None:
                currDN = line.strip()
                continue
                
            tmpm = attrRE.match(line)
            if tmpm <> None:
                result[currDN] = tmpm.group(1).strip()
        
    finally:
        if ldifFile:
            ldifFile.close()

    return result

def parseGLUE1Queues(filename):

    glue1DNRegex = re.compile("dn:\s*GlueCEUniqueID\s*=\s*[^$]+")
    glue1QueueRegex = re.compile("GlueCEName\s*:\s*([^$]+)")

    return readDNsAndAttr(filename, glue1DNRegex, glue1QueueRegex)


def parseGLUE2Shares(filename):

    glue2DNRegex = re.compile("dn:\s*GLUE2ShareID\s*=\s*[^$]+")
    glue2ShareRegex = re.compile("GLUE2ComputingShareMappingQueue\s*:\s*([^$]+)")
    
    return readDNsAndAttr(filename, glue2DNRegex, glue2ShareRegex)


def parseGLUE2Managers(filename):

    managerRegex = re.compile("dn:\s*GLUE2ManagerId\s*=\s*[^$]+")
    manAttrRegex = re.compile("GLUE2ManagerID\s*:\s*([^$]+)")
    
    return readDNsAndAttr(filename, managerRegex, manAttrRegex)


def readConfigFile(configFile):

    pRegex = re.compile('^\s*([^=\s]+)\s*=([^$]+)$')
    conffile = None
    config = dict()
    
    try:
    
        conffile = open(configFile)
        for line in conffile:
            parsed = pRegex.match(line)
            if parsed:
                config[parsed.group(1)] = parsed.group(2).strip(' \n\t"')
            else:
                tmps = line.strip()
                if len(tmps) > 0 and not tmps.startswith('#'):
                    raise Exception("Error parsing configuration file " + configFile)

    finally:
        if conffile:
            conffile.close()

    return config


def errorMsgFromTrace():

    etype, evalue, etraceback = sys.exc_info()
    trMessage = ''
    
    trList = traceback.extract_tb(etraceback)
    for trArgs in trList:
        if 'TorqueInfoUtils' in trArgs[0]:
            trMessage = '%s: %d' % (trArgs[0], trArgs[1])
    
    result = '%s (%s)' % (evalue, trMessage)
    return result



