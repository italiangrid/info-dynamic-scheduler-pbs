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
import shlex
import subprocess
import traceback
import glob
import ConfigParser
import logging
from threading import Thread

logger = logging.getLogger("CommonUtils")

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



bdiiCfgRegex = re.compile('^\s*BDII_([^=\s]+)\s*=(.+)$')

def getBDIIConfig(bdiiConffile):

    result = dict()
    
    cFile = None
    try:
        cFile = open(bdiiConffile)
        
        for line in cFile:
            parsed = bdiiCfgRegex.match(line)
            if parsed:
                result[parsed.group(1).lower()] = parsed.group(2).strip()

    finally:
        if cFile:
            cFile.close()
    
    return result


glue1DNRegex = re.compile("dn:\s*GlueCEUniqueID\s*=\s*.+")
glue1QueueRegex = re.compile("GlueCEName\s*:\s*(.+)")

glue2DNRegex = re.compile("dn:\s*GLUE2ShareID\s*=\s*.+")
glue2ShareRegex = re.compile("GLUE2ComputingShareMappingQueue\s*:\s*(.+)")

managerRegex = re.compile("dn:\s*GLUE2ManagerId\s*=\s*.+")
manAttrRegex = re.compile("GLUE2ManagerID\s*:\s*(.+)")

def parseLdif(bdiiConffile, glueType):

    bdiiConfig = getBDIIConfig(bdiiConffile)

    if 'ldif_dir' in bdiiConfig:
        ldifDir = bdiiConfig['ldif_dir']
    else:
        ldifDir = '/var/lib/bdii/gip/ldif'
    
    ldifList = glob.glob(ldifDir + '/*.ldif')
    
    if glueType =='GLUE1':
    
        result = dict()
        
        #
        # Shortcut for old installations
        #
        scFilename = ldifDir + '/static-file-CE.ldif'
        if scFilename in ldifList:
            ldifList = [scFilename]
        
        for ldifFilename in ldifList:
        
            ldifFile = None
            currDN = None
            try:
            
                ldifFile = open(ldifFilename)
                for line in ldifFile:
                    parsed = glue1DNRegex.match(line)
                    if parsed:
                        currDN = line.strip()
                        continue
                    
                    parsed = glue1QueueRegex.match(line)
                    if parsed and currDN:
                        result[currDN] = parsed.group(1).strip()
                        continue
                    
                    if len(line.strip()) == 0:
                        currDN = None

            finally:
                if ldifFile:
                    ldifFile.close()

    else:
    
        result = (dict(), dict())

        #
        # Shortcut for old installations
        #
        scFilename1 = ldifDir + '/ComputingManager.ldif'
        scFilename2 = ldifDir + '/ComputingShare.ldif'
        if scFilename1 in ldifList and scFilename2 in ldifList:
            ldifList = [scFilename1, scFilename2]

        for ldifFilename in ldifList:
        
            ldifFile = None
            currDN1 = None
            currDN2 = None
            try:
            
                ldifFile = open(ldifFilename)
                for line in ldifFile:
                    parsed = glue2DNRegex.match(line)
                    if parsed:
                        currDN1 = line.strip()
                        continue
                    
                    parsed = glue2ShareRegex.match(line)
                    if parsed and currDN1:
                        result[0][currDN1] = parsed.group(1).strip()
                        continue
                    
                    parsed = managerRegex.match(line)
                    if parsed:
                        currDN2 = line.strip()
                        continue
                    
                    parsed = manAttrRegex.match(line)
                    if parsed and currDN2:
                        result[1][currDN2] = parsed.group(1).strip()
                        continue
                    
                    if len(line.strip()) == 0:
                        currDN1 = None
                        currDN2 = None

            finally:
                if ldifFile:
                    ldifFile.close()

    return result

def readConfigFile(configFile):

    conffile = None
    config = dict()
    vomap = dict()
    
    try:
    
        tmpConf = ConfigParser.ConfigParser()
        conffile = open(configFile)
        tmpConf.readfp(conffile)
            
        if tmpConf.has_option('Main','outputformat'):
            config['outputformat'] = tmpConf.get('Main', 'outputformat').lower()
        else:
            config["outputformat"] = "both"
                
        if tmpConf.has_option('Main','bdii-configfile'):
            config['bdii-configfile'] = tmpConf.get('Main', 'bdii-configfile')
        else:
            config["bdii-configfile"] = '/etc/bdii/bdii.conf'
                
        if tmpConf.has_option('Main','vomap'):
            lines = tmpConf.get('Main','vomap').split('\n')
            for line in lines:
                tmpl = line.split(':')
                if len(tmpl) == 2:
                    group = tmpl[0].strip()
                    vo = tmpl[1].strip()
                    vomap[group] = vo

        if tmpConf.has_option('LRMS','pbs-host'):
            config['pbs-host'] = tmpConf.get('LRMS', 'pbs-host')
        else:
            config["pbs-host"] = None
    
        if tmpConf.has_option('WSInterface','status-probe'):
            config['status-probe'] = tmpConf.get('WSInterface', 'status-probe')
    
    finally:
        if conffile:
            conffile.close()

    config['vomap'] = vomap
    
    if config["outputformat"] not in ["glue1", "glue2", "both"]:
        raise Exception("FATAL: Unknown output format specified in config file:%s" % config["outputformat"])

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


def interfaceIsOff(config):
    try:
    
        if 'status-probe' in config:
            retcode = subprocess.call(shlex.split(config['status-probe']))
            return retcode == 1 or retcode == 2
        
    except:
        logger.debug("Error running %s", config['status-probe'], exc_info=True)
    
    return False




