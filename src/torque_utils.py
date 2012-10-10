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

from __future__ import generators # only needed in Python 2.2
import os,re

# The programs needed by these utilities. If they are not in a location
# accessible by PATH, specify their location here.
PBSNODES = "pbsnodes"
MOMCTL = "/root/momctl"
QSTAT = "qstat"

def pbsnodes(nodes="", ignoreError=False):
    """
    Interface to the pbsnodes command. It returns a list of nodes found by PBS.
    Specify the list of nodes as a comma-separated string.

    Example usage:

    for node in pbsnodes():
        if node.isUp():
            print node.name, "average load = ", node.status['loadave']
        else:
            print node.name, "is down"

    See the __doc__ string of the nodes returned by pbsnodes() for a list
    of the attributes and methods supported by the node object returned
    by pbsnodes.

    The following keyword arguments are supported:
        ignoreError (True/False): ignore errors in processing individual pbsnodes queries.

    If ignoreError is False and an error is encountered issuing pbsnodes commands, a
    TorqueError exception will be raised.
    """
    if not _pbsnodesOK:
        raise IOError, "'%s' not found" % PBSNODES

    if not nodes:
        nodes = [""] # meaning all nodes
    else:
        nodes = nodes.split(',')

    result = []
    for node in nodes:
        fh = os.popen("%s -a %s" % (PBSNODES,node))
        s = fh.readlines()
        exitcode = fh.close()
        if exitcode:
            if ignoreError:
                continue
            else:
                raise TorqueError, ''.join(s)

        # A generator-based version would be:
        # for entry in _paragraphs(os.popen("pbsnodes -a")): yield PBSNode(entry)
        for entry in _paragraphs(s):
            result.append(PBSNode(entry))

    return result

def qstat(jobs=""):
    if not jobs:
        jobs = [""] # meaning all jobs
    else:
        jobs = jobs.split(',')

    result = []
    for job in jobs:
        fh = os.popen("%s -f %s" % (QSTAT,job))
        s = fh.readlines()
        exitcode = fh.close()
        if exitcode:
            raise TorqueError, ''.join(s)
        
        for entry in _paragraphs(s):
            result.append(PBSJob(entry))
    
    return result

def mom(node):
    """
    Interface to the MOM process on a given node.
    """
    if not _momctlOk:
        raise IOError, "'%s' not found" % MOMCTL

    return MOMNode(node)

class PBSJob(object):
    def __init__(self, para):
        # step1, split on newlines
        step1 = para.split('\n')
        self.header = step1[0]
        step1 = step1[1:] # remove header line

        # step2, join lines broken on \t characters, removing the \t
        step2 = []
        for brokenline in step1:
            line = re.sub('\t+','',brokenline).strip()
            if line.find(' = ') != -1:
                step2.append(line) # this is the start of a real entry
            else:
                step2[-1] += line  # append this line to the previous one

        # now set instance variables, splitting on 'KEY = VALUE'
        for entry in step2:
            key,val = entry.split(' = ')
            self.__dict__[key] = val

    def __str__(self):
        return self.header

class PBSNode(object):
    """
    The representation of a node as reported by Torque.

    Public attributes:
        name (node name:string)
        state (node state:string)
        ntype (node type:string)
        np (number of processors:string)
        properties (node properties:list)
        jobs (jobs running on node:dictionary)
        status (node status:dictionary)
        numCpu (number of CPUs:integer)
        freeCpu (number of free CPUs:integer)

    Public methods:
        isUp()    -- return 1 if node is up
        isDown()  -- return 1 if node is down

    This class is not meant for direct instantiation.
    """
    def __init__(self,para):
        lines = para.splitlines()
        params = ['state','np','properties','ntype','jobs','status']
        self.name = lines[0]
        for line in lines[1:]:
            for param in params:
                m = re.match('^\s+%s = (.*)' % param, line)
                if m:
                    # params are named "_NAME", where NAME is the pbsnodes attribute
                    self.__dict__['_%s' % param] = m.group(1)
                    continue
        # make sure all the required params are in __dict__
        # and set e.g. state = _state (this can be overridden later with properties)
        for param in params:
            self.__dict__.setdefault('_%s' % param,"")
            self.__dict__[param] = self.__dict__['_%s' % param]
    def __str__(self):
        return self.name
    def isUp(self):
        return (self._state.find('down')==-1 and self._state.find('offline')==-1)
    def isDown(self):
        return not self.isUp()

    def _getNumCpu(self):
        try:
            return int(self.np)
        except ValueError:
            return 0
    def _getFreeCpu(self):
        if self.isDown(): return 0

        free = int(self.np)
        myJobs = self._getJobs()
        for cpu in range(free):
            if len(myJobs[cpu]): free = free-1
    
        return free
    def _getProperties(self):
        return [s.strip() for s in self._properties.split(',')]
    def _getJobs(self):
        #return [s.strip()[2:] for s in self._jobs.split(',')]
        d = {}
        for cpu in range(int(self.np)):
            d[cpu] = []

        for entry in self._jobs.split(','):
            try:
                key,val = entry.split('/')
            except ValueError:
                break
            key,val = int(key.strip()),val.strip()
            d[key].append(val)

        return d
    def _getStatus(self):
        d = {}
        for entry in self._status.split(','):
            try:
                key,val = entry.split('=')
            except ValueError:
                break
            (key,val) = (key.strip(),val.strip())
            d[key] = val
        return d

    # class attributes accessed via properties:
    properties = property(_getProperties)
    jobs = property(_getJobs)
    status = property(_getStatus)
    freeCpu = property(_getFreeCpu)
    numCpu = property(_getNumCpu)

class MOMNode(object):
    """
    The reprentation of a MOM process as reported by momctl.
    This class must be instantiated with the node name as parameter.

    Public attributes:
        name (node name:string)
        server (torque server name:string)
        version (torque version:string)
        jobs (jobs running on node:string)
        directory (torque directory:string)
        active(seconds since process active:string)
    """
    def __init__(self,node):
        # horrible parsing due to lack of structure in the output
        self.jobs = []
        for line in os.popen("%s -d 0 -h %s" % (MOMCTL,node)):
            if line.startswith("Host"):
                m = re.search('Host:\s+(\S+)\s+Server:\s+(\S+)\s+Version:\s+(\S+)',line)
                self.name = m.group(1)
                self.server = m.group(2)
                self.version = m.group(3)
            elif line.startswith("HomeDirectory"):
                m = re.search('HomeDirectory:\s+(\S+)',line)
                self.directory = m.group(1)
            elif line.startswith("MOM"):
                m = re.search('MOM active:\s+(.*)',line)
                self.active = m.group(1)
            elif line.startswith("Job"):
                m = re.search('.*\[(.*)\]',line)
                self.jobs.append(m.group(1))

class TorqueError(Exception):
    pass

def _checkProgram(*args):
    """
    Check if the programs passed as argument are found in the current PATH
    and return a list with corresponding True or False values.
    """
    
    ret = []
    dirnames = os.environ["PATH"].split(os.pathsep)
    for app in args:
        for dirname in dirnames:
            filename = os.path.join(dirname, app)
            if (os.path.exists(filename) and
                os.path.isfile(filename) and
                os.access(filename, os.X_OK)):
                ret.append(True)
                break
        else:
            ret.append(False)

    return ret

def _paragraphs(file, separator=None):
    if not callable(separator):
        def separator(line): return re.match('^\s+$',line)
    paragraph = []
    for line in file:
        if separator(line):
            if paragraph:
                yield ''.join(paragraph)
                paragraph = []
        else:
            paragraph.append(line)
    if paragraph: yield ''.join(paragraph)

(_pbsnodesOK, _momctlOK) = _checkProgram(PBSNODES, MOMCTL)
