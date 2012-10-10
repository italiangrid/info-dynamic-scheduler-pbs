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


# Classes representing a generic LRMS (Local Resource Management
# System) -- i.e. a batch queue more or less.

# This defines the main interface to any type of LRMS, to be used
# with the start-time prediction stuff (gott and friends).  One
# would presumably not import this module directly, rather one
# would make a derived class that inherits from these (Server)
# and contains guts appropriate to fill in the various fields.

class Job:

    # simple class to represent all the information we need about
    # a job.  If a dictionary is passed, it is expected to hold
    # all the info about the job.  If not, an empty job is created,
    # set using the Job.set() function.

    def __init__(self, attdict=None):
        if not attdict:
            self.__info__ = {}
        else:
            self.__info__ = attdict

    def set(self,key,val):
        self.__info__[key] = val

    def get(self,key):
        if key not in self.__info__.keys():
            return None
        else:
            return self.__info__[key]
    
    def __contains__(self, key):
        return key in self.__info__

    def attDictString(self):
        return repr(self.__info__)
    
    def __str__(self):
        if self.get('jobid'):
            ostr = 'jobid :: ' + self.get('jobid')
        else:
            ostr = 'jobid :: Unknown'
        if self.get('user'):
            ostr = ostr + '; user :: ' + self.get('user')
        if self.get('group'):
            ostr = ostr + '; group :: ' + self.get('group')
        if self.get('queue'):
            ostr = ostr + '; queue :: ' + self.get('queue')
        if self.get('state'):
            ostr = ostr + '; state :: ' + self.get('state')
        return ostr
    
def filt2str(d):
    """
    Given a dictionary containing selection criterion on which to search the job database,
    return a string suitable for indexing into cached copies of the selection. To ensure
    reproducibility, sort the keys, then make key.val.key.val... string.
    """
    sk = d.keys()
    sk.sort()
    result =""
    for k in sk:
        if result != "":
            result = result + "."
        result = result + "%s.%s" % (k, d[k])
    return result

class Server(object):
    def __init__(self,schedCycle=26):
        self.__jobdict__    = { }
        self.__evtime__     =  0   # set this to be time of current event

        self.__scache__ = {}  # caches results of various slices to speed up
        
        # attributes for 'slots' ... using PBS we don't
        # necessarily have access to info about physical CPUs.

        # slotsUp means slots that can run, or are running, jobs right now.
        # so 'Up' in the sense of 'uptime'.  slotsFree are those 'up'
        # slots that don't have jobs assigned to them right now.
        
        self.__slotsUp__   = -1
        self.__slotsFree__ = -1
        
        # note: if your lrms doesn't have the concept of a scheduler cycle, the
        # appropriate value would be twice the average time it takes from handing a
        # job to your lrms, until it starts to actually run (assuming the job
        # doesn't have to wait for a free slot first).
        
        self.schedCycle = schedCycle

    def jobs(self) :
        """
        Return list of Job objects corresponding to all jobs known to
        the system
        """
        return self.__jobdict__.values()

    def matchingJobs(self, **filtd):
        """
        Return list of Job objects that corresponds to all jobs known to
        the system that match the attributes provided in dictionary filtd.
        This function and the later nmatch do essentially the same thing;
        a future release should rationalize this behavior.
        """
        indstr = filt2str(filtd)

        if indstr not in self.__scache__.keys():
            self.__scache__[indstr] = self.mkview(filtd)
        return self.__scache__[indstr].values()




    def get_slotsUp(self): 
        return self.__slotsUp__
        
    def set_slotsUp(self,n): 
        self.__slotsUp__ = n

    slotsUp = property(get_slotsUp,set_slotsUp,doc="return number of job slots "+\
                       "that are online (up) and controlled by this server")

    def get_slotsFree(self): 
        return self.__slotsFree__
        
    def set_slotsFree(self,n): 
        self.__slotsFree__ = n

    slotsFree = property(get_slotsFree,set_slotsFree,doc="return number of " +\
                         "free job slots")

    def get_snaptime(self):
        return self.__evtime__
        
    def set_snaptime(self, timestamp): 
        self.__evtime__ = timestamp

    now = property(get_snaptime, set_snaptime, doc="timestamp in seconds " +\
                   "specifying when the state currently in this server " +\
                   "object was captured")


    
    def mkview(self,filtd):
        """
        Construct a dict of all jobs matching criteria in dict 'filtd',
        and put the result in the search cache
        """
        #indstr = filt2str(filtd)
        #if indstr in self.__scache__.keys() :
        #    print 'blew it somewhere, trying to create a pre-existing cache!'
        #    return self.__scache__[indstr]
        
        reslt = {}
        for id in self.__jobdict__.keys() :
            j = self.getjob(id)
            match = 1
            for k in filtd.keys():
                if j.get(k) != filtd[k] :
                    match = 0
                    break
            if match == 1 :
                reslt[id] = j
        return reslt

    # returns number of jobs matching filter criteria
    # also caches the 'index' string from the query to be backwards
    # compatible with the original.
    
    def nmatch(self,filtd={}) :
        if len(filtd) == 0 :
            self.__lastquery__ = ''
            return len(self.__jobdict__)
        else:
            indstr = filt2str(filtd)
            self.__lastquery__ = indstr
            if indstr in self.__scache__.keys():
                return len(self.__scache__[indstr])
            else:
                self.__scache__[indstr] = self.mkview(filtd)
                return len(self.__scache__[indstr])
                

    def jobs_last_query(self):
        if self.__lastquery__ == '':
            return self.__jobdict__.values()
        else:
            return self.__scache__[self.__lastquery__].values()

    def jobids_last_query(self):
        if self.__lastquery__ == '':
            return self.__jobdict__.keys()
        else:
            return self.__scache__[self.__lastquery__].keys()
                    
    def getjobids(self, filter={}):
        if len(filter) == 0 :
            newlist = self.__jobdict__.keys()
            return newlist
        else:
            newlist = []
            for j in self.__jobdict__.keys():
                match = 1
                for k in filter.keys():
                    if self.__jobdict__[j].get(k) != filter[k] :
                        match = 0
                if match == 1:
                    newlist.append(j)
        return newlist

    def getjob(self,jid) :
        return self.__jobdict__[jid]
        
    def addjob(self,jid,job) :
        self.__jobdict__[jid] = job

    def deletejob(self,jid) :
        del self.__jobdict__[jid]
    
    
    def cachejob(self, filtd, item=None):
        indstr = lrms.filt2str(filtd)
        if item == None :
            self.__scache__[indstr] = {}
        else :
            if indstr not in self.__scache__.keys() :
                self.__scache__[indstr] = {}
            
            self.__scache__[indstr][item.get('jobid')] = item

