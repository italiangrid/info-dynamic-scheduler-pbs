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

# Two classes derived from lrms.Server are included; one for
# working with a live PBS server, and one for representing historical
# states of the server from PBS accounting log files.  A third class
# (History) is associated with the historical server class.

import commands
import grp
import pwd
import re
import string
import sys
import time
import copy

from lrms import *        # base Server class, Job class
from torque_utils import pbsnodes


class LiveServer(Server):

    def __init__(self,*arg,**kw):

        Server.__init__(self)
        
        cnow = time.ctime() # this hack is to get around time zone problems
        if 'file' in kw.keys() and kw['file'] != None:
            cmdstr = '/bin/cat ' + kw['file']
        else:
            cmdstr = 'qstat -f'
        (stat, qstatout) = commands.getstatusoutput(cmdstr)
        if stat:
            print 'problem getting qstat output; cmd used was', cmdstr
            print 'returned status ', stat, ', text: ', qstatout

        cpucount = 0
        jobcount = 0
        for node in pbsnodes():
            if node.isUp():
                cpucount += node.numCpu
                for cpu in range(node.numCpu):
                    jobcount += len(node.jobs[cpu])
        self.slotsUp   = cpucount
        self.slotsFree = cpucount - jobcount

        nowtuple = time.strptime(cnow,"%c")
        self.__evtime__ = int(time.mktime(nowtuple))
        
        keyvalpat = re.compile(r'   (\S+) = (.*)')
        
        # guard against empty or nonconforming qstat output
        if len(qstatout) == 0:
            verbose_job_info = []  # assume valid output for system with no jobs running!
        elif qstatout.find('Job Id') != 0:
            print 'fatal error, qstat output starts with something other than a job id'
            print 'first few characters are:', qstatout[:min(32,len(qstatout))]
            sys.exit(4)
        else:
            verbose_job_info=string.split(qstatout,"\n\n")
 
        for j in verbose_job_info:
            newj = Job()
            lines = string.split(j,'\n ')
            qstatDict = dict()
            for ll in lines:
                if string.find(ll,'Job Id') == 0:
                    newj.set('jobid',string.split(ll)[2])
                    continue
                l = ll.replace('\n\t','')
                mk = keyvalpat.match(l)
                if not mk:
                    print "fatal error, should always be able to find a match"
                    print "unmatchable string was:", l
                    sys.exit(3)
                qstatDict[mk.group(1)] = mk.group(2)
                
            #    print qstatDict.keys()

            # do owner and group.  try euser and group first, if not found, back off to old method,
            # which was "user@host" parsing plus getgrid on user.

            keysfound = qstatDict.keys()
            if 'euser' in keysfound and 'egroup' in keysfound:
                newj.set('user', qstatDict['euser'])
                newj.set('group',qstatDict['egroup'])
            elif 'Job_Owner' in keysfound:
                user_and_host = qstatDict['Job_Owner']
                user = string.split(user_and_host,'@')[0]
                newj.set('user',user)
                try: 
                   thisgroup=pwd.getpwnam(user)[3]
                   groupname=grp.getgrgid(thisgroup)[0]
                except:
                   thisgroup='unknown'
                   groupname='unknown'
                newj.set('group',groupname)
            else:
                print "Can't find user and group of job", newj.get('jobid')
                sys.exit(4)

            # do job state
            if 'job_state' in keysfound:
                statelett = qstatDict['job_state']
                if statelett in ['Q','W']:
                    val = 'queued'
	        elif statelett in ['R','E']:
                    val = 'running'
	        elif statelett in ['H', 'T']:
                    val = 'pending'
	        elif statelett == 'C':
                    val = 'done'
                else:
                    val = 'unknown'
                newj.set('state',val)
            else:
                newj.set('state','unknown')
            
            newj.set('queue', qstatDict['queue'])

            if 'qtime' in keysfound:
                timestring = qstatDict['qtime']
                timetuple = time.strptime(timestring,"%c")
                newj.set('qtime',time.mktime(timetuple))
            if 'Resource_List.walltime' in keysfound:
                hms = qstatDict['Resource_List.walltime']
                t = string.split(hms,':')
                secs = int(t[2]) + \
                       60.0*(int(t[1]) + 60*int(t[0]))
                newj.set('maxwalltime',secs)

            # do start and wall times.  default is to use qstat "start_time" for start times and
            # qstat "resources_used.walltime" for walltime.  Code can use walltime for start and
            # start for walltime in situations where one is missing.

            if 'start_time' in keysfound:
                timestring = qstatDict['start_time']
                timetuple = time.strptime(timestring,"%c")
                newj.set('start',time.mktime(timetuple))
                newj.set('startAnchor', 'start_time')
                if 'resources_used.walltime' not in keysfound:   # just after job starts, walltime is not printed
                    newj.set('walltime', self.__evtime__ - newj.get('start'))

            if 'resources_used.walltime' in keysfound:
                hms = qstatDict['resources_used.walltime']
                t = string.split(hms,':')
                secs = int(t[2]) + \
                       60.0*(int(t[1]) + 60*int(t[0]))
                newj.set('walltime', secs)
                if 'start_time' not in keysfound:  # older torque version
                    start = self.__evtime__ - secs
                    newj.set('start',start)
                    newj.set('startAnchor', 'resources_used.walltime')
	    if 'Job_Name' in keysfound:
                jnam = qstatDict['Job_Name']
	        newj.set('name',jnam)
	    if 'exec_host' in keysfound:
	        hlist = qstatDict['exec_host']
	        ncpu = hlist.count('+') + 1
	        newj.set('cpucount',ncpu)
                    
            self.addjob(newj.get('jobid'),newj)

## following is helper for class Event.  superseded by newer keyvallist2dict function.
## takes as arg a string of key=val pairs, returns a dict with the same
## structure.  example input string:
## user=tdykstra group=niktheorie jobname=Q11_241828.gjob

def keyval2dict(astring):
    flds = string.split(astring)
    d = {}
    for f in flds:
        kv=f.split("=",1)
        if len(kv) == 2:
            d[kv[0]] = kv[1]
        else:
            print f
            print kv
            raise CantHappenException
    return d

## following is helper for class Event.
## takes as arg a list of key=val pairs, returns a dict with the same
## structure.  example input string:
## ['user=tdykstra', 'group=niktheorie', 'jobname=Q11_241828.gjob']

def keyvallist2dict(kvlist):
    d = {}
    for f in kvlist:
        kv=f.split("=",1)
        if len(kv) == 2:
            d[kv[0]] = kv[1]
        else:
            print "tried to split:", f, ", result was:", kv
            raise CantHappenException
    return d

class Event:

    # simple class to represent events like job queued, job started, etc.

    def __init__(self,evstring,debug=0):

        self.__time__   = None          # default values
        self.__type__   = None
        self.__jobid__  = None
        self.__info__   = { }

        # search pattern for parsing string using "re" module
        # for successful search, fields are:
        # 1) timestamp
        # 2) event type (Q,S,E,D, etc)
        # 3) local PBS jobID
        # 4) rest of line (key=value) to be parsed otherwise
        # this structure is matched by evpatt
        
        evpatt = "^(.+);([A-Z]);(.+);(.*)"
        m = re.search(evpatt,evstring)
        if not m:
            print "parse patt failed, offending line is"
            print evstring
            return
        if debug:
            print "timestamp", m.group(1)
            print "code", m.group(2)
            print "jobid", m.group(3)
            print "attrs", m.group(4)

        # tpatt matches strings of form key=val
        # lookahead assertion is necessary to work around presence of ' ' and '=' in some
        # 'val' strings (like account, or neednodes with multiple processors)
        
        tpatt  = r'[a-z._A-Z]+=[a-z0-9A-Z=/: -@_]+?(?=$| [a-z._A-Z]+=)'
        tprog  = re.compile(tpatt)
        tmatch = tprog.findall(m.group(4))
        if debug:
            print "result of key=val match pattern:", tmatch

        # parse timestamp

        ttup=time.strptime(m.group(1),"%m/%d/%Y %H:%M:%S")

        # last element of time tuple is DST, but PBS log files
        # don't specify time zone.  Setting the last element of
        # the tuple to -1 asks libC to figure it out based on
        # local time zone of machine

        atup = ttup[:8] + (-1,)
        self.__time__   = int(time.mktime(atup))
        self.__type__   = m.group(2)
        self.__jobid__  = m.group(3)
        self.__info__   = keyvallist2dict(tmatch)

    def time(self):
        return self.__time__
    def type(self):
        return self.__type__
    def jobid(self):
        return self.__jobid__
    def info(self,key=''):
        if key == '':
            return self.__info__
        else:
            if key in self.__info__.keys():
                return self.__info__[key]
            else:
                return None
    def __str__(self):
        return self.__jobid__ + ' :: event type ' + self.__type__ + \
               ' at ' + str(self.__time__)

class History:

    # the idea here is to generate a couple of data structures:
    # first the job catalog, which associates things like
    # queue entry time, start time, stop time, owner username/groupname,
    # etc. with each PBS jobid.
    # second the event list, which records when jobs enter the various
    # queues, start to execute, and terminate.  This list only holds
    # a timestamp, jobid, and event type.  This seems to be the
    # minimal amount of information we'll need to be able to generate
    # the state of the queue at any given arbitrary time.

## functions for using the job catalog.  This beast is a dictionary
## that has one entry (an object of class Job) per job seen in the
## log files.

    def addjob(self, jobid, job) : # add new job entry
        if self.hasjob(jobid):
            print "job already found, exiting"
            sys.exit(1)
        job.set('jobid',jobid)
        self.__jobcat__[jobid] = job

    def hasjob(self,jobid) : # test if job is already in catalogue
        if jobid in self.__jobcat__.keys():
            return 1
        else:
            return 0
    
    def setjobinfo(self,jobid,key,val) : # add or set info to/for existing job
        if not self.hasjob(jobid):
            print "job not found:", jobid, key, val
            return 0
        self.__jobcat__[jobid].set(key,val)
        return 1

    def getjobinfo(self,jobid,key) :     # get info for key from job jobid
        if not self.hasjob(jobid):
            print "job not found:", jobid, key
            return 0
        return self.__jobcat__[jobid].get(key)

    def getjoblist(self):
        return self.__jobcat__.keys()

    def getjob(self,jobid):
        return self.__jobcat__[jobid]

## functions for using event list.  This beast is just a list (in sequence)
## of all events seen while parsing the log files

    def getfirst_event_time(self):
        return self.__evlist__[0].time()
	
    def getevent(self,index):
        return self.__evlist__[index]
            
## functions for using the job event list.  This beast is just a dictionary
## (key=jobid), so only entry in dict for each job; the value for each
## jobid is a list of events seen for this job (in sequence).  Rationale
## for this object is it can help in resolving ambiguous cases (like
## multiple "D" events seen).

    def getjobevts(self,jobid):
        if jobid not in self.__jobevs__.keys():
            print 'getjobevs: jobid', jobid, 'not found in job_event db'
            sys.exit(1)
        return self.__jobevs__[jobid]

    def addevt(self,ev):
## append to event list
        self.__evlist__.append(ev)
## append to job event struct; create entry for job if needed
        if ev.jobid() not in self.__jobevs__.keys():
            self.__jobevs__[ev.jobid()] = [ev]
        else:
            self.__jobevs__[ev.jobid()].append(ev)

    def __init__(self,logfilelist):

        self.__evlist__ = [ ]
        self.__jobcat__ = { }
        self.__jobevs__ = { }
                
        for f in logfilelist:
            inf = open(f,'r')
            line = inf.readline()
            while line:
                line = line[:-1] # chop off trailing newline
                ev = Event(line) # parse line, create event
                self.addevt(ev)  # adds event to evlist and jobevt struct
## vars for convenience in typing:
                if ev.type() == 'Q':          # job enters sys for 1st time
                    if self.hasjob(ev.jobid()): # should not happen
                        print 'Error, Q event seen for pre-existing job', \
                              ev.jobid()
                        sys.exit(1)
                    newentry = Job()       # make new entry, fill info
                    newentry.set('qtime',ev.time())
                    newentry.set('queue',ev.info('queue'))
                    self.addjob(ev.jobid(),newentry)
                else:
## for all other event types
                    if not self.hasjob(ev.jobid()):
                        newentry = Job()       # make new entry
                        self.addjob(ev.jobid(),newentry)
                    job = self.getjob(ev.jobid())
                    if ev.info('qtime'):
                        job.set('qtime',int(ev.info('qtime')))
                    if ev.info('queue') and not job.get('queue'):
                        job.set('queue',ev.info('queue'))
                    if ev.info('user') and not job.get('user'):
                        job.set('user',ev.info('user'))
                    if ev.info('group') and not job.get('group'):
                        job.set('group',ev.info('group'))
                    if ev.info('start') and not job.get('start'):
                        job.set('start',int(ev.info('start')))
                    if ev.info('Resource_List.walltime') and not \
                       job.get('maxwalltime'):
                        hms = ev.info('Resource_List.walltime')
                        (h,m,s) = hms.split(":")
                        maxwallsecs = int(h) * 3600 + int(m) * 60 + int(s)
                        job.set('maxwalltime',maxwallsecs)
                    if ev.info('end') and not job.get('end'):
                        job.set('end',int(ev.info('end')))

## special handling for troublesome event types

                if ev.type() == 'T':           # job restart after checkpoint
## previous job start record may not have been seen; if not, we don't
## know if it was running or queued before, so just set current event time
## as start since we know it's running now.
                    if not job.get('start'):
                        job.set('start',ev.time())
## in some cases the T record may be the first (or even only) record
## we see.  in that case, the only thing we can say is that the qtime
## was before the beginning of the log files ... set qtime like that.
## if we see a later record for this job, that record will set the correct
## qtime.
                    if not job.get('qtime'):
                        job.set('qtime',self.getfirst_event_time()-1)

                line = inf.readline()

    def get_evlist(self):
        return self.__evlist__

class LogFileServer(Server):
    def __init__(self,history,debug=0):

        Server.__init__(self)
        
        self.__history__ = history
# want to get first event on first getnextevent call, so set initial index
# to -1 (one previous to 0!)
        self.__evindx__  = -1 
        first_event = history.get_evlist()[0]
        if debug:
            print 'first event data:'
            print first_event.jobid(), first_event.type(), first_event.time()
        starttime = first_event.time()
        jobidlist = history.getjoblist()
        jobidlist.sort()
        if debug:
            print jobidlist
        for jid in jobidlist:
            entry = history.getjob(jid)
            if debug:
                print jid, entry.get('qtime'), starttime
                print entry.get('qtime') >= starttime
            if entry.get('qtime') >= starttime : break # done

            # we are guaranteed that the qtime of all jobs that make
            # it this far are before the first event, so we need to
            # figure if the job is queued or running, nothing else

            job = copy.deepcopy(entry)
            job.set('jobid',jid)
            if job.get('start') and job.get('start') < starttime :
                job.set('state','running')
            else:
                job.set('state','queued')

            self.__jobdict__[jid] = job

## getnextevent shows you what the next event will be.  step actually
## changes the server state to reflect what happened in that event.
## you need the two since you want to calculate ETT for an event
## BEFORE you actually submit the job to the queue!

    def getnextevent(self):                     # return the next event
        return self.__history__.getevent(self.__evindx__ + 1)
    def step(self):                             # step to next event
        self.__evindx__ = self.__evindx__ + 1
        ev = self.__history__.getevent(self.__evindx__)
        self.__evtime__ = ev.time()

        # take care of implications for queue states

        if ev.type() == 'Q' :
            job = self.__history__.getjob(ev.jobid())
            jobcopy = copy.deepcopy(job)
            jobcopy.set('state','queued')
            jobcopy.set('jobid',ev.jobid())
            self.addjob(ev.jobid(),jobcopy)
        elif ev.type() == 'S' :
            job = self.getjob(ev.jobid())
            job.set('state','running')
        elif ev.type() == 'T' :
            job = self.getjob(ev.jobid())
            job.set('state','running')
            job.set('start',ev.time())
        elif ev.type() == 'E' :
            self.deletejob(ev.jobid())
        elif ev.type() == 'D' :         # if it's the last delete evt seen, do it
            jevtl = self.__history__.getjobevts(ev.jobid())
            if jevtl[-1] == ev:
                self.deletejob(ev.jobid())
        return ev

