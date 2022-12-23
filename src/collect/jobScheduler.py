"""
This is a draft of an alternate factoring of scheduling polling jobs that allows
multiple concurrent instances of the collect.py application.
JobScheduler instances coordinate through a shared schedule file.

Each application submits the tables to be polled to its JobScheduler instance.
If a submitted table doesn't  exist in the schedule file, it is added to the file.
If a submitted table exists in the schedule file, the submission updates schedule
goals, but doesn't update scheduling history -- e.g. "cursors" indicating current
status of the downloading.

Implementation will require testing and verification of the JobScheduler, then
replacing collect.py's job scheduling with JobScheduler.

"""
import io
import json
import time
from typing import Dict, Union
from io import TextIOWrapper


class JobScheduler:
    """Maintain a Dictionary of jobs in the shared file, or isolated dict

    """
    # The maintained dynamic attributes
    fields = ('lastId', 'minSec', 'lastSec', 'maxTime', 'nextPoll', 'recordsPerHour', 'startPoll')
    jobDefault = dict.fromkeys(fields, 0.0)
    goodEnough: float = 3.0     # start job iff scheduled time is < goodEnough seconds in the future
    maxTime = time.time() + 10*365*24*60*60*1000  # 10 years from now
    sleepMax: float = 60.0      # maximum time to sleep before checking again
    timeOut: float = 3600 + goodEnough  # reschedule if not yet completed
    retrySeconds: float = 2.0   # seconds to sleep when job file is locked

    def __init__(self, jobsDict: Union[str, Dict[str, float]]):
        """Maintain a Dictionary of jobs in the shared file, or isolated dict

        :param jobsDict:    filename of the jobs dict, or a jobs dict to use instead
        """
        self.jobsDict = jobsDict  # filename of the jobs dict, or a jobs dict to use instead
        self.jsonFile: Union[TextIOWrapper, None] = None  # opened File, or None iff file is not opened

    def post_get(self, completed: Union[Dict[str, Dict[str, float]], None]) -> Dict[str, Dict[str, float]]:
        """Post completion of the completed job. Sleep until returning the next job.
        :param completed:   The completed job, or None.
        :return:
        """
        if isinstance(completed, dict):
            # verify completed job has expected structure
            tableName, attributes = completed.popitem()
            if set(attributes.keys()) != set(self.fields):
                raise ValueError("missing/extra keys in {tablename} attributes: {tableValues}")
        elif completed is None:
            tableName = None
            attributes = None           # make IDE happy
        else:
            raise TypeError(f"completed type={type(completed)}: {completed}")
        while True:
            # acquire the [possibly shared] job dictionary
            jd = self._getJobsDict()
            try:                        # ensure that jobs file is unlocked, despite exception
                if tableName:           # update provided?
                    jd[tableName] = attributes.copy()  # Yes. post it
                # find the next job to process -- job with minimum nextPoll
                key = jd.keys().__iter__().__next__()  # initial guess
                for k, v in jd.items():
                    if v['nextPoll'] < jd[key]['nextPoll']:
                        key = k
                seconds = jd[key]['nextPoll'] - time.time()  # seconds to scheduled time
                if seconds < self.goodEnough:  # very soon?
                    timeout = jd.copy()  # Yes.
                    timeout[key]['nextPoll'] += self.timeOut  # when eligible again if not completed
                    self._putJobsDict(jd)  # Write_back and release jobs dict
                    return {key: jd[key].copy()}    # return a job to do
                self._putJobsDict(jd if tableName else None)  # update & release
                tableName = None        # posting completed and no longer necessary
                time.sleep(min(seconds, self.sleepMax))  # wait a while before ...
                continue                # ... checking again
            except Exception as exc:
                self._putJobsDict(jd if tableName else None)  # update & release
                raise Exception(exc)

    def _getJobsDict(self) -> Dict[str, Dict[str, float]]:
        """Obtain exclusive access to job dictionary and return it

        :return:    the job dictionary
        """
        if isinstance(self.jobsDict, dict):  # local isolated job dictionary?
            return self.jobsDict        # Yes. Simply return it
        elif self.jsonFile:             # jsonFile is already open?
            raise AssertionError("_getJobsDict called when jsonfile wss already open")
        elif isinstance(self.jobsDict, str):  # No. Filename of shared file?
            while True:                 # Yes. Obtain exclusive access
                try:
                    self.jsonFile = open(self.jobsDict, mode='r+')
                except FileNotFoundError as fnf:
                    print("ERROR no jobDicts file", fnf)
                    raise FileNotFoundError(fnf)
                except OSError:         # Hopefully this is the correct Exception
                    print("jobsDict file locked. Sleeping")
                    time.sleep(self.retrySeconds)
                    continue            # keep trying
                # successfully opened the file
                states = json.load(self.openfile)
                return states
        else:                           # internal error
            raise TypeError(f"self.jobDict type is {type(self.jobsDict)}: {self.jobsDict}")

    def _putJobsDict(self, jobDict: Union[Dict[str, Dict[str, float]], None]):
        """Put completed job [if present] back into the schedule

        :param jobDict:     the job, or None if no job
        :return:
        """
        if isinstance(jobDict, dict):   # local isolated job dictionary?
            self.jobsDict = jobDict     # Yes. replace with updated copy
            return
        elif isinstance(jobDict, str):  # No. Filename of shared file?:
            self.jsonFile.seek(0, __whence=io.SEEK_SET)  # Yes.
            self.jsonFile.truncate(0)   # Truncate to zero size
            json.dump(jobDict, self.jsonFile)  # and write back to shared file
            self.jsonFile.close()       # release the file for other to use
            self.jsonFile = None        # Cause exception if called out of order
            return
        else:                           # internal error
            raise TypeError(f"jobDict type is {type(jobDict)} {jobDict}")
