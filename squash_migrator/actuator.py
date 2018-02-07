"""Base class for the Extractor, Transformer, and Loader classes.
   Contains utility methods common to those classes.
"""
import glob
import json
import os
import requests


class Actuator(object):
    """Base class for SQuaSH migration components.
    """

    def __init__(self, context=None):
        if not context:
            raise RuntimeError("Transformer must be given execution context")
        self.context = context
        self.directory = os.path.abspath(context.directory)
        self.job_numbers = self.context.job_numbers
        self.session = requests.Session()

    def scan_for_jobs(self, directory):
        """Find jobs matching job file name format in a given directory.
        """
        fileglobstr = os.path.join(directory, "job-*.json")
        return glob.glob(fileglobstr)

    def get_jobnum_for_job(self, job):
        """Given a job, get the job number for it.  If new-style, it needs
        to have been given the (non-persisted) "_job_number" field.
        """
        if "_job_number" in job:
            return job["_job_number"]
        # I feel like "ID" should be a field, but....
        jobnumstr = job["links"]["self"].split("/")[-2]
        jobnum = int(jobnumstr)
        return jobnum

    def get_filename_for_jobnum(self, directory, jobnum):
        """Given a job number and directory, produce the job file name.
        """
        fname = os.path.join(directory, "job-%s.json" % str(jobnum))
        return fname

    def show_response_error(self, resp, exc):
        """Show an HTTP response error, eliding text if necessary.
        """
        rtext = resp.text
        # There seems to be a break between 22601 bytes and much bigger.
        etext = self.maybe_elide_long_string(rtext)
        errstr = (("Response from '%s': exception '%s', " +
                   "HTTP status code %r, text '%s'") %
                  (resp.url, str(exc), resp.status_code, etext))
        self.logger.error(errstr)

    def maybe_elide_long_string(self, input):
        """Show just beginning and end of string larger than 30000
        characters.
        """
        ll = len(input) - 2000
        instr = input
        # There seems to be a break between 22601 bytes and much bigger in
        #  job sizes
        if ll > 30000:
            instr = (input[:1000] +
                     "...[ %d characters elided ]..." % ll +
                     input[-1000:])
        return instr

    def write_job(self, job, directory):
        """Write JSON for job to file in specified directory.
        """
        jobnum = self.get_jobnum_for_job(job)
        job["_job_number"] = jobnum
        fname = self.get_filename_for_jobnum(directory, jobnum)
        if os.path.exists(fname):
            self.logger.info(
                "File '%s' exists; remove to allow it rewriting." % fname)
            return
        with open(fname, "w") as fp:
            self.logger.debug("Writing job to file '%s'." % fname)
            json.dump(job, fp, indent=4, sort_keys=True)
