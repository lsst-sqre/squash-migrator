"""Module to extract squash jobs from the old database and write them to
a local directory.
"""
import json
import logging
import os
import requests

MAX_TIMEOUT = 10 * 60
BASE_TIMEOUT = 15


class Extractor:

    def __init__(self, context=None):
        if not context:
            raise RuntimeError("Extractor must be given execution context")
        self.context = context
        self.directory = os.path.abspath(context.directory)
        self.url = context.from_url
        logger = logging.getLogger(__name__)
        logger.setLevel(context.loglevel)
        self.logger = logger
        self.session = None

    def extract(self, job_numbers=set()):
        """Connect to the squash DB to copy from, and extract all the
        jobs.  Since jobs are immutable, if there is already a file
        representing the job, don't rewrite it.
        """
        jobdir = os.path.join(self.directory, "jobs")
        os.makedirs(jobdir, mode=0o755, exist_ok=True)
        if not self.session:
            self.session = requests.Session()
        if not job_numbers:
            self._bulk_extract()
        else:
            self._individual_extract(job_numbers)

    def _get_job(self, url):
        timeout = BASE_TIMEOUT
        saved_exception = None
        while timeout <= MAX_TIMEOUT:
            try:
                resp = self.session.get(url, timeout=timeout)
                return resp
            except requests.exceptions.ConnectionError as exc:
                self.logger.warning(
                    "Connection error: %s / timeout %d s" % (str(exc), timeout)
                )
                saved_exception = exc
            timeout = timeout * 2
        raise saved_exception

    def _bulk_extract(self):
        nexturl = self.url + "/jobs"
        so_far = 0
        while nexturl:
            url = nexturl
            resp = self._get_job(url)
            nexturl = None
            try:
                j_resp = resp.json()
            except json.decoder.JSONDecodeError as exc:
                self._showerror(resp, exc)
                break
            if "next" in j_resp:
                nexturl = j_resp["next"]
            jobs = j_resp["results"]
            so_far = so_far + len(jobs)
            for job in j_resp["results"]:
                self.write_job(job)
            self.logger.info("%s: %d/%s" % (self.url, so_far, j_resp["count"]))

    def _individual_extract(self, job_numbers):
        lenjob = len(job_numbers)
        so_far = 0
        while job_numbers:
            jobnum = job_numbers.pop()
            fname = self._get_filename_for_jobnum(jobnum)
            if os.path.exists(fname):
                self.logger.info(
                    "File '%s' exists; remove to re-fetch." % fname)
                so_far = so_far + 1
                continue
            url = self.url + "/jobs/" + str(jobnum) + "/"
            try:
                resp = self._get_job(url)
            except requests.exceptions.ConnectionError as exc:
                self.logger.error("Did not fetch '%s': %s" % (url, str(exc)))
                continue
            try:
                j_resp = resp.json()
            except json.decoder.JSONDecodeError as exc:
                self._showerror(resp, exc)
                continue
            try:
                self.write_job(j_resp)
                so_far = so_far + 1
            except KeyError:
                self.logger.error("Job %d malformed: cannot write." % jobnum)
            self.logger.info("%s: %d/%d", url, so_far, lenjob)

    def write_job(self, job):
        """Write JSON for job to file.
        """
        fname = self._get_filename_for_job(job)
        if os.path.exists(fname):
            self.logger.info("File '%s' exists; remove to re-extract." % fname)
            return
        with open(fname, "w") as fp:
            self.logger.debug("Writing job to file '%s'." % fname)
            json.dump(job, fp, indent=4, sort_keys=True)

    def _get_filename_for_job(self, job):
        # I feel like "ID" should be a field, but....
        jobnum = job["links"]["self"].split("/")[-2]
        return self._get_filename_for_jobnum(jobnum)

    def _get_filename_for_jobnum(self, jobnum):
        jobdir = os.path.join(self.directory, "jobs")
        fname = os.path.join(jobdir, "job-%s.json" % jobnum)
        return fname

    def _showerror(self, resp, exc):
        rtext = resp.text
        ltext = len(rtext) - 2000
        # There seems to be a break between 22601 bytes and much bigger.
        if len(rtext) > 30000:
            rtext = (rtext[:1000] +
                     ("[ ...%d characters elided... ]" % ltext) +
                     rtext[-1000:])
        errstr = (("Response from '%s': exception '%s', " +
                   "HTTP status code %r, text '%s'") %
                  (resp.url, str(exc), resp.status_code, rtext))
        self.logger.error(errstr)
