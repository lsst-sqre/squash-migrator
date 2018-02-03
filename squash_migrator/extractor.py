"""Module to extract squash jobs from the old database and write them to
a local directory.
"""
import json
import logging
import os
import requests
from .actuator import Actuator

MAX_TIMEOUT = 10 * 60
BASE_TIMEOUT = 15


class Extractor(Actuator):

    def __init__(self, context=None):
        super().__init__(context=context)
        self.url = context.from_url
        self.output_directory = os.path.join(self.directory, "jobs")
        logger = logging.getLogger(__name__)
        logger.setLevel(context.loglevel)
        self.logger = logger

    def extract(self):
        """Connect to the squash DB to copy from, and extract some or all
        jobs.  Since jobs are immutable, if there is already a file
        representing the job, don't rewrite it.
        """
        job_numbers = self.context.job_numbers
        os.makedirs(self.output_directory, mode=0o755, exist_ok=True)
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
                self.write_job(job, self.output_directory)
            self.logger.info("%s: %d/%s" % (self.url, so_far, j_resp["count"]))

    def _individual_extract(self, job_numbers):
        lenjob = len(job_numbers)
        so_far = 0
        for jobnum in job_numbers:
            fname = self.get_filename_for_jobnum(self.output_directory, jobnum)
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
                self.write_job(j_resp, self.output_directory)
                so_far = so_far + 1
            except KeyError:
                self.logger.error("Job %d malformed: cannot write." % jobnum)
            self.logger.info("%s: %d/%d", url, so_far, lenjob)
