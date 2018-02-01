"""Module to extract squash jobs from the old database and write them to
a local directory.
"""
import json
import logging
import os
import requests


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

    def extract(self, job_numbers=set()):
        """Connect to the squash DB to copy from, and extract all the
        jobs.  Since jobs are immutable, if there is already a file
        representing the job, don't rewrite it.
        """
        logger = self.logger
        jobdir = os.path.join(self.directory, "jobs")
        os.makedirs(jobdir, mode=0o755, exist_ok=True)
        jobs = []
        if not job_numbers:
            nexturl = self.url + "/jobs"
            while nexturl:
                url = nexturl
                resp = requests.get(url)
                nexturl = None
                try:
                    j_resp = resp.json()
                except json.decoder.JSONDecodeError:
                    errstr = ("Response from '%s': status code %d, text '%s'" %
                              (nexturl, resp.status_code, resp.text))
                    logger.error(errstr)
                    break
                if "next" in j_resp:
                    nexturl = j_resp["next"]
                jobs.extend(j_resp["results"])
                logger.info("%s: %d/%s" %
                            (url, len(jobs), j_resp["count"]))

        else:
            lenjob = len(job_numbers)
            while job_numbers:
                jobnum = job_numbers.pop()
                url = self.url + "/jobs/" + str(jobnum) + "/"
                resp = requests.get(url)
                try:
                    j_resp = resp.json()
                except json.decoder.JSONDecodeError:
                    errstr = ("Response from '%s': status code %d, text '%s'" %
                              (url, resp.status_code, resp.text))
                    logger.error(errstr)
                    continue
                jobs.append(j_resp)
                logger.info("%s: %d/%d", url, len(jobs), lenjob)
        for job in jobs:
            jobnum = job["links"]["self"].split("/")[-2]
            fname = os.path.join(jobdir, "job-%s.json" % jobnum)
            if os.path.exists(fname):
                logger.debug("File '%s' already exists; remove to re-extract.")
                continue
            with open(fname, "w") as fp:
                logger.debug("Writing job to file '%s'." % fname)
                json.dump(job, fp, indent=4, sort_keys=True)