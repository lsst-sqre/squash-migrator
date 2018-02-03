"""Class to load new representation into database."""
import json
import logging
import os
from .actuator import Actuator


class Loader(Actuator):
    """Push transformed jobs into the new database.
    """

    def __init__(self, context=None):
        super().__init__(context=context)
        self.input_dir = os.path.join(self.directory, "transformed")
        self.to_url = self.context.to_url
        logger = logging.getLogger(__name__)
        logger.setLevel(context.loglevel)
        self.logger = logger

    def load(self):
        """Push transformed jobs into target database"""
        headers = self.context.headers
        if not headers:
            self.logger.warning("No authentication to load jobs.")
            return
        inputfiles = []
        job_numbers = self.job_numbers
        if not job_numbers:
            inputfiles = self.scan_for_jobs(self.input_dir)
        else:
            for jobnum in job_numbers:
                inputfiles.append(self.get_filename_for_jobnum(
                    self.input_dir, jobnum))
        if not inputfiles:
            self.logger.error("No input files found in %s to transform" %
                              self.input_dir)
            return
        session = self.session
        session.headers.update(self.context.headers)
        so_far = 0
        numfiles = len(inputfiles)
        jobmap = {}
        for fname in inputfiles:
            with open(fname, "r") as f:
                job = json.load(f)
                # _job_number is used in new jobs for correlation but should
                #   not be sent
                jobnumstr = os.path.basename(fname)[4:-5]
                jobnum = int(jobnumstr)
                save_num = None
                if "_job_number" in job:
                    save_num = job["_job_number"]
                    del job["_job_number"]
                if save_num is not None and save_num != jobnum:
                    errstr = ("_job_number %d did not match filename %d" %
                              (save_num, jobnum))
                    self.logger.error(errstr)
                url = self.to_url + "/job"
                self.logger.info(
                    "Sending transformed job %d to %s" % (jobnum, url))
                resp = session.post(url, json=job)
                if (resp.status_code < 200 or
                        resp.status_code > 299):
                    self.logger.error("Error posting '%s': HTTP %d / '%s'" %
                                      (fname, resp.status_code, resp.text))
                    continue
                r_json = resp.json()
                message = r_json["message"]
                # This is cheesy, but we rely on the filename format
                #  and the message format to just extract the old and new
                #  job IDs
                # To wit, 'job-XYZ.json' and
                #  'Job `XYZ` successfully created' or similar, where XYZ
                #  is within backticks.
                try:
                    new_jobnum = int(message.split('`')[1])
                    jobmap[jobnum] = new_jobnum
                except ValueError:
                    errstr = "Could not determine job numbers for '%s' % fname"
                    self.logger.error(errstr)
                so_far = so_far + 1
                self.logger.info("%s: %d/%d" % (fname, so_far, numfiles))
        if jobmap:
            with open(os.path.join(self.directory, "jobmap.json"), "w") as f:
                json.dump(jobmap, f)  # This makes the object key a string
                # because JSON
