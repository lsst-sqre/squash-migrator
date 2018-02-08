import json
import logging
import os
import time
from .actuator import Actuator


class Loader(Actuator):
    """Class to load new SQuaSH representation into database."""

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
            self.logger.error("No input files found in %s to load" %
                              self.input_dir)
            return
        session = self.session
        session.headers.update(self.context.headers)
        so_far = 0
        numfiles = len(inputfiles)
        jobmap = {}
        for fname in inputfiles:
            try:
                with open(fname, "r") as f:
                    self.logger.debug("Loading '%s' for transmission" % fname)
                    job = json.load(f)
                    # _job_number is used in new jobs for correlation but
                    #   should not be sent
                    jobnumstr = os.path.basename(fname)[4:-5]
                    jobnum = int(jobnumstr)
                    save_num = None
                    if "_job_number" in job:
                        save_num = job["_job_number"]
                        del job["_job_number"]
                        if save_num is not None and save_num != jobnum:
                            errstr = ("_job_number %d != filename %d" %
                                      (save_num, jobnum))
                            self.logger.error(errstr)
                    url = self.to_url + "/job"
                    self.logger.info(
                        "Sending transformed job %d to %s" % (jobnum, url))
                    resp = session.post(url, json=job)
                    if (resp.status_code < 200 or
                            resp.status_code > 299):
                        # Should always be 202 if it worked.
                        self.logger.error("POST error '%s': HTTP %d / '%s'" %
                                          (fname, resp.status_code, resp.text))
                    continue
                    try:
                        r_json = resp.json()
                        message = r_json["message"]
                        statuslink = r_json["status"]
                    except (json.decoder.JSONDecodeError, KeyError) as exc:
                        self.logger.error("Malformed response from " +
                                          "%s (%s): %s" % (url, str(exc),
                                                           resp.text))
                        continue
                    # This is cheesy, but we rely on the message format
                    #  to extract the new job ID
                    # To wit, 'Job `XYZ` accepted' or similar, where
                    #  XYZ is within backticks.
                    try:
                        new_jobnum = int(message.split('`')[1])
                        jobmap[jobnum] = new_jobnum
                    except ValueError:
                        errstr = "Could not get job numbers for '%s' % fname"
                        self.logger.error(errstr)
                    try:
                        self._check_status(statuslink)
                    except (RuntimeError, KeyError,
                            json.decoder.JSONDecodeError) as exc:
                        self.logger.error("Data load failed: %s" % str(exc))
                        continue
                    so_far = so_far + 1
                    self.logger.info("%s: %d/%d" % (fname, so_far, numfiles))
                if jobmap:
                    with open(os.path.join(self.directory, "jobmap.json"),
                              "w") as f:
                        # This makes the object key a string
                        json.dump(jobmap, f)
                    # because JSON
            except OSError:
                self.logger.error("Could not read file '%s'" % fname)

    def _check_status(self, statuslink):
        self.logger.info("Checking S3 upload status.")
        maxtries = 60
        delay = 5
        attempt = 0
        while attempt < maxtries:
            attempt = attempt + 1
            resp = self.session.get(statuslink)
            r_json = resp.json()
            status = r_json["status"]
            if status == "SUCCESS":
                return
            elif status == "FAILURE":
                break
            elif status == "PENDING" or status == "STARTED":
                self.logger.debug("Upload %s [%d/%d]; waiting %ds" % (status,
                                                                      attempt,
                                                                      maxtries,
                                                                      delay))
            else:
                self.logger.error("Unknown status %s" % status)
                break
            time.sleep(delay)
        raise RuntimeError("Upload to s3 failed")
