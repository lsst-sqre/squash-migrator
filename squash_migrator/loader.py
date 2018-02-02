"""Class to load new representation into database."""
import json
import logging
import os
import requests


class Loader:

    def __init__(self, context=None):
        if not context:
            raise RuntimeError("Transformer must be given execution context")
        self.context = context
        self.directory = os.path.abspath(context.directory)
        self.input_dir = os.path.join(self.directory, "transformed")
        self.job_numbers = self.context.job_numbers
        logger = logging.getLogger(__name__)
        logger.setLevel(context.loglevel)
        self.logger = logger
        self.session = None

    def load(self):
        """Push transformed jobs into target database"""
        headers = self.context.headers
        if not headers:
            self.logger.warning("No authentication to load jobs.")
            return
        inputfiles = []
        job_numbers = self.job_numbers
        if not job_numbers:
            inputfiles = self._scan_inputdir()
        else:
            for jobnum in job_numbers:
                inputfiles.append(os.path.join(
                    self.input_dir, "job-%s.json" % jobnum))
        if not inputfiles:
            self.logger.error("No input files found in %s to transform" %
                              self.input_dir)
            return
        if not self.session:
            session = requests.Session()
            session.headers.update(self.context.headers)
        self.session = session
        so_far = 0
        numfiles = len(inputfiles)
        jobmap = {}
        for fname in inputfiles:
            with open(fname, "r") as f:
                job = json.load(f)
                self.logger.debug("Sending '%s'" % fname)
                url = self.context.to_url + "/job"
                resp = session.post(url, json=job)
                if resp.status_code != requests.codes.ok:
                    self.logger.error("Error posting '%s': HTTP %d / '%s'" %
                                      (fname, resp.status_code, resp.text))
                    continue
                r_json = resp.json()
                message = r_json["message"]
                # This is cheesy, but we rely on the filename format
                #  and the message format to just extract the old and new
                #  job IDs
                # To wit, 'job-XYZ.json' and
                #  'Job `XYZ` successfully created'
                try:
                    old_jobnum = int(os.path.basename(fname[4:-5]))
                    new_jobnum = int(message[5:-22])
                    jobmap[old_jobnum] = new_jobnum
                except ValueError:
                    errstr = "Could not determine job numbers for '%s' % fname"
                    self.logger.error(errstr)
                so_far = so_far + 1
                self.logger.info("%s: %d/%d" % (fname, so_far, numfiles))
        if jobmap:
            with open(os.path.join(self.directory, "jobmap.json"), "w") as f:
                json.dump(jobmap, f, indent=4, sort_keys=True)

    def _scan_inputdir(self):
        fileglobstr = os.path.join(self.input_dir, "job-*.json")
        return glob.glob(fileglobstr)
