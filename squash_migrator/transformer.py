"""Class to transform old representation into new one."""
import glob
import json
import logging
import os
import requests


class Transformer:

    def __init__(self, context=None):
        if not context:
            raise RuntimeError("Transformer must be given execution context")
        self.context = context
        self.directory = os.path.abspath(context.directory)
        self.input_dir = os.path.join(self.directory, "jobs")
        self.output_dir = os.path.join(self.directory, "transformed")
        logger = logging.getLogger(__name__)
        logger.setLevel(context.loglevel)
        self.logger = logger
        self.session = None
        self.metric_map = {}

    def transform(self, jobs=set()):
        """Transform old-style representations into new ones.
        """
        inputfiles = []
        if not jobs:
            inputfiles = self._scan_inputdir()
        else:
            for jobnum in jobs:
                inputfiles.append(os.path.join(
                    self.input_dir, "job-%s.json" % jobnum))
        if not inputfiles:
            self.logger.error("No input files found in %s to transform" %
                              self.input_dir)
            return
        os.makedirs(self.output_dir, mode=0o755, exist_ok=True)
        self._make_metric_map()
        numfiles = len(inputfiles)
        so_far = 0
        for inp_file in inputfiles:
            job = None
            with open(inp_file, "r") as f:
                job = json.load(f)
            self.logger.debug("Loaded '%s'" % inp_file)
            transformed_job = self.transform_job(job)
            self.write_transformed_job(transformed_job)
            so_far = so_far + 1
            self.logger.info("%s: %d/%d" % (inp_file, so_far, numfiles))

    def _make_metric_map(self):
        nexturl = self.context.from_url + "/metrics"
        if not self.session:
            self.session = requests.Session()
        m_map = {}
        so_far = 0
        while nexturl:
            url = nexturl
            resp = self.session.get(url)
            nexturl = None
            try:
                j_resp = resp.json()
            except json.decoder.JSONDecodeError as exc:
                self._showerror(resp, exc)
                break
            if "next" in j_resp:
                nexturl = j_resp["next"]
            count = j_resp["count"]
            for result in j_resp["results"]:
                metric = result["metric"]
                unit = result["unit"]
                m_map[metric] = unit
                so_far = so_far + 1
                self.logger.debug("Metric unit map: %d/%d" % (so_far, count))
        self.logger.info("Created metric unit map.")
        self.metric_map = m_map

    def transform_job(self, job):
        """Does the heavy lifting to turn an old-style job into a new one.
        """
        tjob = {"measurements": [],
                "meta": {},
                "blobs": []
                }
        tm = tjob["measurements"]
        jm = job["measurements"]
        for meas in jm:
            nm = {}
            nm["identifier"] = None
            nm["value"] = meas["value"]
            nm["metric"] = "validate_drp." + meas["metric"]
            nm["unit"] = self.metric_map.get(meas["metric"])
            nm["blob_refs"] = self._get_blob_refs(job)
            jm.append(nm)
        tjob["blobs"] = job["blobs"]
        # Inject blobs parameters here when Angelo figures it out.
        tjob["meta"]["env"] = {}
        te = tjob["meta"]["env"]
        for fld in ["ci_id", "ci_name", "ci_dataset", "ci_label",
                    "date", "ci_url", "status"]:
            self.logger.debug("Copying field '%s'" % fld)
            te[fld] = job.get(fld)
        te["env_name"] = "jenkins"
        package_obj = self._transform_packages(job)
        tjob["meta"]["packages"] = package_obj
        return tjob

    def _transform_packages(self, job):
        retval = {}
        for pck in job["packages"]:
            nnm = pck["name"]
            self.logger.debug("Adding package '%s'" % nnm)
            retval[nnm] = pck
        return retval

    def _get_blob_refs(self, job):
        blobs = None
        try:
            blobs = job["measurements"]["metadata"]["blobs"]
        except (KeyError, TypeError):
            return None
        bref = []
        for k, v in blobs:
            self.logger.debug("Adding values for blob '%s'" % k)
            bref.append(v)
        return bref

    def write_transformed_job(self, job):
        fname = self._get_filename_for_job(job)
        if os.path.exists(fname):
            self.logger.info(
                "File '%s' exists; remove to re-transform." % fname)
            return
        with open(fname, "w") as f:
            self.logger.debug("Writing job to file '%s'." % fname)
            json.dump(job, f, sort_keys=True, indent=4)

    def _get_filename_for_job(self, job):
        # I feel like "ID" should be a field, but....
        jobnum = job["links"]["self"].split("/")[-2]
        return self._get_filename_for_jobnum(jobnum)

    def _get_filename_for_jobnum(self, jobnum):
        fname = os.path.join(self.output_dir, "job-%s.json" % jobnum)
        return fname

    def _scan_inputdir(self):
        fileglobstr = os.path.join(self.input_dir, "job-*.json")
        return glob.glob(fileglobstr)
