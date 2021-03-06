import json
import logging
import os
import uuid
import requests
from .actuator import Actuator

# Metric map generated by Simon Krughoff
METRICS = {(u'AD2', u'design', u'HSC-I'): 'validate_drp.AD2_design',
           (u'AF1', u'design', u'r'): 'validate_drp.AF1_design',
           (u'PA2', u'minimum', u'r'): 'validate_drp.PA2_minimum_gri',
           (u'AF2', u'stretch', u'r'): 'validate_drp.AF2_stretch',
           (u'AF1', u'minimum', u'r'): 'validate_drp.AF1_minimum',
           (u'AD2', u'minimum', u'z'): 'validate_drp.AD2_minimum',
           (u'AD1', u'design', u'r'): 'validate_drp.AD1_design',
           (u'AD2', u'minimum', u'HSC-I'): 'validate_drp.AD2_minimum',
           (u'AD1', u'stretch', u'HSC-I'): 'validate_drp.AD1_stretch',
           (u'AD1', u'stretch', u'r'): 'validate_drp.AD1_stretch',
           (u'PA2', u'stretch', u'z'): 'validate_drp.PA2_stretch_uzy',
           (u'PA2', u'design', u'HSC-I'): 'validate_drp.PA2_design_gri',
           (u'AM1', None, u'r'): 'validate_drp.AM1',
           (u'AF2', u'minimum', u'HSC-I'): 'validate_drp.AF2_minimum',
           (u'AD2', u'design', u'z'): 'validate_drp.AD2_design',
           (u'PA1', None, u'z'): 'validate_drp.PA1',
           (u'PA2', u'design', u'z'): 'validate_drp.PA2_design_uzy',
           (u'AF1', u'minimum', u'z'): 'validate_drp.AF1_minimum',
           (u'PA1', None, None): 'validate_drp.PA1',
           (u'PA2', u'stretch', u'r'): 'validate_drp.PA2_stretch_gri',
           (u'AF2', u'minimum', u'r'): 'validate_drp.AF2_minimum',
           (u'AF1', u'stretch', u'z'): 'validate_drp.AF1_stretch',
           (u'PF1', u'minimum', u'r'): 'validate_drp.PF1_minimum_gri',
           (u'AF1', u'stretch', u'HSC-I'): 'validate_drp.AF1_stretch',
           (u'PF1', u'stretch', u'HSC-I'): 'validate_drp.PF1_stretch_gri',
           (u'AM2', None, u'z'): 'validate_drp.AM2',
           (u'PF1', u'design', u'z'): 'validate_drp.PF1_design_uzy',
           (u'PA2', u'minimum', u'z'): 'validate_drp.PA2_minimum_uzy',
           (u'AF2', u'design', u'z'): 'validate_drp.AF2_design',
           (u'AM2', None, None): 'validate_drp.AM2',
           (u'AF2', u'design', u'r'): 'validate_drp.AF2_design',
           (u'PA1', None, u'HSC-I'): 'validate_drp.PA1',
           (u'AD2', u'stretch', u'z'): 'validate_drp.AD2_stretch',
           (u'AD1', u'design', u'z'): 'validate_drp.AD1_design',
           (u'AF1', u'design', u'z'): 'validate_drp.AF1_design',
           (u'TE2', None, u'r'): 'validate_drp.TE2',
           (u'AD1', u'stretch', u'z'): 'validate_drp.AD1_stretch',
           (u'TE2', None, u'HSC-I'): 'validate_drp.TE2',
           (u'AD1', u'minimum', u'HSC-I'): 'validate_drp.AD1_minimum',
           (u'AF2', u'stretch', u'HSC-I'): 'validate_drp.AF2_stretch',
           (u'AF2', u'stretch', u'z'): 'validate_drp.AF2_stretch',
           (u'AD2', u'minimum', u'r'): 'validate_drp.AD2_minimum',
           (u'AD2', u'design', u'r'): 'validate_drp.AD2_design',
           (u'PA2', u'minimum', u'HSC-I'): 'validate_drp.PA2_minimum_gri',
           (u'PF1', u'minimum', u'z'): 'validate_drp.PF1_minimum_uzy',
           (u'AF2', u'design', u'HSC-I'): 'validate_drp.AF2_design',
           (u'AD2', u'stretch', u'HSC-I'): 'validate_drp.AD2_stretch',
           (u'PA2', u'stretch', u'HSC-I'): 'validate_drp.PA2_stretch_gri',
           (u'TE1', None, u'HSC-I'): 'validate_drp.TE1',
           (u'AM1', None, u'z'): 'validate_drp.AM1',
           (u'AF1', u'design', u'HSC-I'): 'validate_drp.AF1_design',
           (u'AD1', u'design', u'HSC-I'): 'validate_drp.AD1_design',
           (u'PF1', u'stretch', u'r'): 'validate_drp.PF1_stretch_gri',
           (u'PA1', None, u'r'): 'validate_drp.PA1',
           (u'PA2', u'design', u'r'): 'validate_drp.PA2_design_gri',
           (u'AF1', u'minimum', u'HSC-I'): 'validate_drp.AF1_minimum',
           (u'PF1', u'design', u'r'): 'validate_drp.PF1_design_gri',
           (u'AM2', None, u'HSC-I'): 'validate_drp.AM2',
           (u'PF1', u'design', u'HSC-I'): 'validate_drp.PF1_design_gri',
           (u'PF1', u'stretch', u'z'): 'validate_drp.PF1_stretch_uzy',
           (u'AD1', u'minimum', u'z'): 'validate_drp.AD1_minimum',
           (u'PF1', u'minimum', u'HSC-I'): 'validate_drp.PF1_minimum_gri',
           (u'AD1', u'minimum', u'r'): 'validate_drp.AD1_minimum',
           (u'AF1', u'stretch', u'r'): 'validate_drp.AF1_stretch',
           (u'AM1', None, None): 'validate_drp.AM1',
           (u'AM2', None, u'r'): 'validate_drp.AM2',
           (u'AD2', u'stretch', u'r'): 'validate_drp.AD2_stretch',
           (u'AF2', u'minimum', u'z'): 'validate_drp.AF2_minimum',
           (u'AM1', None, u'HSC-I'): 'validate_drp.AM1',
           (u'TE1', None, u'r'): 'validate_drp.TE1'}


class Transformer(Actuator):
    """Class to transform old SQuaSH representation into new one.
    """

    def __init__(self, context=None):
        super().__init__(context=context)
        self.input_dir = os.path.join(self.directory, "jobs")
        self.output_dir = os.path.join(self.directory, "transformed")
        logger = logging.getLogger(__name__)
        logger.setLevel(context.loglevel)
        self.logger = logger
        self.metric_map = {}

    def transform(self):
        """Transform old-style representations into new ones.
        """
        inputfiles = []
        job_numbers = self.job_numbers
        if not job_numbers:
            inputfiles = self.scan_for_jobs(self.input_dir)
        else:
            for jobnum in job_numbers:
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
            basefile = os.path.basename(inp_file)
            out_file = os.path.join(self.output_dir, basefile)
            if os.path.exists(out_file):
                self.logger.info(
                    "File '%s' exists; remove to re-transform." % out_file)
                continue
            job = None
            try:
                with open(inp_file, "r") as f:
                    job = json.load(f)
            except OSError as exc:
                self.logger.error(
                    "Could not load '%s': %s" % (inp_file, str(exc)))
                continue
            self.logger.debug("Loaded '%s'" % inp_file)
            transformed_job = self.transform_job(job)
            self.write_job(transformed_job, self.output_dir)
            so_far = so_far + 1
            self.logger.info("%s: %d/%d" % (inp_file, so_far, numfiles))

    def _make_metric_map(self):
        nexturl = self.context.from_url + "/metrics/"
        if not self.session:
            self.session = requests.Session()
        m_map = {}
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
            for result in j_resp["results"]:
                metric = result["metric"]
                unit = result["unit"]
                m_map[metric] = unit
        self.logger.debug("Created metric unit map.")
        self.metric_map = m_map

    def transform_job(self, job):
        """Does the heavy lifting to turn an old-style SQuaSH job into a
        new one.
        """
        tjob = {"measurements": [],
                "meta": {},
                "blobs": []
                }
        tm = tjob["measurements"]
        jm = job["measurements"]
        newblobs = []
        # Iterate over measurements
        for meas in jm:
            nm = {}
            nm["identifier"] = None
            nm["value"] = meas["value"]
            metadata = meas.get("metadata")
            if type(metadata) is str:
                metadata = self._fix_input_string(metadata)
            # Map old metric/spec/filter to new metric
            new_metric = self._get_metric_name(meas["metric"], metadata)
            nm["metric"] = new_metric
            nm["unit"] = self.metric_map.get(meas["metric"])
            nm["blob_refs"] = self._get_blob_refs(metadata)
            if metadata:
                # Glue extras/parameters into blobs/bob_refs.
                newblobdata = {}
                if "extras" in metadata:
                    newblobdata.update(metadata["extras"])
                if "parameters" in metadata:
                    newblobdata.update(metadata["parameters"])
                if newblobdata:
                    blobid = uuid.uuid4().hex
                    newblob = {"name": new_metric,
                               "data": newblobdata,
                               "identifier": blobid}
                    newblobs.append(newblob)
                    if nm["blob_refs"] is None:
                        nm["blob_refs"] = []
                    nm["blob_refs"].append(blobid)
            tm.append(nm)
        blobs = job["blobs"]
        if type(blobs) is str:
            blobs = self._fix_input_string(blobs)
        if newblobs:
            blobs.extend(newblobs)
        tjob["blobs"] = blobs
        # Move old top-level fields into .meta.env
        tjob["meta"]["env"] = {}
        te = tjob["meta"]["env"]
        for fld in ["ci_id", "ci_name", "ci_dataset", "ci_label",
                    "date", "ci_url", "status"]:
            te[fld] = job.get(fld)
        te["env_name"] = "jenkins"
        package_obj = self._transform_packages(job)
        tjob["meta"]["packages"] = package_obj
        tjob["_job_number"] = self.get_jobnum_for_job(job)
        return tjob

    def _get_metric_name(self, metric, metadata):
        newname = metric
        spec_name = None
        flt = None
        if metadata:
            spec_name = metadata.get("spec_name")
            if spec_name:
                newname += "_" + spec_name
            flt = metadata.get("filter_name")
            if flt:
                newname += "_" + flt
        metric_tuple = (metric, spec_name, flt)
        retval = METRICS[metric_tuple] or metric
        return retval

    def _transform_packages(self, job):
        retval = {}
        for pck in job["packages"]:
            pck["eups_version"] = pck["build_version"]
            del pck["build_version"]
            pck["git_sha"] = pck["git_commit"]
            del pck["git_commit"]
            nnm = pck["name"]
            retval[nnm] = pck
        return retval

    def _get_blob_refs(self, metadata):
        if not metadata:
            return None
        try:
            blobs = metadata["blobs"]
        except (KeyError, TypeError) as exc:
            self.logger.debug("Could not extract metadata blobs: %s" %
                              str(exc))
            return None
        blobrefs = list(blobs.values())
        return blobrefs

    def _fix_input_string(self, input):
        # Effectively, some of these were serialized with __repr__ rather
        #  than via the json module.
        obj = None
        try:
            obj = json.loads(input)
        except json.decoder.JSONDecodeError as exc:
            sexc = str(exc)
            if sexc.find("property name enclosed in double quotes") != -1:
                # This seems like a terrible idea.
                obj = eval(input)
            else:
                einput = self.maybe_elide_long_string(input)
                self.logger.warning("Input string transformation failed: %s" %
                                    einput)
        return obj
