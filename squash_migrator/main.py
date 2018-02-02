#!/usr/bin/env python3

import logging
import os
from optparse import OptionParser
from .context import Context
from .defaults import SQUASH_MIGRATOR_NAMESPACE, SQUASH_API_URL,\
    SQUASH_RESTFUL_API_URL
from .extractor import Extractor
from .transformer import Transformer
from .loader import Loader


class Migrator:

    def __init__(self, context=None, extractor=None, transformer=None,
                 loader=None):
        self.context = context
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader
        self.logger = logging.getLogger(__name__)
        self.loglevel = context.loglevel
        self.logger.setLevel(self.loglevel)

    def etl(self, jobs=None):
        """Perform the extract/transform/load operation."""
        self.extractor.extract(job_numbers=jobs)


def _empty(obj, param):
    if not param or param not in obj or not obj.get(param):
        return True
    return False


def get_options():
    params = {}
    parser = OptionParser()
    parser.add_option("-d", "--directory", help="directory for squash data",
                      default=os.environ.get(SQUASH_MIGRATOR_NAMESPACE +
                                             "DIRECTORY") or
                      os.path.join(os.getcwd(), "squash_data"))
    parser.add_option("-u", "--user", "--username",
                      help="username for API communication")
    parser.add_option("-p", "--password", "--pass", "--pw",
                      help="username for API communication")
    parser.add_option("-k", "--token",
                      help="token for API communication")
    parser.add_option("-l", "--loglevel",
                      help="loglevel to use",
                      default="info")
    parser.add_option("-f", "--from-url", "--from",
                      help="URL of old service",
                      default=os.environ.get(SQUASH_MIGRATOR_NAMESPACE +
                                             "API_URL") or SQUASH_API_URL)
    parser.add_option("-t", "--to-url", "--to",
                      help="URL of new service",
                      default=(os.environ.get(SQUASH_MIGRATOR_NAMESPACE +
                                              "RESTFUL_API_URL")
                               or SQUASH_RESTFUL_API_URL))
    parser.add_option("-j", "--jobs", help="Job numbers to fetch")
    (params, _) = parser.parse_args()
    loglevel = params.loglevel
    if not loglevel:
        loglevel = 'info'
    loglevel = loglevel.lower()
    logmap = {'critical': logging.CRITICAL,
              'error': logging.ERROR,
              'warning': logging.WARNING,
              'info': logging.INFO,
              'debug': logging.DEBUG}
    params.loglevel = logmap.get(loglevel)
    logging.basicConfig(level=params.loglevel)
    logger = logging.getLogger(__name__)
    logger.setLevel(params.loglevel)
    logger.info("Creating migration object.")
    if params.jobs:
        jl = params.jobs.split(",")
        jobset = set()
        for j in jl:
            if j.find("-") != -1:
                fj, lj = j.split("-")
                jobset.update(range(int(fj), int(lj) + 1))
            else:
                jobset.add(int(j))
        params.jobs = jobset
    return params


def standalone():
    params = get_options()
    context = Context(user=params.user,
                      password=params.password,
                      token=params.token,
                      logger=None,
                      loglevel=params.loglevel,
                      directory=params.directory,
                      from_url=params.from_url,
                      to_url=params.to_url)
    extractor = Extractor(context=context)
    transformer = Transformer(context=context)
    loader = Loader(context=context)
    migrator = Migrator(context, extractor, transformer, loader)
    migrator.etl(jobs=params.jobs)
    transformer.transform(jobs=params.jobs)


if __name__ == "__main__":
    standalone()
