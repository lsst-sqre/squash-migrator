#!/usr/bin/env python3
"""Standalone executable and main class for SQuaSH migration tool.
"""

import argparse
import logging
import os
from .context import Context
from .defaults import SQUASH_MIGRATOR_NAMESPACE, SQUASH_API_URL,\
    SQUASH_RESTFUL_API_URL
from .extractor import Extractor
from .transformer import Transformer
from .loader import Loader


class Migrator:
    """Class to perform entire ETL process, looping over SQuaSH database jobs.
    """

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
        """Perform the extract/transform/load operation by delegating to
        actuators.
        """
        self.extractor.extract()
        self.transformer.transform()
        self.loader.load()


def _empty(obj, param):
    if not param or param not in obj or not obj.get(param):
        return True
    return False


def get_options():
    params = {}
    descstr = ("Command-line tool for SQuaSH ETL from old to new format. " +
               "Any option may also be specified in the environment as " +
               SQUASH_MIGRATOR_NAMESPACE + " prepended to the parameter " +
               "name (e.g. " + SQUASH_MIGRATOR_NAMESPACE + "USER). " +
               "The directory is used as a persistent cache, which must " +
               "be cleared if re-extraction/re-transformation is desired.")
    parser = argparse.ArgumentParser(description=descstr)
    parser.add_argument("-u", "--user", "--username",
                        help="username for (write) SQuaSH API communication",
                        default=os.environ.get(SQUASH_MIGRATOR_NAMESPACE +
                                               "USER"))
    parser.add_argument("-p", "--password", "--pass", "--pw",
                        help="password for (write) SQuaSH API communication",
                        default=os.environ.get(SQUASH_MIGRATOR_NAMESPACE +
                                               "PASSWORD"))
    parser.add_argument("-k", "--token",
                        help=("token for (write) SQuaSH API communication" +
                              " (overrides user/password if present)"),
                        default=os.environ.get(SQUASH_MIGRATOR_NAMESPACE +
                                               "TOKEN"))
    parser.add_argument("-d", "--directory",
                        help=("directory for SQuaSH data [default: " +
                              "./squash_data]"),
                        default=(os.environ.get(SQUASH_MIGRATOR_NAMESPACE +
                                                "DIRECTORY") or
                                 os.path.join(os.getcwd(), "squash_data")))
    parser.add_argument("-l", "--loglevel",
                        help="loglevel to use [default: info]",
                        default=os.environ.get(SQUASH_MIGRATOR_NAMESPACE +
                                               "TOKEN") or "info")
    parser.add_argument("-f", "--from-url", "--from",
                        help=("URL of old SQuaSH service [default: %s]" %
                              SQUASH_API_URL),
                        default=os.environ.get(SQUASH_MIGRATOR_NAMESPACE +
                                               "FROM_URL") or SQUASH_API_URL)
    parser.add_argument("-t", "--to-url", "--to",
                        help=("URL of new SQUaSH service [default: %s]" %
                              SQUASH_RESTFUL_API_URL),
                        default=(os.environ.get(SQUASH_MIGRATOR_NAMESPACE +
                                                "TO_URL")
                                 or SQUASH_RESTFUL_API_URL))
    parser.add_argument("-j", "--jobs",
                        help="Job numbers to fetch [default: all]",
                        default=os.environ.get(SQUASH_MIGRATOR_NAMESPACE +
                                               "JOBS"))

    params = parser.parse_args()
    loglevel = params.loglevel
    if not loglevel:
        loglevel = 'info'
    loglevel = loglevel.lower()[0]
    logmap = {'c': logging.CRITICAL,
              'e': logging.ERROR,
              'w': logging.WARNING,
              'i': logging.INFO,
              'd': logging.DEBUG}
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
                      to_url=params.to_url,
                      job_numbers=params.jobs)
    extractor = Extractor(context=context)
    transformer = Transformer(context=context)
    loader = Loader(context=context)
    migrator = Migrator(context, extractor, transformer, loader)
    migrator.etl()


if __name__ == "__main__":
    standalone()
