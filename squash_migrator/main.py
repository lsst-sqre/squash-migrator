#!/usr/bin/env python3

import logging
import os
from optparse import OptionParser
from .context import Context
from .defaults import SQUASH_MIGRATOR_NAMESPACE, SQUASH_API_URL
from .extractor import Extractor
from .transformer import Transformer
from .loader import Loader


class Migrator:

    def __init__(self, context, extractor, transformer, loader):
        self.context = context
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    def etl(self):
        pass


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
    parser.add_option("-n", "--user", "--username",
                      help="username for API communication")
    parser.add_option("-p", "--password", "--pass", "--pw",
                      help="username for API communication")
    parser.add_option("-t", "--token",
                      help="token for API communication")
    parser.add_option("-l", "--loglevel",
                      help="loglevel to use",
                      default="info")
    parser.add_option("-u", "--url",
                      help="URL for API communication",
                      default=os.environ.get(SQUASH_MIGRATOR_NAMESPACE +
                                             "API_URL") or SQUASH_API_URL)
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
    return params


def standalone():
    params = get_options()
    context = Context(user=params.user,
                      password=params.password,
                      token=params.token,
                      logger=None,
                      loglevel=params.loglevel,
                      directory=params.directory,
                      url=params.url)
    extractor = Extractor(context=context)
    transformer = Transformer(context=context)
    loader = Loader(context=context)
    migrator = Migrator(context, extractor, transformer, loader)
    migrator.etl()
