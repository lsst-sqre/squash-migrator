"""Module to capture execution context for Squash ETL."""

import logging
import requests


class Context:

    def __init__(self, user=None, password=None, token=None,
                 logger=None, loglevel=None, directory=None, from_url=None,
                 to_url=None):
        self.from_url = from_url
        self.to_url = to_url
        if not logger:
            logger = logging.getLogger(__name__)
        self.logger = logger
        if not loglevel:
            loglevel = logging.INFO
        self.loglevel = loglevel
        logger.setLevel(loglevel)
        self.directory = directory
        if to_url and not token:
            logger.debug("Trying to acquire token for '%s'." % to_url)
            ustruct = {"username": user,
                       "password": password
                       }
            if user and password:
                resp = requests.get(to_url + "/user/ " + user)
                if resp.status_code != requests.codes.ok:
                    logger.debug("Trying to create user '%s'." % user)
                    # Try creating the user
                    resp = requests.post(to_url + "/register", json=ustruct)
                    # If we don't have a user, this will fail.
                logger.debug("Getting token for user '%s'" % user)
                resp = requests.post(to_url + "/auth", json=ustruct)
                try:
                    token = resp.json()['access_token']
                except (KeyError, ValueError):
                    pass
        user = None
        password = None
        if token:
            self.headers = {
                "Authorization": "JWT " + token
            }
        else:
            self.headers = {}
            logger.warning("Could not get token for context; no write to %s" %
                           to_url)