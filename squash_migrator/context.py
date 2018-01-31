"""Module to capture execution context for Squash ETL."""

import logging
import os
import requests


class Context:

    def __init__(self, user=None, password=None, token=None,
                 logger=None, loglevel=None, directory=None, url=None):
        if not url:
            raise RuntimeError("Context requires URL")
        self.url = url
        if not logger:
            logger = logging.getLogger(__name__)
        self.logger = logger
        if not loglevel:
            loglevel = logging.INFO
        self.loglevel = loglevel
        logger.setLevel(loglevel)
        if not token:
            logging.debug("Trying to acquire token.")
            ustruct = {"username": user,
                       "password": password
                       }
            if user and password:
                resp = requests.get(url + "/user/ " + user)
                if resp.status_code != requests.codes.ok:
                    logging.debug("Trying to create user '%s'." % user)
                    # Try creating the user
                    resp = requests.post(url + "/register", json=ustruct)
                    # If we don't have a user, this will fail.
                logging.debug("Getting token for user '%s'" % user)
                resp = requests.post(url + "/auth", json=ustruct)
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
            logging.warning("Could not get token for context; no write to %s" %
                            url)
