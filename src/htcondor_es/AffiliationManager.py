#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Christian Ariza <christian.ariza AT gmail [DOT] com>
# pylint: disable=line-too-long
import errno
import json
import logging
import os
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path

import requests

AFFILIATION_LOG_DIR = "/home/cmsjobmon/cms-htcondor-es/log_aff"


def setup_logging():
    """
    Affiliation cache logger
    """
    _logger = logging.getLogger("affiliation_logger")
    _logger.setLevel(logging.INFO)
    try:
        if not os.path.exists(AFFILIATION_LOG_DIR):
            os.makedirs(AFFILIATION_LOG_DIR)
    except Exception as e:
        _logger.warning("AFFILIATION_LOG_DIR does not exist: " + AFFILIATION_LOG_DIR + str(e))
    log_file = os.path.join(AFFILIATION_LOG_DIR, "affiliation.log")
    log_handler = RotatingFileHandler(log_file, maxBytes=100000, backupCount=5)
    log_handler.setFormatter(logging.Formatter("%(asctime)s : %(name)s:%(levelname)s - %(message)s"))
    _logger.addHandler(log_handler)


setup_logging()
aff_logger = logging.getLogger("affiliation_logger")


class AffiliationManager:
    __DEFAULT_DIR_PATH = Path(os.path.join(os.getenv("SPIDER_WORKDIR", "/home/cmsjobmon/cms-htcondor-es"),
                                           ".affiliation_dir.json"))
    __DEFAULT_URL = "https://cms-cric.cern.ch/api/accounts/user/query/?json"
    __DEFAULT_CA_CERT = "/etc/pki/tls/certs/CERN-bundle.pem"
    __DEFAULT_ROBOT_CERT = "/home/cmsjobmon/.globus/usercert.pem"
    __DEFAULT_ROBOT_KEY = "/home/cmsjobmon/.globus/userkey.pem"

    def __init__(
        self,
        dir_file=__DEFAULT_DIR_PATH,
        recreate=False,
        recreate_older_days=None,
        service_url=__DEFAULT_URL,
        robot_cert=__DEFAULT_ROBOT_CERT,
        robot_key=__DEFAULT_ROBOT_KEY,
        ca_cert=__DEFAULT_CA_CERT,
    ):
        """
        params:
            recreate: boolean
            recreate_older_days: int, recreate the dir if is older
                    than that number of days.
        """
        self.path = Path(dir_file)
        self.url = service_url
        self.path = Path(dir_file)
        self.url = service_url
        self.robot_cert = robot_cert
        self.robot_key = robot_key
        self.ca_cert = ca_cert
        if not recreate and recreate_older_days:
            if self.path.is_file():
                _min_date = datetime.now() - timedelta(days=recreate_older_days)
                _dir_time = datetime.fromtimestamp(self.path.stat().st_mtime)
                recreate = _dir_time < _min_date
            else:
                recreate = True

        try:
            self.__dir = self.loadOrCreateDirectory(recreate)
            self.__dn_dir = {
                person["dn"]: person for person in list(self.__dir.values())
            }
        except (
            IOError,
            requests.RequestException,
            requests.HTTPError,
            json.JSONDecodeError,
        ) as cause:
            aff_logger.error("Affiliation instance initialization error: " + str(cause))
            raise AffiliationManagerException from cause

    def loadOrCreateDirectory(self, recreate=False):
        """
        Create or load from a json file an inverted
        index of instutions by person login. e.g.:

        {
            'valya':{u'country': u'US',
                     u'institute': u'Cornell University'},
            'belforte': {u'country': u'IT',
                         u'institute': u'Universita e INFN Trieste'}
            ...
        }
        raises  IOError if the file doesn't exist (of it cannot be read)
                   RequestException if something happen with the request
                   HTTPError if the response was something different
                   to a success
        """
        aff_logger.debug("Affiliation load or create args. recreate:" + str(recreate))
        _tmp_dir = None
        if recreate:
            # response = requests.get(self.url) #no auth
            cert = (self.robot_cert, self.robot_key)
            response = requests.get(self.url, cert=cert, verify=self.ca_cert)
            response.raise_for_status()

            _json = json.loads(response.text)
            _tmp_dir = {}
            for person in list(_json.values()):
                login = None
                for profile in person["profiles"]:
                    if "login" in profile:
                        login = profile["login"]
                        break
                if login and "institute" in person:
                    _tmp_dir[login] = {
                        "institute": person["institute"],
                        "country": person["institute_country"],
                        "dn": person["dn"],
                    }
            aff_logger.debug("Temp affiliations before written: " + str(_tmp_dir))
            # Only override the file if the dict is not empty.
            if _tmp_dir:
                with open(self.path, "w") as _dir_file:
                    json.dump(_tmp_dir, _dir_file)
            aff_logger.info("Successfully recreated: " + str(self.path))
        elif self.path.is_file():
            with open(self.path, "r") as dir_file:
                _tmp_dir = json.load(dir_file)
        else:
            raise IOError(errno.ENOENT, os.strerror(errno.ENOENT), self.path)
        return _tmp_dir

    def getAffiliation(self, login=None, dn=None):
        """
        Returns a python dictionary with the institute and country
        for the given login or dn.
        Returns None if not found.
        """
        if login:
            return self.__dir.get(login)
        if dn:
            return self.__dn_dir.get(dn)
        return None


class AffiliationManagerException(Exception):
    """
    Exception wrapper for problems that prevents us to obtain the affiliation info. 
    """
    pass
