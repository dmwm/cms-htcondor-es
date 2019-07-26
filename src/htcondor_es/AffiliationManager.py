#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Christian Ariza <christian.ariza AT gmail [DOT] com>
# pylint: disable=line-too-long
import os
from pathlib import Path
import errno
import json
import requests
from datetime import datetime, timedelta


class AffiliationManager:
    __DEFAULT_URL = "https://cms-cric-dev.cern.ch/api/accounts/user/query/?json"
    __DEFAULT_DIR_PATH = Path.home().joinpath(".affiliation_dir.json")

    def __init__(
        self,
        dir_file=__DEFAULT_DIR_PATH,
        recreate=False,
        recreate_older_days=None,
        service_url=__DEFAULT_URL,
    ):
        """
        params:
            recreate: boolean
            recreate_older_days: int, recreate the dir if is older
                    than that number of days.
        """
        self.path = Path(dir_file)
        self.url = service_url
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
        raises  IOError if the file doesn't exists (of it cannot be read)
                   RequestException if something happen with the request
                   HTTPError if the response was something different
                   to a success
        """
        _tmp_dir = None
        if recreate:
            response = requests.get(self.url)
            response.raise_for_status()
            _json = json.loads(response.text)
            _tmp_dir = {}
            for person in list(_json.values()):
                login = None
                for profile in person["profiles"]:
                    if "login" in profile:
                        login = profile["login"]
                        break
                if login:
                    _tmp_dir[login] = {
                        "institute": person["institute"],
                        "country": person["institute_country"],
                        "dn": person["dn"],
                    }
            # Only override the file if the dict is not empty.
            if _tmp_dir:
                with open(self.path, "w") as _dir_file:
                    json.dump(_tmp_dir, _dir_file)
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
