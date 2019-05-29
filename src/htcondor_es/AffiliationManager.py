#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Christian Ariza <christian.ariza AT gmail [DOT] com>
# pylint: disable=line-too-long
import os
import errno
import json
import requests
from datetime import datetime, timedelta



class AffiliationManager():
    __DEFAULT_URL = 'https://cms-cric-dev.cern.ch/api/accounts/user/query/?json'
    __DEFAULT_DIR_PATH = '/tmp/dir.json'

    def __init__(self,
                 dir_file=__DEFAULT_DIR_PATH,
                 recreate=False,
                 recreate_older_days=None,
                 service_url=__DEFAULT_URL):
        """
        params:
            recreate: boolean
            recreate_older_days: int, recreate the dir if is older
                    than that number of days.
        """
        self.path = dir_file
        self.url = service_url
        if not recreate\
           and recreate_older_days:
                if os.path.isfile(self.path):
                    _min_date = datetime.now() - timedelta(days=recreate_older_days)
                    _dir_time = datetime.fromtimestamp(os.path.getmtime(self.path))
                    recreate = _dir_time < _min_date
                else:
                    recreate = True

        try:
            self.__dir = self.loadOrCreateDirectory(recreate)
            self.__dn_dir = {person["dn"]:person for person in self.__dir.values()}
        except (IOError, requests.RequestException, requests.HTTPError) as cause:
            raise AffiliationManagerException(cause)
            # python 3 note:
            # this line should be:
            #  raise AffiliationManagerException()  from cause
            # In order to keep the traceback.

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
            with open(self.path, 'wb') as _dir_file:
                response = requests.get(self.url, verify=False)
                response.raise_for_status()
                _json = json.loads(response.text)
                _tmp_dir = {}
                for person in _json.values():
                    login = None
                    for profile in person['profiles']:
                        if 'login' in profile:
                            login = profile['login']
                            break
                    if login:
                        _tmp_dir[login] = {'institute': person['institute'],
                                           'country': person['institute_country'],
                                           'dn': person['dn']}
                json.dump(_tmp_dir, _dir_file)
        elif os.path.isfile(self.path):
            with open(self.path, 'rb') as dir_file:
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
        elif dn:
            return self.__dn_dir.get(dn)
        return None


class AffiliationManagerException(Exception):
    pass
