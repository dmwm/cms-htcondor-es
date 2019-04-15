#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Christian Ariza <christian.ariza AT gmail [DOT] com>
# pylint: disable=line-too-long
import os
import json
import requests
from datetime import datetime, timedelta


class AffiliationManager():
    __DEFAULT_URL = 'https://cms-cric-dev.cern.ch/api/accounts/user/query/?json'
    __DEFAULT_DIR_PATH = 'dir.pkl'

    def __init__(self,
                 pickle_file=__DEFAULT_DIR_PATH,
                 recreate=False,
                 recreate_older_days=None,
                 service_url=__DEFAULT_URL):
        """
        params:
            recreate: boolean
            recreate_older_days: int, recreate the dir if is older
                    than that number of days.
        """
        self.path = pickle_file
        self.url = service_url
        if not recreate\
           and recreate_older_days \
           and os.path.isfile(self.path):
            _min_date = datetime.now() - timedelta(days=recreate_older_days)
            _dir_time = datetime.fromtimestamp(os.path.getmtime(self.path))
            recreate = _dir_time < _min_date
        self.__dir = self.loadOrCreateDirectory(recreate)

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
        """
        _tmp_dir = None
        if not os.path.isfile(self.path) or recreate:
            with open(self.path, 'wb') as _dir_file:
                response = requests.get(self.url, verify=False)
                _json = json.loads(response.text)
                _tmp_dir = {}
                for x in _json:
                    person = _json[x]
                    login = None
                    for profile in person['profiles']:
                        if 'login' in profile:
                            login = profile['login']
                            break
                    if login:
                        _tmp_dir[login] = {'institute': _json[x]['institute'],
                                           'country': _json[x]['institute_country'],
                                           'dn': _json[x]['dn']}
                json.dump(_tmp_dir, _dir_file)
        else:
            with open(self.path, 'rb') as dir_file:
                _tmp_dir = json.load(dir_file)
        return _tmp_dir

    def getAffiliation(self, login=None, dn=None):
        """
        Returns a python dictionary with the institute and country
        for the given login or dn.
        Returns None if not found.
        """
        if login:
            return self.__dir[login]
        elif dn:
            for _person in self.__dir.values():
                if _person['dn'] == dn:
                    return _person
        return None
