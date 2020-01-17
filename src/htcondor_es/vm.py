import os
import time
import logging
import requests


def filter_attrs(ads, attrs=None):
    "Filter attributes from given class ad"
    doc = {}
    if attrs:
        if isinstance(attrs, str):
            attrs = [attrs]
        for attr in attrs:
            if attr in ads:
                doc[attr] = ads.get(attr)
        return doc
    # default mode
    return doc

def post_ads(url, ads, metadata=None, attrs=None):
    "Post classAds docs into VM url"
    if not len(ads):
        logging.warning("No new documents found")
        return

    starttime = time.time()
    failed = []
    data = [filter_attrs(ad, attrs) for _, ad in ads]
    req = requests.post(url, data)
    if req.status_code != 200:
        failed = data
        logging.warning("Fail to send to {}, reason {}".format(url, req.reason))
    elapsed = time.time() - starttime
    return (len(ads) - len(failed), len(ads), elapsed)
