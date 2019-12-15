import os
import time
import logging
import requests


def post_ads(url, ads, metadata=None):
    "Post classAds docs into VM url"
    if not len(ads):
        logging.warning("No new documents found")
        return

    starttime = time.time()
    failed = []
    data = [ad for _, ad in ads]
    req = requests.post(url, data)
    if req.status_code != 200:
        failed = data
        logging.warning("Fail to send to {}, reason {}".format(url, req.reason))
    elapsed = time.time() - starttime
    return (len(ads) - len(failed), len(ads), elapsed)
