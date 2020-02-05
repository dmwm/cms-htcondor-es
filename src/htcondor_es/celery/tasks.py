# -*- coding: utf-8 -*-
# Author: Christian Ariza <christian.ariza AT gmail [DOT] com>
"""The tasks module define the spider's celery tasks
i.e. the task a spider celery worker knows.
"""

import os
import re
import time
import htcondor
import classad
from celery import group
from itertools import zip_longest, islice
import collections
from htcondor_es.convert_to_json import (
    convert_to_json,
    unique_doc_id,
    convert_dates_to_millisecs,
)
from htcondor_es.utils import (
    get_schedds,
    set_up_logging,
    send_email_alert,
    TIMEOUT_MINS,
)
from htcondor_es.amq import post_ads
import traceback
import redis

from .celery import app

QUERY_QUEUES = """
         (JobStatus < 3 || JobStatus > 4 
         || EnteredCurrentStatus >= %(completed_since)d
         || CRAB_PostJobLastUpdate >= %(completed_since)d
         ) && (CMS_Type != "DONOTMONIT")
         """
QUERY_HISTORY = """
        ( EnteredCurrentStatus >= %(last_completion)d 
        || CRAB_PostJobLastUpdate >= %(last_completion)d )
        && (CMS_Type != "DONOTMONIT")
        """
__REDIS_CONN = None


@app.task(max_retries=3)
def query_schedd(
    schedd_ad,
    start_time=None,
    keep_full_queue_data=False,
    dry_run=False,
    chunk_size=50,
    bunch=20000,
    query_type="queue",
):
    pool_name = schedd_ad.get("CMS_Pool", "Unknown")
    if not start_time:
        start_time = time.time()
    query_iter = []
    schedd = htcondor.Schedd(schedd_ad)
    hist_time = start_time
    if query_type == "queue":
        _completed_since = start_time - (TIMEOUT_MINS + 1) * 60
        query = QUERY_QUEUES % {"completed_since": _completed_since}
        query_iter = schedd.xquery(requirements=query) if not dry_run else []
    elif query_type == "history":
        last_completion = getRedisConnection().get(schedd_ad["name"]) or (
            start_time - 3600
        )
        history_query = classad.ExprTree(
            QUERY_HISTORY % {"last_completion": last_completion}
        )
        query_iter = schedd.history(history_query, [], 10000)
        hist_time = time.time()
    responses = send_data(
        query_iter,
        chunk_size,
        bunch,
        pool_name,
        keep_full_queue_data=keep_full_queue_data,
    )
    if query_type == "history":
        getRedisConnection().set(schedd_ad["name"], hist_time)
    return (schedd_ad["name"], responses)


@app.task
def process_docs(docs, reduce_data=True, pool_name="UNKNOWN"):
    converted_docs = []
    for doc in docs:
        try:
            c_doc = convert_to_json(
                doc, return_dict=True, reduce_data=reduce_data, pool_name=pool_name
            )
            if c_doc:
                converted_docs.append(
                    (unique_doc_id(c_doc), convert_dates_to_millisecs(c_doc))
                )
        except Exception as e:
            traceback.print_exc()
            continue
    return post_ads(converted_docs) if converted_docs else []


def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return filter(None, zip_longest(*args, fillvalue=fillvalue))


def consume(iterator, n=None):
    "Advance the iterator n-steps ahead. If n is None, consume entirely."
    # Use functions that consume iterators at C speed.
    if n is None:
        # feed the entire iterator into a zero-length deque
        collections.deque(iterator, maxlen=0)
    else:
        # advance to the empty slice starting at position n
        next(islice(iterator, n, n), None)


def send_data(query_iter, chunk_size, bunch, pool_name, keep_full_queue_data=False):
    responses = []
    for docs_bunch in grouper(query_iter, bunch):
        process_and_send = group(
            process_docs.s(
                list(filter(None, X)),
                reduce_data=not keep_full_queue_data,
                pool_name=pool_name,
            )
            for X in grouper(docs_bunch, chunk_size)
        )
        responses.append(process_and_send.apply_async())
    return responses


def getRedisConnection():
    global __REDIS_CONN
    if not __REDIS_CONN:
        __REDIS_CONN = redis.Redis.from_url(
            os.getenv("SPIDER_CHECKPOINT", "redis://localhost/1")
        )
    return __REDIS_CONN
