#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Christian Ariza <christian.ariza AT gmail [DOT] com>

"""The tasks module define the spider's celery tasks
i.e. the task a spider celery worker knows.
"""

import os
import time
import traceback

import classad
import htcondor

import htcondor_es.es
import redis
from htcondor_es.AffiliationManager import AffiliationManager
from htcondor_es.amq import post_ads
from htcondor_es.celery.celery import app
from htcondor_es.convert_to_json import (
    convert_to_json,
    unique_doc_id,
    convert_dates_to_millisecs,
)
from htcondor_es.utils import collect_metadata, grouper, send_email_alert, TIMEOUT_MINS

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

# logger = get_task_logger(__name__)


def log_failure(self, exc, task_id, args, kwargs, einfo):
    """Send email message and log error.
    (this only should be send if all retries failed)
    """
    message = f"failed to query {args}, {kwargs}, {exc}, task_id: {task_id}, einfo: {einfo}"
    print(f"failed to query {args}, {kwargs}")
    # logger.error(f"failed to query {args}, {kwargs}")
    # TODO: Change email with parameters
    send_email_alert("ceyhun.uzunoglu@cern.ch", "[Spider] Failed to query", message)


# ---Tasks----
@app.task(
    max_retries=3,
    autoretry_for=(RuntimeError,),  # When a schedd cannot be contacted, retry.
    acks_late=True,  # Only ack the message when done
    retry_backoff=5,  # Wait between retries (5, 10,15)s
    reject_on_worker_lost=True,  # If the worker is killed (e.g. by k8s) reasign the task
    on_failure=log_failure,
    task_reject_on_worker_lost=True,
    soft_time_limit=300,  # 5 min
)
def query_schedd(
    schedd_ad,
    start_time=None,
    keep_full_queue_data=False,
    dry_run=False,
    chunk_size=50,
    bunch=20000,
    query_type="queue",
    es_index_template="cms-test",
    feed_es=False,
):
    """
    Query a schedd for the job classads, either from the queues
    or the history, convert the documents to the appropiated format and
    send it to AMQ and, if required, to ES.
    params:
        schedd_ad: Condor Schedd classad to query.
        start_time: timestamp
        keep_full_queue_data: should we keep all the fields on non completed jobs?
        dry_run: do not query
        chunk_size: How many documents should we send to the AMQ/ES in each batch?
        bunch: How many documents should we process in each processing task?
        query_type: either history or queue
        es_index_template: ES index prefix
        feed_es: should we send the data to ES?
    """
    pool_name = schedd_ad.get("CMS_Pool", "Unknown")
    if not start_time:
        start_time = time.time()
    query_iter = []
    schedd = htcondor.Schedd(schedd_ad)
    hist_time = start_time
    if query_type == "queue":
        _completed_since = start_time - (TIMEOUT_MINS + 1) * 60
        query = QUERY_QUEUES % {"completed_since": _completed_since}
        query_iter = schedd.xquery(constraint=query) if not dry_run else []
    elif query_type == "history":
        last_completion = float(
            get_redis_connection().get(schedd_ad["name"]) or (start_time - 3600)
        )
        history_query = classad.ExprTree(
            QUERY_HISTORY % {"last_completion": last_completion}
        )
        hist_time = time.time()
        query_iter = schedd.history(history_query, [], 10000) if not dry_run else []
    _metadata = collect_metadata()
    n_tasks = send_data(
        query_iter,
        chunk_size,
        bunch,
        pool_name,
        keep_full_queue_data=keep_full_queue_data,
        feed_es=feed_es,
        es_index=es_index_template,
        metadata={"spider_source": f"condor_{query_type}", **_metadata},
        start_time=start_time,
    )
    if query_type == "history":
        get_redis_connection().set(schedd_ad["name"], hist_time)
    return schedd_ad["name"], n_tasks


@app.task(
    ignore_result=True,
    queue="convert",
    acks_late=True,
    max_retries=3,
    autoretry_for=(OSError,),
)
def process_docs(
    docs,
    reduce_data=True,
    pool_name="UNKNOWN",
    feed_es=True,
    es_index="cms-test-k8s",
    metadata=None,
    feed_es_only_completed=True,
    start_time=None,
):
    """
    process the documents to a suitable format,
    and send it to AMQ and, if required, to ES.
    params:
        docs: iterable with the jobs' classads.
        reduce_data: Should we slim down the running/pending jobs records?
        pool_name: pool of the source schedd
        feed_es: Should we send the data to ES?
        es_index: Elasticsearch index prefix
        metadata: dictionary with the additional metadata
           (used only for ES documents).
        feed_es_only_completed: Should we send all the documents to ES
            or only the completed/removed.
    """
    converted_docs = []
    es_docs = []
    for doc in docs:
        try:
            c_doc = convert_to_json(
                doc,
                return_dict=True,
                reduce_data=reduce_data,
                pool_name=pool_name,
                start_time=start_time,
            )
            if c_doc:
                converted_docs.append(
                    (unique_doc_id(c_doc), convert_dates_to_millisecs(c_doc.copy()))
                )
            if (
                feed_es
                and c_doc
                and (
                    not feed_es_only_completed
                    or c_doc["Status"] in ("Completed", "Removed")
                )
            ):
                es_docs.append((unique_doc_id(c_doc), c_doc))
        except Exception as e:
            print("[LOGGING - test]Error on convert_to_json: {} {}".format(str(e), str(traceback.format_exc())))
            # logging.error("[LOGGING - test]Error on convert_to_json: {} {}".
            # format(str(e), str(traceback.format_exc())))
            # logger.error("Error on convert_to_json: {}".format(str(e)))
            continue
    if es_docs:
        post_ads_es.si(es_docs, es_index, metadata).apply_async()
    return post_ads(converted_docs, metadata=metadata) if converted_docs else []


@app.task(ignore_result=True, queue="es_post")
def post_ads_es(es_docs, es_index, metadata=None):
    """
    Send the messages to ES.
    Determine the index and send the messages.
    params:
        es_docs: iterable with pairs (doc_id, doc)
        es_index: index prefix
        metadata: dictionary with the metadata.
    """
    try:
        metadata = metadata or {}
        es = htcondor_es.es.get_server_handle()

        es_indexes = {}
        for job in es_docs:
            _idx = htcondor_es.es.get_index(job[1]["RecordTime"], es_index)
            if _idx not in es_indexes:
                es_indexes[_idx] = []
            es_indexes[_idx].append(job)
        for _idx in es_indexes:
            htcondor_es.es.post_ads(
                es.handle, _idx, es_indexes[_idx], metadata=metadata
            )
    except Exception as e:
        print("[LOGGING - test] Error on post_ads_es: {}".format(str(e)))
        # logging.error("[LOGGING - test] Error on post_ads_es: {}".format(str(e)))
        # logger.error("Error on post_ads_es: {}".format(str(e)))


@app.task(ignore_result=True)
def create_affiliation_dir(days=1):
    try:
        output_file = os.getenv(
            "AFFILIATION_DIR_LOCATION",
            AffiliationManager._AffiliationManager__DEFAULT_DIR_PATH,
        )
        AffiliationManager(recreate_older_days=days, dir_file=output_file)
        print("Affiliation creation successful.")
    except Exception as e:
        print("[LOGGING - test] Error on create_affiliation_dir: {}".format(str(e)))
        # logging.error("[LOGGING - test] Error on create_affiliation_dir: {}".format(str(e)))
        # logger.error("Error on create_affiliation_dir: {}".format(str(e)))
        pass


def send_data(
    query_iter,
    chunk_size,
    bunch,
    pool_name,
    keep_full_queue_data=False,
    feed_es=False,
    es_index="cms-test",
    metadata=None,
    start_time=None,
):
    """
    Send the data to AMQ and, optionally, to ES.
    It will recieve an iterator, and will process
    the documents in {bunch} batches, and send the converted documents
    in {chunks}
    params:
        query_iter: Iterable (generator) with the job classads.
        chunk_size: how many documents send in each batch.
        bunch: how many documents process in each task.
        keep_full_queue_data: should we kee all the fields for all the jobs?
        feed_es: should we send the data to es?
        es_index: index prefix.
    """
    # responses = []
    total_tasks = 0
    for docs_bunch in grouper(query_iter, bunch):
        for X in grouper(docs_bunch, chunk_size):
            process_docs(
                list(filter(None, X)),  # filter: If function is None, return the items that are true.
                reduce_data=not keep_full_queue_data,
                pool_name=pool_name,
                feed_es=feed_es,
                es_index=es_index,
                metadata=metadata,
                start_time=start_time,
            )
        total_tasks += len(list(filter(None, docs_bunch)))
        # responses.append(process_and_send.apply_async(serializer="pickle"))
    return total_tasks


def get_redis_connection():
    """
    A singleton-like method to mantain the redis connection.
    The redis object will mantain a connection pool and
    will be resposible to close the connections once
    the worker is terminated.
    see redis-py documentation for more details.
    """
    global __REDIS_CONN
    if not __REDIS_CONN:
        __REDIS_CONN = redis.Redis.from_url(
            os.getenv("SPIDER_CHECKPOINT", "redis://localhost/1")
        )
    return __REDIS_CONN
