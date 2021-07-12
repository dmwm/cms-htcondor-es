#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Christian Ariza <christian.ariza AT gmail [DOT] com>, Ceyhun Uzunoglu <cuzunogl AT gmail [DOT] com>

"""The tasks module define the spider's celery tasks
i.e. the task a spider celery worker knows.

Notes:
    ``SPIDER_CHECKPOINT`` environment variable stands for `redis-checkpoint` instance endpoint in k8s. It is required
    to be set in `spider-cron-queues`[1] and `spider-worker`[2] yamls. It represented by
    `redis://$(REDIS_CHECKPOINT_SERVICE_HOST):$(REDIS_CHECKPOINT_SERVICE_PORT)/0` string, and `REDIS_CHECKPOINT_SERVICE`
    values come from `redis-checkpoint` service[3] and deployment[4].
    ``AFFILIATION_DIR_LOCATION`` should be set in k8s. Please see `affiliation_cache.py` for detailed information.

Attributes:
    QUERY_QUEUES (str): Htcondor query for queues. Gets all jobs except for `Removed` and `Completed` jobs.
                        `EnteredCurrentStatus` or `CRAB_PostJobLastUpdate` should be greater than ``completed_since``
                        value which is now - 12 minutes as default.
    QUERY_HISTORY (str): Htcondor query for history jobs. Gets all jobs in history schedds. `EnteredCurrentStatus` or
                         `CRAB_PostJobLastUpdate` should be greater than `last_completion` value
                         which is fetched from ``Redis`` cache as default.
    `__REDIS_CONN` (object): Redis communication interface.

References:
    - [1] https://github.com/dmwm/CMSKubernetes/blob/master/kubernetes/spider/cronjobs/spider-cron-queues.yaml
    - [2] https://github.com/dmwm/CMSKubernetes/blob/master/kubernetes/spider/deployments/spider-worker.yaml
    - [3] https://github.com/dmwm/CMSKubernetes/blob/master/kubernetes/spider/service/spider-redis-cp.yaml
    - [4] https://github.com/dmwm/CMSKubernetes/blob/master/kubernetes/spider/deployments/spider-redis-cp.yaml

"""

import os
import time
import traceback

import classad
import htcondor
import htcondor_es.es
import redis
from htcondor_es.AffiliationManager import AffiliationManager
from htcondor_es.amq import amq_post_ads
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


def log_failure(self, exc, task_id, args, kwargs, einfo):
    """Send email message and log error.

    This only should be send if all retries failed.
    """
    message = f"failed to query {args}, {kwargs}, {exc}, task_id: {task_id}, einfo: {einfo}"
    print(f"ERROR: failed to query {args}, {kwargs}")
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
    soft_time_limit=1440,  # 24 min
    result_expires=43200,  # Delete tasks from Redis backend if older than 12 hours
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
    """Celery task which query a schedd for the job classads, either from the queues or the history.

    Then convert the documents to the appropiated format and send it to AMQ and, if required, to ES.
    This is **initial** task which called in `spider_cron_queue` k8s deployment via `celery_spider_cms`.

    Notes:
        - Uses `htcondor.Schedd` class to query pools (Global, ITB, Volunteer, etc).
        - Each `query_schedd` query either queue or history.
        - Checkpoint only set for ``history`` queries in Redis, `redis-checkpoint``.
        - Queue queries only fetch last 12 minutes results in condor schedds.
        - First run (no checkpoint) of history queries fetch last 1 hour results.
        - History query has 10000 match limit, see in condor documentation.
        - ``pool_name (str)``: Pool name. Used to provide CMS_Pool value in converted jsons in `convert_to_json`.
        - ``schedd (object)``: Client object for a condor_schedd.
        - ``query_iter (list[ClassAd])``: Iterator which consists of ClassAd objects.
        - ``start_time (float)``: Provided by `celery_spider_cms.main` which means when the `query_schedd` put into
                queue, this value is already set.
        - ``hist_time (float)``: Set exactly before querying history.
                Used to set checkpoint for that schedd in ``redis-checkpoint``, i.e. ``SPIDER_CHECKPOINT``
        - ``_metadata (dict)``: Metadata of running environment. See `htcondor_es.utils.collect_metadata`

    Args:
        schedd_ad: Condor Schedd classad to query. See `htcondor_es.utils.get_schedds` for it's details.
        start_time: timestamp
        keep_full_queue_data: should we keep all the fields on non completed jobs?
        dry_run: do not query
        chunk_size: How many documents should we send to the AMQ/ES in each batch?
        bunch: How many documents should we process in each processing task?
        query_type: either history or queue
        es_index_template: ES index prefix
        feed_es: should we send the data to ES?

    Returns:
        tuple: (str, int) # (schedd_ad["name"], n_tasks), n_tasks comes from `send_data` method.

    References:
        - htcondor.Schedd:
            -- https://htcondor.readthedocs.io/en/latest/apis/python-bindings/api/htcondor.html#htcondor.Schedd
        - schedd.xquery:
            -- https://htcondor.readthedocs.io/en/latest/apis/python-bindings/api/htcondor.html#htcondor.Schedd.query
        - schedd.history:
            -- https://htcondor.readthedocs.io/en/latest/apis/python-bindings/api/htcondor.html#htcondor.Schedd.history

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
        query_iter = schedd.history(constraint=history_query,
                                    projection=[],  # An empty list (the default) returns all attributes.
                                    match=10000  # An limit on the number of jobs to include.
                                    ) if not dry_run else []
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
    result_expires=43200,  # delete tasks from Redis backend if older than 12 hours
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
    """Celery task which processes the documents to a suitable format send to AMQ and ES.

    Sends converted ClassAd json to AMQ by running `amq_post_ads` function
    and also to ES if required by calling `post_ads_es` task.

    Args:
        docs (list[dict]): iterable with the jobs' classads.
        reduce_data (bool): Should we slim down the running/pending jobs records? Related with `keep_full_queue_data`
            see `send_data` method: `reduce_data=not keep_full_queue_data`
        pool_name (str): Pool of the source schedd
        feed_es (bool): Should we send the data to ES?
        es_index (str): Elasticsearch index prefix
        metadata (dict): dictionary with the additional metadata (used only for ES documents).
        feed_es_only_completed (bool): Should we send all the documents to ES or only the completed/removed.
            Default is used as True.
        start_time (float): Comes from celery_spider_cms -> query_schedd -> send_data

    Returns:
        htcondor_es.amq.amq_post_ads # It returns tuple of (successful tasks, total tasks, elapsed time)

    """
    converted_docs = []
    #: list(tuple(str, dict)): unique_doc_id and converted ClassAd dict pairs.
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
                and
                (
                    not feed_es_only_completed
                    or c_doc["Status"] in ("Completed", "Removed")
                )
            ):
                es_docs.append((unique_doc_id(c_doc), c_doc))
        except Exception as e:
            print("WARNING: Error on convert_to_json: {} {}".format(str(e), str(traceback.format_exc())))
            continue
    if es_docs:
        post_ads_es.si(es_docs, es_index, metadata).apply_async()
    return amq_post_ads(converted_docs, metadata=metadata) if converted_docs else []


@app.task(ignore_result=True, queue="es_post")
def post_ads_es(es_docs, es_index, metadata=None):
    """Celery task which send the messages to ES.

    Determine the index and send the messages.

    Args:
        es_docs (list(tuple(str, dict)): iterable with pairs (doc_id, doc).
        es_index (str): Index prefix.
        metadata (dict): Dictionary with the metadata.

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
        print("ERROR: Error on post_ads_es: {}".format(str(e)))


@app.task(ignore_result=True)
def create_affiliation_dir(days=1):
    """Celery task which creates affiliation json.

    Notes:
        Please `affiliation_cache` file to see where affiliation json is stored in k8s.

    """
    try:
        output_file = os.getenv(
            "AFFILIATION_DIR_LOCATION",
            AffiliationManager._AffiliationManager__DEFAULT_DIR_PATH,
        )
        AffiliationManager(recreate_older_days=days, dir_file=output_file)
        print("Affiliation creation successful.")
    except Exception as e:
        print("ERROR: Error on create_affiliation_dir: {}".format(str(e)))
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
    """Sends data to AMQ and, optionally, to ES.

    Called by `query_schedd` task and calls `process_docs` after grouped `query_iter` results into chunks.
    It will receive an iterator which consists of ClassAd objects, and will process
    the documents in {bunch} batches, and send the converted documents in {chunks}

    Args:
        query_iter (list[ClassAd]): Iterable (generator) with the job ClassAds.
        chunk_size (int): How many documents send in each batch.
        bunch (int): How many documents process in each task.
        pool_name (str): Get from schedd_ad.get("CMS_Pool").
            Used to provide `CMS_Pool` value in converted json in `convert_to_json`.
        keep_full_queue_data (bool): Should we kee all the fields for all the jobs.
            Related with `drop_fields_for_running_jobs` in `convert_to_json`
        feed_es (bool): Should we send the data to es.
        es_index (str): Index prefix.
        metadata (dict): Metadata of running environment. See `htcondor_es.utils.collect_metadata`
        start_time (float): Provided by `celery_spider_cms.main` which means when the `query_schedd` put into
                queue, this value is already set.

    """
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
        # filter: If function is None, return the items that are true.
        #: int: Total successful tasks.
        total_tasks += len(list(filter(None, docs_bunch)))
        # responses.append(process_and_send.apply_async(serializer="pickle"))
    return total_tasks


def get_redis_connection():
    """A singleton-like method to mantain the redis connection.

    The redis object will mantain a connection pool and will be resposible to close the connections once
    the worker is terminated. See redis-py documentation for more details.

    Returns:
        object: Redis interface
    """
    global __REDIS_CONN
    if not __REDIS_CONN:
        __REDIS_CONN = redis.Redis.from_url(
            os.getenv("SPIDER_CHECKPOINT", "redis://localhost/1")
        )
    return __REDIS_CONN
