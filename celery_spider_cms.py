#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Authors: Christian Ariza <christian.ariza AT gmail [DOT] com>, Ceyhun Uzunoglu <cuzunogl AT gmail [DOT] com>

"""
Celery version of the cms htcondor_es spider.
This version has some major changes:
    - Celery based
    - The same function is used for either the queues and history queries.
    - The history checkpoint for the schedds is stored in Redis instead of a json file.
    - Parallelism is managed by celery
Used in `spider-cron-queues` CronJob which runs in each 12 minutes.

Note:
    - ``COLLECTORS_FILE_LOCATION`` should be set in k8s pods. Currently, it is provided by the secret ``collectors``.
        Collectors file is necessary to get schedds information from htcondor.
        Usage in k8s:
            In Kubernetes, this environment variable set the collectors file location. Used in:
            https://github.com/dmwm/CMSKubernetes/blob/master/kubernetes/spider/cronjobs/spider-cron-queues.yaml
            https://github.com/dmwm/CMSKubernetes/blob/master/kubernetes/spider/deployments/spider-worker.yaml
        Deployment of ``collectors`` secret:
            https://github.com/dmwm/CMSKubernetes/blob/master/kubernetes/spider/deploy.sh
    - celery `group` primitive runs `query_schedd` tasks in parallel, however, because of `worker_prefetch_multiplier`
        is set as 1 in ``htcondor_es.celery.celery.py``, `query_schedd` tasks are evenly distributed among workers.

Attributes:
    __TYPE_HISTORY (str): Used to set type is history. History results send to es-cms directly.
        Used with ``--skip_history`` parameter
    __TYPE_QUEUE (str): Used to set type is queue.
        Used with ``--skip_queue`` parameter

Examples:
    In ``spider-cron-queues`` production:
        $ python /cms-htcondor-es/celery_spider_cms.py \
            --feed_es \
            --query_queue_batch_size "500" \
            --amq_bunch_size "500"
            # --es_index_template "cms-test-k8s" # default
            # --schedd_filter "" # default, No filter
            # --collectors_file is set by `COLLECTORS_FILE_LOCATION` in argument defaults

"""

import argparse
import os
import time

from celery import group

from htcondor_es.celery.tasks import query_schedd, create_affiliation_dir
from htcondor_es.utils import get_schedds, get_schedds_from_file

__TYPE_HISTORY = "history"
__TYPE_QUEUE = "queue"


def main_driver(args):
    """Gets condor schedds and calls celery `query_schedd` task for each schedd.

    Important:
        Tasks are called in order.
        - ``query_schedd`` task runs `send_data` method.
        - `send_data` method calls ``process_docs`` task.
        - ``process_docs`` task runs `convert_to_json`, `amq_post_ads` methods and calls ``post_ads_es`` task.

    Notes:
        Submission of tasks to queue entailed with `group`[1] primitive of celery. `group` accepts list of tasks and
        they are applied in parallel. Details of current task submission:
            - `query_schedd.si()` creates a signature of `query_schedd` task. This signature will be passed to workers.
            - `query_schedd` task signature is created for each types(queue, history) of each schedd.
                -- `htcondor_es.utils.get_schedds_from_file` describes the schedd format.
            - All task signatures of all types of all schedds are given to `group` primitive to run in parallel.
            - `query_schedd` task is the initial task, it calls `process_docs` and `post_ads_es` with indirect calls.
                -- Indirect calls means, for example, it runs `send_data` but `send_data` calls `process_docs` task.
            - `propagate=False` important. Because, if it is not given, default value is True which raise exception
                if any schedd query fails. This means that the rest of the schedds in queue will also be terminated.
        References:
        [1]: https://docs.celeryproject.org/en/stable/userguide/canvas.html#the-primitives

    Args:
        args (argparse.Namespace): Please see main method argument definitions

    """
    start_time = time.time()
    """int: Used for `start_time` of `query_schedd` task. And used in calculation of task duration.
    Please see src.htcondor_es.celery.tasks.query_schedd method for usage of `start_time`.
    """

    if args.collectors_file:
        schedd_ads = get_schedds_from_file(args, collectors_file=args.collectors_file)
        """list: Htcondor schedds to query. `collectors_file`, which is defined by `COLLECTORS_FILE_LOCATION` in k8s,
        used to get schedds information.

        Please see what `htcondor_es.utils.get_schedds_from_file` method returns.
        """
        del args.collectors_file  # sending a file through postprocessing will cause problems.
    else:
        schedd_ads = get_schedds(args, collectors=args.collectors)
        """list: Htcondor schedds to query. Not used in current k8s deployment because collectors_file is given."""

    #: list: Includes query types: history, queue.
    _types = []
    if not args.skip_history:
        _types.append(__TYPE_HISTORY)
    if not args.skip_queues:
        _types.append(__TYPE_QUEUE)

    #: celery.result.AsyncResult: Async method to update affiliations. `get()` waits for the results.
    aff_res = create_affiliation_dir.si().apply_async()
    aff_res.get()

    #: celery.result.GroupResult: Async method for group call for query_schedd task
    res = group(
        query_schedd.si(
            sched,
            dry_run=args.dry_run,
            start_time=start_time,
            keep_full_queue_data=args.keep_full_queue_data,
            chunk_size=args.query_queue_batch_size,
            bunch=args.amq_bunch_size,
            query_type=_type,
            es_index_template=args.es_index_template,
            feed_es=args.feed_es and _type is __TYPE_HISTORY,
        )
        for _type in _types
        for sched in schedd_ads
    ).apply_async()

    # - Use the get to wait for results. We could also chain it to a chord to process the responses
    # for logging purposes.
    #
    # The propagate false will prevent it to raise an exception if any of the schedds query failed.
    #: list(tuple): results of `query_schedd` method, i.e [('vocmsXXXX.xxx.xx', 6), ...]
    _query_res = res.get(propagate=False)
    print("Get schedds query result:", _query_res)
    if res.failed():
        print("At least one of the schedd queries failed")
    duration = time.time() - start_time
    print("Duration of whole process: {} seconds".format(round(duration, 2)))
    if duration > 60*10:  # if duration is greater than 10 minutes
        print("Duration exceeded 10 minutes!")
    if duration > 60*12:  # if duration is greater than 12 minutes
        print("ATTENTION: Duration exceeded 12 minutes!")


def main():
    """Main method for the spider_cms script. Parses arguments and invokes main_driver."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--schedd_filter",
        default="",
        type=str,
        dest="schedd_filter",
        help=(
            "Comma separated list of schedd names to process "
            "[default is to process all]"
        ),
    )
    parser.add_argument(
        "--skip_history",
        action="store_true",
        dest="skip_history",
        help="Skip processing the history. (Only do queues.)",
    )
    parser.add_argument(
        "--skip_queues",
        action="store_true",
        dest="skip_queues",
        help="Skip processing the queues. (Only do history.)",
    )
    parser.add_argument(
        "--read_only",
        action="store_true",
        dest="read_only",
        help="Only read the info, don't inject it.",
    )
    parser.add_argument(
        "--dry_run",
        action="store_true",
        dest="dry_run",
        help=(
            "Don't even read info, just pretend to. (Still "
            "query the collector for the schedd's though.)"
        ),
    )
    parser.add_argument(
        "--max_documents_to_process",
        default=0,
        type=int,
        dest="max_documents_to_process",
        help=(
            "Abort after this many documents (per schedd). "
            "[default: %(default)d (process all)]"
        ),
    )
    parser.add_argument(
        "--keep_full_queue_data",
        action="store_true",
        dest="keep_full_queue_data",
        help="Drop all but some fields for running jobs. See `running_fields` in convert_to_json",
    )
    parser.add_argument(
        "--amq_bunch_size",
        default=5000,
        type=int,
        dest="amq_bunch_size",
        help=("Send docs to AMQ in bunches of this number "
              "[default: %(default)d]"),
    )

    parser.add_argument(
        "--query_queue_batch_size",
        default=500,
        type=int,
        dest="query_queue_batch_size",
        help=(
            "Send docs to listener in batches of this number "
            "[default: %(default)d]"
        ),
    )
    parser.add_argument(
        "--collectors",
        default=[],
        action="append",
        dest="collectors",
        help="Collectors' addresses",
    )
    parser.add_argument(
        "--collectors_file",
        default=os.getenv("COLLECTORS_FILE_LOCATION", None),
        action="store",
        type=str,
        dest="collectors_file",
        help=(
            "File defining the pools and collectors"
            "`COLLECTORS_FILE_LOCATION` should be provided as environment variable"
            ),
    )
    parser.add_argument(
        "--es_index_template",
        default="cms-test-k8s",
        type=str,
        dest="es_index_template",
        help=(
            "Trunk of index pattern. "
            "Needs to start with 'cms' "
            "[default: %(default)s]"
        ),
    )
    parser.add_argument(
        "--feed_es",
        action="store_true",
        dest="feed_es",
        help="Send data also to the es-cms ES instance",
    )
    args = parser.parse_args()

    # --dry_run implies read_only
    args.read_only = args.read_only or args.dry_run

    main_driver(args)


if __name__ == "__main__":
    main()
