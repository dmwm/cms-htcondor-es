# -*- coding: utf-8 -*-
# Author: Christian Ariza <christian.ariza AT gmail [DOT] com>
"""
Celery version of the cms htcondor_es spider. 
This version has some major changes:
    - Celery based
    - The same function is used for either the queues and 
       history queries. 
    - The history checkpoint for the schedds is stored in Redis instead of a json file.
    - Parallelism is managed by celery
"""
import os
import logging
import argparse
import time
import traceback
from celery import group
from htcondor_es.celery.tasks import query_schedd, create_affiliation_dir
from htcondor_es.celery.celery import app
from htcondor_es.utils import get_schedds, get_schedds_from_file

logging.basicConfig(level=os.environ.get("LOGLEVEL", "ERROR"))
__TYPE_HISTORY = "history"
__TYPE_QUEUE = "queue"


def main_driver(args):
    logging.debug(app.conf.humanize(with_defaults=False))
    schedd_ads = []
    start_time = time.time()
    if args.collectors_file:
        schedd_ads = get_schedds_from_file(args, collectors_file=args.collectors_file)
        del (
            args.collectors_file
        )  # sending a file through postprocessing will cause problems.
    else:
        schedd_ads = get_schedds(args, collectors=args.collectors)
    _types = []
    if not args.skip_history:
        _types.append(__TYPE_HISTORY)
    if not args.skip_queues:
        _types.append(__TYPE_QUEUE)
    aff_res = create_affiliation_dir.si().apply_async()
    aff_res.get()
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
    # Use the get to wait for results
    # We could also chain it to a chord to process the responses
    # for logging pourposes.
    # The propagate false will prevent it to raise
    # an exception if any of the schedds query failed.
    groups = res.get(propagate=False)
    logging.debug(groups)
    print(time.time() - start_time)


def main():
    """
    Main method for the spider_cms script.
    Parses arguments and invokes main_driver
    """
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
        help="Only read the info, don't submit it.",
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
        help="Drop all but some fields for running jobs.",
    )
    parser.add_argument(
        "--amq_bunch_size",
        default=5000,
        type=int,
        dest="amq_bunch_size",
        help=("Send docs to AMQ in bunches of this number " "[default: %(default)d]"),
    )

    parser.add_argument(
        "--query_queue_batch_size",
        default=500,
        type=int,
        dest="query_queue_batch_size",
        help=(
            "Send docs to listener in batches of this number " "[default: %(default)d]"
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
        type=argparse.FileType("r"),
        dest="collectors_file",
        help="FIle defining the pools and collectors",
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
