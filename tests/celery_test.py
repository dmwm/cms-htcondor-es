#!/usr/bin/env python
# coding=utf8
import os
import argparse
import time
import traceback
from celery import group
from htcondor_es.celery.tasks import query_schedd
from htcondor_es.utils import get_schedds, get_schedds_from_file


def main_driver(args):
    os.environ["CMS_HTCONDOR_BROKER"] = "cms-test-mb.cern.ch"
    os.environ["CMS_HTCONDOR_PRODUCER"] = "condor-test"
    schedd_ads = []
    start_time = time.time()
    if args.collectors_file:
        schedd_ads = get_schedds_from_file(args, collectors_file=args.collectors_file)
        del (
            args.collectors_file
        )  # sending a file through postprocessing will cause problems.
    else:
        schedd_ads = get_schedds(args, collectors=args.collectors)

    res = group(
        query_schedd.s(
            sched,
            dry_run=args.dry_run,
            start_time=start_time,
            keep_full_queue_data=args.keep_full_queue_data,
            bunch=args.amq_bunch_size,
        )
        for sched in schedd_ads
    ).apply_async()
    groups = res.get()
    print([g.collect() for g in groups])


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
        default=50,
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
    args = parser.parse_args()

    # --dry_run implies read_only
    args.read_only = args.read_only or args.dry_run

    main_driver(args)


if __name__ == "__main__":
    main()
