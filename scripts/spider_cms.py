#!/usr/bin/env python
"""
Script for processing the contents of the CMS pool.
"""

import os
import sys
import time
import signal
import logging
import argparse
import multiprocessing

import htcondor_es
import htcondor_es.history
import htcondor_es.queues
from htcondor_es.utils import (
    get_schedds,
    get_schedds_from_file,
    set_up_logging,
)
from htcondor_es.utils import collect_metadata, TIMEOUT_MINS


def main_driver(args):
    """
    Driver method for the spider script.
    """
    starttime = time.time()

    signal.alarm(TIMEOUT_MINS * 60 + 60)

    # Get all the schedd ads
    if args.collectors_file:
        schedd_ads = get_schedds_from_file(args, collectors_file=args.collectors_file)
        del args.collectors_file  # sending a file through postprocessing will cause problems.
    else:
        schedd_ads = get_schedds(args, collectors=args.collectors)
    logging.warning("&&& There are %d schedds to query.", len(schedd_ads))

    pool = multiprocessing.Pool(processes=args.query_pool_size)

    metadata = collect_metadata()

    if not args.skip_history:
        htcondor_es.history.process_histories(
            schedd_ads=schedd_ads,
            starttime=starttime,
            pool=pool,
            args=args,
            metadata=metadata,
        )

    # Now that we have the fresh history, process the queues themselves.
    if args.process_queue:
        htcondor_es.queues.process_queues(
            schedd_ads=schedd_ads,
            starttime=starttime,
            pool=pool,
            args=args,
            metadata=metadata,
        )

    pool.close()
    pool.join()

    logging.warning(
        "@@@ Total processing time: %.2f mins", ((time.time() - starttime) / 60.0)
    )

    return 0


def main():
    """
    Main method for the spider_cms script.

    Parses arguments and invokes main_driver
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--process_queue", action="store_true", dest="process_queue",
        help="Process also schedd queue (Running/Idle/Pending jobs)",
    )
    parser.add_argument(
        "--feed_es", action="store_true", dest="feed_es", help="Feed to Elasticsearch"
    )
    parser.add_argument(
        "--feed_es_for_queues", action="store_true", dest="feed_es_for_queues",
        help="Feed queue data also to Elasticsearch",
    )
    parser.add_argument(
        "--feed_amq", action="store_true", dest="feed_amq", help="Feed to CERN AMQ"
    )
    parser.add_argument(
        "--schedd_filter", default="", type=str, dest="schedd_filter",
        help="Comma separated list of schedd names to process [default is to process all]",
    )
    parser.add_argument(
        "--skip_history", action="store_true", dest="skip_history",
        help="Skip processing the history. (Only do queues.)",
    )
    parser.add_argument(
        "--read_only", action="store_true", dest="read_only", help="Only read the info, don't submit it.",
    )
    parser.add_argument(
        "--dry_run", action="store_true", dest="dry_run",
        help="Don't even read info, just pretend to. (Still query the collector for the schedd's though.)",
    )
    parser.add_argument(
        "--max_documents_to_process", default=0, type=int, dest="max_documents_to_process",
        help="Abort after this many documents (per schedd). [default: %(default)d (process all)]",
    )
    parser.add_argument(
        "--keep_full_queue_data", action="store_true", dest="keep_full_queue_data",
        help="Drop all but some fields for running jobs.",
    )
    parser.add_argument(
        "--amq_bunch_size", default=5000, type=int, dest="amq_bunch_size",
        help="Send docs to AMQ in bunches of this number " "[default: %(default)d]",
    )
    parser.add_argument(
        "--es_bunch_size", default=250, type=int, dest="es_bunch_size",
        help="Send docs to ES in bunches of this number " "[default: %(default)d]",
    )
    parser.add_argument(
        "--query_queue_batch_size", default=50, type=int, dest="query_queue_batch_size",
        help="Send docs to listener in batches of this number " "[default: %(default)d]",
    )
    parser.add_argument(
        "--upload_pool_size", default=8, type=int, dest="upload_pool_size",
        help="Number of parallel processes for uploading. SHOULD BE LESS THAN #CPUS or StompAMQ fails! Suggested: #cpus/2 . [default: %(default)d]",
    )
    parser.add_argument(
        "--query_pool_size", default=8, type=int, dest="query_pool_size",
        help="Number of parallel processes for querying. SHOULD BE LESS THAN #CPUS! [default: %(default)d]",
    )
    parser.add_argument(
        "--es_index_template", type=str, dest="es_index_template", default=None,
        help="Trunk of index pattern. Needs to start with 'cms' [default: %(default)s]",
    )
    parser.add_argument(
        "--log_dir", default="log/", type=str, dest="log_dir",
        help="Directory for logging information [default: %(default)s]",
    )
    parser.add_argument(
        "--log_level", default="WARNING", type=str, dest="log_level",
        help="Log level (CRITICAL/ERROR/WARNING/INFO/DEBUG) " "[default: %(default)s]",
    )
    parser.add_argument(
        "--email_alerts", default=[], action="append", dest="email_alerts",
        help="Email addresses for alerts [default: none]",
    )
    parser.add_argument(
        "--collectors",
        default=[
            "cmssrv623.fnal.gov:9620",
            "cmsgwms-collector-tier0.cern.ch:9620",
            "cmssrv276.fnal.gov",
            "cmsgwms-collector-itb.cern.ch",
            "vocms0840.cern.ch",
        ],
        action="append",
        dest="collectors",
        help="Collectors' addresses",
    )
    parser.add_argument(
        "--collectors_file", default=None, action="store", type=argparse.FileType("r"), dest="collectors_file",
        help="FIle defining the pools and collectors",
    )
    parser.add_argument(
        "--history_query_max_n_minutes", default=12 * 60, type=int, dest="history_query_max_n_minutes",
        help="If no checkpoint provided, query max N minutes from now of completed job results in history shcedds. "
             "It gives elasticity to test jobs. [default: %(default)d]",
    )
    parser.add_argument(
        "--mock_cern_domain", action="store_true", dest="mock_cern_domain",
        help="ElasticsearchInterface forces to be in CERN domain. In dev tests, mock that using this var",
    )
    args = parser.parse_args()
    # ============ Checks ==============
    # Check AMQ environment variables
    if (not os.getenv("CMS_HTCONDOR_PRODUCER")) or (not os.getenv("CMS_HTCONDOR_BROKER")):
        logging.error("Please set required environment variables for AMQ")
        sys.exit(1)

    # Check ES params are defined for feed_es(history schedds)
    if args.feed_es:
        _workdir = os.getenv("SPIDER_WORKDIR", "/home/cmsjobmon/cms-htcondor-es")
        _es_creds_file = os.path.join(_workdir, "etc/es_conf.json")
        if (not args.es_index_template) or (not os.path.exists(_es_creds_file)):
            logging.error(
                "Please [set es_index_template param] AND [provide etc/es_conf.json], because feed_es is set.")
            sys.exit(1)

    if args.feed_amq:
        _workdir = os.getenv("SPIDER_WORKDIR", "/home/cmsjobmon/cms-htcondor-es")
        if (not os.path.exists(os.path.join(_workdir, "etc/amq_username"))) or \
            (not os.path.exists(os.path.join(_workdir, "etc/amq_password"))):
            logging.error("Please provide [etc/amq_username] and [etc/amq_password], because feed_amq is set.")
            sys.exit(1)

    set_up_logging(args)

    # --dry_run implies read_only
    args.read_only = args.read_only or args.dry_run

    main_driver(args)


if __name__ == "__main__":
    main()
