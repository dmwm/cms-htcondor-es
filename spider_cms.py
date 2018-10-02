#!/usr/bin/python
"""
Script for processing the contents of the CMS pool.
"""

import os
import sys
import time
import random
import signal
import logging
import argparse
import multiprocessing

import classad
import htcondor

try:
    import htcondor_es
except ImportError:
    if os.path.exists("src/htcondor_es/__init__.py") and "src" not in sys.path:
        sys.path.append("src")

import htcondor_es.history
import htcondor_es.queues
from htcondor_es.utils import set_up_logging, TIMEOUT_MINS


signal.alarm(TIMEOUT_MINS*60 + 60)


def get_schedds(args=None):
    """
    Return a list of schedd ads representing all the schedds in the pool.
    """
    schedd_query = classad.ExprTree('!isUndefined(CMSGWMS_Type)')
    collectors = ["cmssrv221.fnal.gov:9620",
                  "cmsgwms-collector-tier0.cern.ch:9620",
                  "cmssrv276.fnal.gov"]

    schedd_ads = {}
    for host in collectors:
        coll = htcondor.Collector(host)
        schedds = coll.query(htcondor.AdTypes.Schedd,
                             schedd_query,
                             projection=["MyAddress", "ScheddIpAddr", "Name"])

        for schedd in schedds:
            try:
                schedd_ads[schedd['Name']] = schedd
            except KeyError:
                pass

    schedd_ads = schedd_ads.values()
    random.shuffle(schedd_ads)

    if args and args.schedd_filter:
        return [s for s in schedd_ads if s['Name'] in args.schedd_filter.split(',')]

    return schedd_ads


def main_driver(args):
    """
    Driver method for the spider script.
    """
    starttime = time.time()

    # Get all the schedd ads
    schedd_ads = get_schedds(args)
    logging.warning("&&& There are %d schedds to query.", len(schedd_ads))

    pool = multiprocessing.Pool(processes=args.query_pool_size)

    if not args.skip_history:
        htcondor_es.history.process_histories(schedd_ads=schedd_ads,
                                              starttime=starttime,
                                              pool=pool,
                                              args=args)

    # Now that we have the fresh history, process the queues themselves.
    if args.process_queue:
        htcondor_es.queues.process_queues(schedd_ads=schedd_ads,
                                          starttime=starttime,
                                          pool=pool,
                                          args=args)

    pool.close()
    pool.join()

    logging.warning("@@@ Total processing time: %.2f mins", ((time.time()-starttime)/60.))

    return 0

def main():
    """
    Main method for the spider_cms script.

    Parses arguments and invokes main_driver
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--process_queue", action='store_true',
                        dest="process_queue",
                        help="Process also schedd queue (Running/Idle/Pending jobs)")
    parser.add_argument("--feed_es", action='store_true',
                        dest="feed_es",
                        help="Feed to Elasticsearch")
    parser.add_argument("--feed_es_for_queues", action='store_true',
                        dest="feed_es_for_queues",
                        help="Feed queue data also to Elasticsearch")
    parser.add_argument("--feed_amq", action='store_true',
                        dest="feed_amq",
                        help="Feed to CERN AMQ")

    parser.add_argument("--schedd_filter", default='',
                        type=str, dest="schedd_filter",
                        help=("Comma separated list of schedd names to process "
                              "[default is to process all]"))
    parser.add_argument("--skip_history", action='store_true',
                        dest="skip_history",
                        help="Skip processing the history. (Only do queues.)")
    parser.add_argument("--read_only", action='store_true',
                        dest="read_only",
                        help="Only read the info, don't submit it.")
    parser.add_argument("--dry_run", action='store_true',
                        dest="dry_run",
                        help=("Don't even read info, just pretend to. (Still "
                              "query the collector for the schedd's though.)"))
    parser.add_argument("--keep_full_queue_data", action='store_true',
                        dest="keep_full_queue_data",
                        help="Drop all but some fields for running jobs.")
    parser.add_argument("--bunching", default=250,
                        type=int, dest="bunching",
                        help=("Send docs in bunches of this number "
                              "[default: %(default)d]"))
    parser.add_argument("--query_queue_batch_size", default=50,
                        type=int, dest="query_queue_batch_size",
                        help=("Send docs to listener in batches of this number "
                              "[default: %(default)d]"))
    parser.add_argument("--upload_pool_size", default=8,
                        type=int, dest="upload_pool_size",
                        help=("Number of parallel processes for uploading "
                              "[default: %(default)d]"))
    parser.add_argument("--query_pool_size", default=8,
                        type=int, dest="query_pool_size",
                        help=("Number of parallel processes for querying "
                              "[default: %(default)d]"))

    parser.add_argument("--es_hostname", default='es-cms.cern.ch',
                        type=str, dest="es_hostname",
                        help="Hostname of the elasticsearch instance to be used "
                             "[default: %(default)s]")
    parser.add_argument("--es_port", default=9203,
                        type=int, dest="es_port",
                        help="Port of the elasticsearch instance to be used "
                             "[default: %(default)d]")
    parser.add_argument("--es_index_template", default='cms',
                        type=str, dest="es_index_template",
                        help=("Trunk of index pattern. "
                              "Needs to start with 'cms' "
                              "[default: %(default)s]"))
    parser.add_argument("--log_dir", default='log/',
                        type=str, dest="log_dir",
                        help="Directory for logging information [default: %(default)s]")
    parser.add_argument("--log_level", default='WARNING',
                        type=str, dest="log_level",
                        help="Log level (CRITICAL/ERROR/WARNING/INFO/DEBUG) "
                             "[default: %(default)s]")
    parser.add_argument("--email_alerts", default=[], action='append',
                        dest="email_alerts",
                        help="Email addresses for alerts [default: none]")

    args = parser.parse_args()
    set_up_logging(args)

    # --dry_run implies read_only
    args.read_only = args.read_only or args.dry_run

    main_driver(args)


if __name__ == "__main__":
    main()
