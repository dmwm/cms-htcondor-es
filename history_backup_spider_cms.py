#!/usr/bin/env python
"""
Script to fill the gap for any failure of history results of schedds.

How to run:
   - Please create new directory and copy etc/es.conf, password, username from /home/cmsjobmon/cms-htcondor-es directory from vocm0240
   - Please activate venv in /home/cmsjobmon/cms-htcondor-es, it includes required htcondor/classad versions
   - Please test your script in read_only mode, it will just print converted classAd results:
    > python history_backup_spider_cms.py --read_only --backup_start 1662268653 --backup_end 1662322692 --log_dir /$TEST_DIR/cms-htcondor-es/log_history/ --log_level INFO  --collectors_file /home/cmsjobmon/cms-htcondor-es/etc/collectors.json
   - Check only ES with test index pattern, in below example it will send data to "cms-test-backup" index in es-cms.cern.ch:
    > python history_backup_spider_cms.py --feed_es --es_index_template "cms-test-backup" --backup_start 1662268653 --backup_end 1662269653 --log_dir /$TEST_DIR/cms-htcondor-es/log_history/ --log_level INFO  --collectors_file /home/cmsjobmon/cms-htcondor-es/etc/collectors.json --es_hostname "es-cms.cern.ch"
   - If everything fine, please find the gap time period in UTC unix epoch seconds format and give "--backup_start" and "--backup_end" parameters:
    > python history_backup_spider_cms.py --backup_start 1662268653 --backup_end 1662322692  --feed_amq --feed_es --es_bunch_size 2000 --email_alerts 'cms-comp-monit-alerts@cern.ch' --log_dir /$TEST_DIR/cms-htcondor-es/log_history/ --log_level WARNING  --collectors_file /home/cmsjobmon/cms-htcondor-es/etc/collectors.json  --es_hostname "es-cms.cern.ch"

   - It will use checkpoint_test.json just to fill last checkpoints, it will not use it.
   - [ATTENTION] to be able to get affiliations from "/home/cmsjobmon/.affiliation_dir.json" file, you need to run this script in vocms0240 and with cmsjobmon user.
       You don't need to give this file name, because it's read from "AffiliationManager._AffiliationManager__DEFAULT_DIR_PATH" in convert_to_json.py

"""

import os
import sys
import time
import signal
import logging
import argparse
import multiprocessing

try:
    import htcondor_es
except ImportError:
    if os.path.exists("src/htcondor_es/__init__.py") and "src" not in sys.path:
        sys.path.append("src")

import htcondor_es.history_backup
from htcondor_es.utils import (
    get_schedds,
    get_schedds_from_file,
    set_up_logging,
    send_email_alert,
)
from htcondor_es.utils import collect_metadata, TIMEOUT_MINS


def main_driver(args):
    """
    Driver method for the spider script.
    """
    starttime = time.time()

    signal.alarm(TIMEOUT_MINS * 60 + 60)

    # Get all the schedd ads
    schedd_ads = []
    if args.collectors_file:
        schedd_ads = get_schedds_from_file(args, collectors_file=args.collectors_file)
        del (
            args.collectors_file
        )  # sending a file through postprocessing will cause problems.
    else:
        schedd_ads = get_schedds(args, collectors=args.collectors)
    logging.warning("&&& There are %d schedds to query.", len(schedd_ads))

    pool = multiprocessing.Pool(processes=args.query_pool_size)

    metadata = collect_metadata()

    if not args.skip_history:
        htcondor_es.history_backup.process_histories(
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
        "--process_queue",
        action="store_true",
        dest="process_queue",
        help="Process also schedd queue (Running/Idle/Pending jobs)",
    )
    parser.add_argument(
        "--feed_es", action="store_true", dest="feed_es", help="Feed to Elasticsearch"
    )
    parser.add_argument(
        "--feed_es_for_queues",
        action="store_true",
        dest="feed_es_for_queues",
        help="Feed queue data also to Elasticsearch",
    )
    parser.add_argument(
        "--feed_amq", action="store_true", dest="feed_amq", help="Feed to CERN AMQ"
    )

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
        "--es_bunch_size",
        default=250,
        type=int,
        dest="es_bunch_size",
        help=("Send docs to ES in bunches of this number " "[default: %(default)d]"),
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
        "--upload_pool_size",
        default=8,
        type=int,
        dest="upload_pool_size",
        help=("Number of parallel processes for uploading " "[default: %(default)d]"),
    )
    parser.add_argument(
        "--query_pool_size",
        default=8,
        type=int,
        dest="query_pool_size",
        help=("Number of parallel processes for querying " "[default: %(default)d]"),
    )

    parser.add_argument(
        "--es_hostname",
        default="es-cms.cern.ch",
        type=str,
        dest="es_hostname",
        help="Hostname of the elasticsearch instance to be used "
        "[default: %(default)s]",
    )
    parser.add_argument(
        "--es_port",
        default=9203,
        type=int,
        dest="es_port",
        help="Port of the elasticsearch instance to be used " "[default: %(default)d]",
    )
    parser.add_argument(
        "--es_index_template",
        default="cms",
        type=str,
        dest="es_index_template",
        help=(
            "Trunk of index pattern. "
            "Needs to start with 'cms' "
            "[default: %(default)s]"
        ),
    )
    parser.add_argument(
        "--log_dir",
        default="log/",
        type=str,
        dest="log_dir",
        help="Directory for logging information [default: %(default)s]",
    )
    parser.add_argument(
        "--log_level",
        default="WARNING",
        type=str,
        dest="log_level",
        help="Log level (CRITICAL/ERROR/WARNING/INFO/DEBUG) " "[default: %(default)s]",
    )
    parser.add_argument(
        "--email_alerts",
        default=[],
        action="append",
        dest="email_alerts",
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
        "--collectors_file",
        default=None,
        action="store",
        type=argparse.FileType("r"),
        dest="collectors_file",
        help="FIle defining the pools and collectors",
    )
    parser.add_argument(
        "--backup_start",
        default=None,
        action="store",
        type=int,
        dest="backup_start",
        help="Unix epoch timestamp, UTC tz in seconds",
    )
    parser.add_argument(
        "--backup_end",
        default=None,
        action="store",
        type=int,
        dest="backup_end",
        help="Unix epoch timestamp, UTC tz in seconds",
    )
    args = parser.parse_args()
    set_up_logging(args)

    # --dry_run implies read_only
    args.read_only = args.read_only or args.dry_run

    main_driver(args)


if __name__ == "__main__":
    main()
