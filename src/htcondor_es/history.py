"""
Methods for processing the history in a schedd queue.
"""

import datetime
import json
import logging
import multiprocessing
import os
import time
import traceback

import classad
import elasticsearch
import htcondor

import htcondor_es.amq
import htcondor_es.es
from htcondor_es.convert_to_json import convert_dates_to_millisecs
from htcondor_es.convert_to_json import convert_to_json
from htcondor_es.convert_to_json import unique_doc_id
from htcondor_es.utils import send_email_alert, time_remaining, TIMEOUT_MINS

# Main query time, should be same with cron schedule.
QUERY_TIME_PERIOD = 720  # 12 minutes

# Even in checkpoint.json last query time is older than this, older than "now()-12h" results will be ignored.
CRAB_MAX_QUERY_TIME_SPAN = 12 * 3600  # 12 hours

# If last query time in checkpoint.json is too old, but not from crab, results older
# than "now()-RETENTION_POLICY" will be ignored.
RETENTION_POLICY = 39 * 24 * 3600  # 39 days

_WORKDIR = os.getenv("SPIDER_WORKDIR", "/home/cmsjobmon/cms-htcondor-es")
_CHECKPOINT_JSON = os.path.join(_WORKDIR, "checkpoint.json")


def process_schedd(
    starttime, last_completion, checkpoint_queue, schedd_ad, args, metadata=None
):
    """
    Given a schedd, process its entire set of history since last checkpoint.
    """
    my_start = time.time()
    pool_name = schedd_ad.get("CMS_Pool", "Unknown")
    if time_remaining(starttime) < 10:
        message = (
            "No time remaining to process %s history; exiting." % schedd_ad["Name"]
        )
        logging.error(message)
        send_email_alert(
            args.email_alerts, "spider_cms history timeout warning", message
        )
        return last_completion

    metadata = metadata or {}
    schedd = htcondor.Schedd(schedd_ad)
    _q = """
        (JobUniverse == 5) && (CMS_Type != "DONOTMONIT")
        &&
        (
            EnteredCurrentStatus >= %(last_completion)d
            || CRAB_PostJobLastUpdate >= %(last_completion)d
        )
        """
    history_query = classad.ExprTree(_q % {"last_completion": last_completion - QUERY_TIME_PERIOD})
    logging.info(
        "Querying %s for history: %s.  " "%.1f minutes of ads",
        schedd_ad["Name"],
        history_query,
        (time.time() - last_completion) / 60.0,
    )
    buffered_ads = {}
    count = 0
    total_upload = 0
    sent_warnings = False
    timed_out = False
    error = False
    try:
        if not args.dry_run:
            history_iter = schedd.history(history_query, [], match=-1)
        else:
            history_iter = []

        for job_ad in history_iter:
            dict_ad = None
            try:
                dict_ad = convert_to_json(job_ad, return_dict=True, pool_name=pool_name)
            except Exception as e:
                message = "Failure when converting document on %s history: %s" % (
                    schedd_ad["Name"],
                    str(e),
                )
                exc = traceback.format_exc()
                message += "\n{}".format(exc)
                logging.warning(message)
                if not sent_warnings:
                    send_email_alert(
                        args.email_alerts,
                        "spider_cms history document conversion error",
                        message,
                    )
                    sent_warnings = True

            if not dict_ad:
                continue

            idx = htcondor_es.es.get_index(
                timestamp=job_ad["QDate"],
                template=args.es_index_template,
                args=args,
                update_es=(args.feed_es and not args.read_only),
            )
            # Initialize in for loop. setdefault returns the value of the key always
            ad_list = buffered_ads.setdefault(idx, [])
            # ad_list keeps the dict_ad values and is propagated in each iteration until buffered_ads[idx] set empty
            ad_list.append((unique_doc_id(dict_ad), dict_ad))

            if len(ad_list) == args.es_bunch_size:
                st = time.time()
                if not args.read_only:
                    if args.feed_es:
                        htcondor_es.es.post_ads(args=args, idx=idx, ads=ad_list, metadata=metadata)
                    if args.feed_amq:
                        data_for_amq = [
                            (id_, convert_dates_to_millisecs(dict_ad))
                            for id_, dict_ad in ad_list
                        ]
                        htcondor_es.amq.post_ads(data_for_amq, metadata=metadata)

                logging.debug(
                    "...posting %d ads from %s (process_schedd)",
                    len(ad_list),
                    schedd_ad["Name"],
                )
                total_upload += time.time() - st
                # Clear the buffer dict after batch post operation and set buffered_ads[idx] as empty list
                buffered_ads[idx] = []

            count += 1

            # Find the most recent job and use that date as the new
            # last_completion date
            job_completion = job_ad.get("EnteredCurrentStatus")
            if job_completion > last_completion:
                last_completion = job_completion

            if time_remaining(starttime) < 10:
                message = (
                    "History crawler on %s has been running for "
                    "more than %d minutes; exiting." % (schedd_ad["Name"], TIMEOUT_MINS)
                )
                logging.error(message)
                send_email_alert(
                    args.email_alerts, "spider_cms history timeout warning", message
                )
                timed_out = True
                break

            if args.max_documents_to_process and count > args.max_documents_to_process:
                logging.warning(
                    "Aborting after %d documents (--max_documents_to_process option)"
                    % args.max_documents_to_process
                )
                break
        # Post the remaining ads
        for idx, ad_list in list(buffered_ads.items()):
            if ad_list:
                logging.debug(
                    "...posting remaining %d ads from %s " "(process_schedd)",
                    len(ad_list),
                    schedd_ad["Name"],
                )
                if not args.read_only:
                    if args.feed_es:
                        htcondor_es.es.post_ads(args=args, idx=idx, ads=ad_list, metadata=metadata)
                    if args.feed_amq:
                        data_for_amq = [
                            (id_, convert_dates_to_millisecs(dict_ad))
                            for id_, dict_ad in ad_list
                        ]
                        htcondor_es.amq.post_ads(data_for_amq, metadata=metadata)
    except RuntimeError:
        message = "Failed to query schedd for job history: %s" % schedd_ad["Name"]
        exc = traceback.format_exc()
        message += "\n{}".format(exc)
        logging.error(message)
        error = True

    except Exception as exn:
        message = "Failure when processing schedd history query on %s: %s" % (
            schedd_ad["Name"],
            str(exn),
        )
        exc = traceback.format_exc()
        message += "\n{}".format(exc)
        logging.exception(message)
        send_email_alert(
            args.email_alerts, "spider_cms schedd history query error", message
        )
        error = True

    total_time = (time.time() - my_start) / 60.0
    total_upload /= 60.0
    last_formatted = datetime.datetime.fromtimestamp(last_completion).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    logging.warning(
        "Schedd %-25s history: response count: %5d; last completion %s; "
        "query time %.2f min; upload time %.2f min",
        schedd_ad["Name"],
        count,
        last_formatted,
        total_time - total_upload,
        total_upload,
    )

    # If we got to this point without a timeout, all these jobs have
    # been processed and uploaded, so we can update the checkpoint
    if not timed_out and not error:
        checkpoint_queue.put((schedd_ad["Name"], last_completion))

    return last_completion


def update_checkpoint(name, completion_date):
    try:
        with open(_CHECKPOINT_JSON, "r") as fd:
            checkpoint = json.load(fd)
    except Exception as e:
        logging.warning("!!! checkpoint.json is not found or not readable. "
                        "It will be created and fresh results will be written. " + str(e))
        checkpoint = {}

    checkpoint[name] = completion_date

    with open(_CHECKPOINT_JSON, "w") as fd:
        json.dump(checkpoint, fd)


def process_histories(schedd_ads, starttime, pool, args, metadata=None):
    """
    Process history files for each schedd listed in a given
    multiprocessing pool
    """
    try:
        checkpoint = json.load(open(_CHECKPOINT_JSON))
    except Exception as e:
        # Exception should be general
        logging.warning("!!! checkpoint.json is not found or not readable. Empty dict will be used. " + str(e))
        checkpoint = {}

    futures = []
    metadata = metadata or {}
    metadata["spider_source"] = "condor_history"

    manager = multiprocessing.Manager()
    checkpoint_queue = manager.Queue()

    for schedd_ad in schedd_ads:
        name = schedd_ad["Name"]

        # Check for last completion time
        # If there was no previous completion, get last 12 h
        history_query_max_n_minutes = args.history_query_max_n_minutes  # Default 12 * 60
        last_completion = checkpoint.get(name, time.time() - history_query_max_n_minutes * 60)
        last_completion = max(last_completion, time.time() - RETENTION_POLICY)

        # For CRAB, only ever get a maximum of 12 h
        if name.startswith("crab") and last_completion < time.time() - CRAB_MAX_QUERY_TIME_SPAN:
            last_completion = time.time() - history_query_max_n_minutes * 60

        future = pool.apply_async(
            process_schedd,
            (starttime, last_completion, checkpoint_queue, schedd_ad, args, metadata),
        )
        futures.append((name, future))

    def _chkp_updater():
        while True:
            try:
                job = checkpoint_queue.get()
                if job is None:  # Swallow poison pill
                    break
            except EOFError as error:
                logging.warning(
                    "EOFError - Nothing to consume left in the queue %s", error
                )
                break
            update_checkpoint(*job)

    chkp_updater = multiprocessing.Process(target=_chkp_updater)
    chkp_updater.start()

    # Check whether one of the processes timed out and reset their last
    # completion checkpoint in case
    timed_out = False
    for name, future in futures:
        if time_remaining(starttime) > -10:
            try:
                future.get(time_remaining(starttime) + 10)
            except multiprocessing.TimeoutError:
                # This implies that the checkpoint hasn't been updated
                message = "Schedd %s history timed out; ignoring progress." % name
                exc = traceback.format_exc()
                message += "\n{}".format(exc)
                logging.error(message)
                send_email_alert(
                    args.email_alerts, "spider_cms history timeout warning", message
                )
            except elasticsearch.exceptions.TransportError:
                message = (
                    "Transport error while sending history data of %s; ignoring progress."
                    % name
                )
                exc = traceback.format_exc()
                message += "\n{}".format(exc)
                logging.error(message)
                send_email_alert(
                    args.email_alerts,
                    "spider_cms history transport error warning",
                    message,
                )
        else:
            timed_out = True
            break
    if timed_out:
        pool.terminate()

    checkpoint_queue.put(None)  # Send a poison pill
    chkp_updater.join()

    logging.warning(
        "Processing time for history: %.2f mins", ((time.time() - starttime) / 60.0)
    )
