"""
Methods for processing the history in a schedd queue.

Created for backup script "history_backup_spider_cms.py". It will not use checkpoint.json file but backup_start and backup_end parameters.
"""

import json
import time
import logging
import datetime
import traceback
import multiprocessing

import classad
import htcondor
import elasticsearch

import htcondor_es.es
import htcondor_es.amq
from htcondor_es.utils import send_email_alert, time_remaining, TIMEOUT_MINS
from htcondor_es.convert_to_json import convert_to_json
from htcondor_es.convert_to_json import convert_dates_to_millisecs
from htcondor_es.convert_to_json import unique_doc_id


def unix_ts_to_str(ts):
    """Convert UTC timestamp in seconds to date string"""
    return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def process_schedd(
    starttime, last_completion, checkpoint_queue, schedd_ad, args, metadata=None
):
    """
    Given a schedd, process its entire set of history since last checkpoint.
    """
    my_start = time.time()
    pool_name = schedd_ad.get("CMS_Pool", "Unknown")
    if time_remaining(starttime) < 0:
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
        ( 
          ( EnteredCurrentStatus >= %(backup_start)d && EnteredCurrentStatus < %(backup_end)d ) 
          || 
          ( CRAB_PostJobLastUpdate >= %(backup_start)d && CRAB_PostJobLastUpdate < %(backup_end)d ) 
        )
        && (CMS_Type != "DONOTMONIT")
        """
    history_query = classad.ExprTree(_q % {"backup_start": args.backup_start, "backup_end": args.backup_end})
    logging.info(
        "Querying %s for history: %s.  " " between %s ",
        schedd_ad["Name"],
        history_query,
        unix_ts_to_str(args.backup_start) + " - " + unix_ts_to_str(args.backup_end),
    )
    buffered_ads = {}
    count = 0
    total_upload = 0
    sent_warnings = False
    timed_out = False
    error = False
    if not args.read_only:
        if args.feed_es:
            es = htcondor_es.es.get_server_handle(args)
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
                job_ad["QDate"],
                template=args.es_index_template,
                update_es=(args.feed_es and not args.read_only),
            )
            ad_list = buffered_ads.setdefault(idx, [])
            ad_list.append((unique_doc_id(dict_ad), dict_ad))

            if len(ad_list) == args.es_bunch_size:
                st = time.time()
                if not args.read_only:
                    if args.feed_es:
                        htcondor_es.es.post_ads(
                            es.handle, idx, ad_list, metadata=metadata
                        )
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
                buffered_ads[idx] = []

            count += 1

            # Find the most recent job and use that date as the new
            # last_completion date
            job_completion = job_ad.get("EnteredCurrentStatus")
            if job_completion > last_completion:
                last_completion = job_completion

            if time_remaining(starttime) < 0:
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
                        htcondor_es.es.post_ads(
                            es.handle, idx, ad_list, metadata=metadata
                        )
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
        with open("checkpoint_test.json", "r") as fd:
            checkpoint = json.load(fd)
    except Exception as e:
        logging.warning("ERROR - checkpoint_test.json is not readable as json. " + str(e))
        checkpoint = {}

    checkpoint[name] = completion_date

    with open("checkpoint_test.json", "w") as fd:
        json.dump(checkpoint, fd)


def process_histories(schedd_ads, starttime, pool, args, metadata=None):
    """
    Process history files for each schedd listed in a given
    multiprocessing pool
    """
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
        last_completion = args.backup_start


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
