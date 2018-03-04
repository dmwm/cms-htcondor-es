"""
Methods for processing the history in a schedd queue.
"""

import os
import json
import time
import logging
import datetime
import tempfile
import multiprocessing

import classad
import htcondor

import htcondor_es.es
import htcondor_es.amq
from htcondor_es.utils import send_email_alert, time_remaining, TIMEOUT_MINS
from htcondor_es.convert_to_json import convert_to_json
from htcondor_es.convert_to_json import convert_dates_to_millisecs


def process_schedd(starttime, last_completion, schedd_ad, args):
    """
    Given a schedd, process its entire set of history since last checkpoint.
    """
    my_start = time.time()
    if time_remaining(starttime) < 0:
        message = ("No time remaining to process %s history; exiting." %
                   schedd_ad['Name'])
        logging.error(message)
        send_email_alert(args.email_alerts,
                         "spider_cms history timeout warning",
                         message)
        return last_completion

    schedd = htcondor.Schedd(schedd_ad)
    history_query = classad.ExprTree("EnteredCurrentStatus >= %d" % last_completion)
    logging.info("Querying %s for history: %s.  "
                 "%.1f minutes of ads", schedd_ad["Name"],
                 history_query,
                 (time.time()-last_completion)/60.)
    buffered_ads = {}
    count = 0
    total_upload = 0
    sent_warnings = False
    if not args.read_only:
        if args.feed_es:
            es = htcondor_es.es.get_server_handle(args)
    try:
        if not args.dry_run:
            history_iter = schedd.history(history_query, [], 10000)
        else:
            history_iter = []

        for job_ad in history_iter:
            dict_ad = None
            try:
                dict_ad = convert_to_json(job_ad, return_dict=True)
            except Exception as e:
                message = ("Failure when converting document on %s history: %s" %
                           (schedd_ad["Name"], str(e)))
                logging.warning(message)
                if not sent_warnings:
                    send_email_alert(args.email_alerts,
                                     "spider_cms history document conversion error",
                                     message)
                    sent_warnings = True

            if not dict_ad:
                continue

            idx = htcondor_es.es.get_index(job_ad["QDate"],
                                           template=args.es_index_template,
                                           update_es=(args.feed_es and not args.read_only))
            ad_list = buffered_ads.setdefault(idx, [])
            ad_list.append((job_ad["GlobalJobId"], dict_ad))

            if len(ad_list) == args.bunching:
                st = time.time()
                if not args.read_only:
                    if args.feed_es:
                        data_for_es = [(id_, json.dumps(dict_ad)) for id_, dict_ad in ad_list]
                        htcondor_es.es.post_ads(es=es.handle, idx=idx, ads=data_for_es)
                    if args.feed_amq:
                        data_for_amq = [(id_, convert_dates_to_millisecs(dict_ad)) for \
                                        id_, dict_ad in ad_list]
                        htcondor_es.amq.post_ads(data_for_amq)
                logging.debug("...posting %d ads from %s (process_schedd)", len(ad_list),
                              schedd_ad["Name"])
                total_upload += time.time() - st
                buffered_ads[idx] = []

            count += 1
            job_completion = job_ad.get("EnteredCurrentStatus")
            if job_completion > last_completion:
                last_completion = job_completion
            if time_remaining(starttime) < 0:
                message = ("History crawler on %s has been running for "
                           "more than %d minutes; exiting." % (schedd_ad["Name"], TIMEOUT_MINS))
                logging.error(message)
                send_email_alert(args.email_alerts,
                                 "spider_cms history timeout warning",
                                 message)
                break


    except RuntimeError:
        logging.error("Failed to query schedd for job history: %s", schedd_ad["Name"])

    except Exception as exn:
        message = ("Failure when processing schedd history query on %s: %s" %
                   (schedd_ad["Name"], str(exn)))
        logging.exception(message)
        send_email_alert(args.email_alerts, "spider_cms schedd history query error",
                         message)

    # Post the remaining ads
    for idx, ad_list in buffered_ads.items():
        if ad_list:
            logging.debug("...posting remaining %d ads from %s "
                          "(process_schedd)", len(ad_list),
                          schedd_ad["Name"])
            if not args.read_only:
                if args.feed_es:
                    htcondor_es.es.post_ads(es=es.handle, idx=idx,
                                            ads=[(id_, json.dumps(dict_ad)) for
                                                 id_, dict_ad in ad_list])
                if args.feed_amq:
                    data_for_amq = [(id_, convert_dates_to_millisecs(dict_ad)) for \
                                    id_, dict_ad in ad_list]
                    htcondor_es.amq.post_ads(data_for_amq)


    total_time = (time.time() - my_start) / 60.
    total_upload /= 60.
    last_formatted = datetime.datetime.fromtimestamp(last_completion).strftime("%Y-%m-%d %H:%M:%S")
    logging.warning("Schedd %-25s history: response count: %5d; last completion %s; "
                    "query time %.2f min; upload time %.2f min",
                    schedd_ad["Name"],
                    count,
                    last_formatted,
                    total_time - total_upload,
                    total_upload)

    try:
        checkpoint_new = json.load(open("checkpoint.json"))
    except:
        checkpoint_new = {}

    if ((schedd_ad["Name"] not in checkpoint_new) or
            (checkpoint_new[schedd_ad["Name"]] < last_completion)):
        checkpoint_new[schedd_ad["Name"]] = last_completion

    fd, tmpname = tempfile.mkstemp(dir=".", prefix="checkpoint.json.new")
    fd = os.fdopen(fd, "w")
    json.dump(checkpoint_new, fd)
    fd.close()
    os.rename(tmpname, "checkpoint.json")

    return last_completion


def process_histories(schedd_ads, starttime, pool, args):
    """
    Process history files for each schedd listed in a given
    multiprocessing pool
    """
    try:
        checkpoint = json.load(open("checkpoint.json"))
    except:
        checkpoint = {}

    futures = []

    for schedd_ad in schedd_ads:
        name = schedd_ad["Name"]

        # Check for last completion time
        # If there was no previous completion, get last 12 h
        last_completion = checkpoint.get(name, time.time()-12*3600)

        # For CRAB, only ever get a maximum of 12 h
        if name.startswith("crab") and last_completion < time.time()-12*3600:
            last_completion = time.time()-12*3600

        future = pool.apply_async(process_schedd,
                                  (starttime,
                                   last_completion,
                                   schedd_ad,
                                   args))
        futures.append((name, future))

    # Check whether one of the processes timed out and reset their last
    # completion checkpoint in case
    timed_out = False
    for name, future in futures:
        if time_remaining(starttime) > -10:
            try:
                last_completion = future.get(time_remaining(starttime)+10)
                if name:
                    checkpoint[name] = last_completion

            except multiprocessing.TimeoutError:
                message = "Schedd %s history timed out; ignoring progress." % name
                logging.error(message)
                send_email_alert(args.email_alerts,
                                 "spider_cms history timeout warning",
                                 message)

        else:
            timed_out = True
            break
    if timed_out:
        pool.terminate()


    # Update the last completion checkpoint file
    try:
        checkpoint_new = json.load(open("checkpoint.json"))
    except:
        checkpoint_new = {}

    for key, val in checkpoint.items():
        if (key not in checkpoint_new) or (val > checkpoint_new[key]):
            checkpoint_new[key] = val

    fd, tmpname = tempfile.mkstemp(dir=".", prefix="checkpoint.json.new")
    fd = os.fdopen(fd, "w")
    json.dump(checkpoint_new, fd)
    fd.close()
    os.rename(tmpname, "checkpoint.json")

    logging.warning("Processing time for history: %.2f mins",
                    ((time.time()-starttime)/60.))
