#!/usr/bin/python

import os
import sys
import json
import time
import errno
import random
import signal
import socket
import classad
import logging
import htcondor
import datetime
import tempfile
import traceback
import collections
import multiprocessing

from argparse import ArgumentParser
from logging.handlers import RotatingFileHandler

try:
    import htcondor_es
except ImportError:
    if os.path.exists("src/htcondor_es/__init__.py") and "src" not in sys.path:
        sys.path.append("src")

import htcondor_es.es
import htcondor_es.amq
from htcondor_es.utils import send_email_alert, time_remaining, set_up_logging, TIMEOUT_MINS
from htcondor_es.convert_to_json import convert_to_json
from htcondor_es.convert_to_json import convert_dates_to_millisecs
from htcondor_es.convert_to_json import get_data_collection_time


now = time.time()
now_ns = int(time.time())*int(1e9)

signal.alarm(TIMEOUT_MINS*60 + 60)


def get_schedds(args=None):
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
            except KeyError: pass

    schedd_ads = schedd_ads.values()
    random.shuffle(schedd_ads)

    if args and args.schedd_filter:
        return [s for s in schedd_ads if s['Name'] in args.schedd_filter.split(',')]

    return schedd_ads


def process_schedd_queue(starttime, schedd_ad, queue, args):
    my_start = time.time()
    logging.info("Querying %s queue for jobs." % schedd_ad["Name"])
    if time_remaining(starttime) < 0:
        message = ("No time remaining to run queue crawler on %s; "
                   "exiting." % schedd_ad['Name'] )
        logging.error(message)
        send_email_alert(args.email_alerts,
                         "spider_cms queue timeout warning",
                         message)
        return

    count = 0
    queue.put(schedd_ad['Name'], timeout=time_remaining(starttime))

    schedd = htcondor.Schedd(schedd_ad)
    had_error = True
    sent_warnings = False
    batch = []
    try:
        query_iter = schedd.xquery() if not args.dry_run else []
        for job_ad in query_iter:
            dict_ad = None
            try:
                dict_ad = convert_to_json(job_ad, return_dict=True,
                                          reduce_data=args.reduce_running_data)
            except Exception as e:
                message = ("Failure when converting document on %s queue: %s" %
                                (schedd_ad["Name"], str(e)))
                logging.warning(message)
                if not sent_warnings:
                    send_email_alert(args.email_alerts,
                                     "spider_cms queue document conversion error",
                                     message)
                    sent_warnings = True

            if not dict_ad:
                continue

            batch.append((job_ad["GlobalJobId"], dict_ad))
            count += 1

            if not args.dry_run and len(batch) == args.query_queue_batch_size:
                queue.put(batch, timeout=time_remaining(starttime))
                batch = []

            if time_remaining(starttime) < 0:
                message = ("Queue crawler on %s has been running for "
                           "more than %d minutes; exiting" %
                               (schedd_ad['Name'], TIMEOUT_MINS))
                logging.error(message)
                send_email_alert(args.email_alerts,
                                 "spider_cms queue timeout warning",
                                 message)
                break

        if batch:  # send remaining docs
            queue.put(batch, timeout=time_remaining(starttime))
            batch = []


        had_error = False

    except RuntimeError, e:
        logging.error("Failed to query schedd %s for jobs: %s" %
                      (schedd_ad["Name"], str(e)))
    except Exception, e:
        message = ("Failure when processing schedd queue query on %s: %s" % 
                       (schedd_ad["Name"], str(e)))
        logging.error(message)
        send_email_alert(args.email_alerts, "spider_cms schedd queue query error",
                         message)
        traceback.print_exc()

    queue.put(schedd_ad['Name'], timeout=time_remaining(starttime))
    total_time = (time.time() - my_start)/60.
    logging.warning(("Schedd %-25s queue: response count: %5d; "
                     "query time %.2f min; ") % (
                           schedd_ad["Name"], count, total_time))

    return count


def process_schedd(starttime, last_completion, schedd_ad, args):
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
    logging.info("Querying %s for history: %s.  %.1f minutes of ads" % (schedd_ad["Name"],
                                                                 history_query,
                                                                 (time.time()-last_completion)/60.))
    buffered_ads = {}
    count = 0
    total_upload = 0
    sent_warnings = False
    if not args.read_only:
        if args.feed_es:
            es = htcondor_es.es.get_server_handle(args) # es-cms.cern.ch now
        if args.feed_amq:
            amq = htcondor_es.amq.get_amq_interface()
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
                        data_for_amq = [(id_, convert_dates_to_millisecs(dict_ad)) for id_, dict_ad in ad_list]
                        htcondor_es.amq.post_ads(data_for_amq)
                logging.debug("...posting %d ads from %s (process_schedd)" % (len(ad_list), schedd_ad["Name"]))
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
        logging.error("Failed to query schedd for job history: %s" % schedd_ad["Name"])

    except Exception, e:
        message = ("Failure when processing schedd history query on %s: %s" % 
                       (schedd_ad["Name"], str(e)))
        logging.exception(message)
        send_email_alert(args.email_alerts, "spider_cms schedd history query error",
                         message)

    # Post the remaining ads
    for idx, ad_list in buffered_ads.items():
        if ad_list:
            logging.debug("...posting remaining %d ads from %s (process_schedd)" % (len(ad_list), schedd_ad["Name"]))
            if not args.read_only:
                if args.feed_es:
                    htcondor_es.es.post_ads(es=es.handle, idx=idx,
                                            ads=[(id_, json.dumps(dict_ad)) for
                                                    id_, dict_ad in ad_list])
                if args.feed_amq:
                    data_for_amq = [(id_, convert_dates_to_millisecs(dict_ad)) for id_, dict_ad in ad_list]
                    htcondor_es.amq.post_ads(data_for_amq)


    total_time = (time.time() - my_start) / 60.
    total_upload /= 60.
    logging.warning(("Schedd %-25s history: response count: %5d; last completion %s; "
                     "query time %.2f min; upload time %.2f min") % (
                      schedd_ad["Name"],
                      count,
                      datetime.datetime.fromtimestamp(last_completion).strftime("%Y-%m-%d %H:%M:%S"),
                      total_time - total_upload,
                      total_upload))

    try:
        checkpoint_new = json.load(open("checkpoint.json"))
    except:
        checkpoint_new = {}

    if ( (schedd_ad["Name"] not in checkpoint_new) or 
         (checkpoint_new[schedd_ad["Name"]] < last_completion) ):
        checkpoint_new[schedd_ad["Name"]] = last_completion

    fd, tmpname = tempfile.mkstemp(dir=".", prefix="checkpoint.json.new")
    fd = os.fdopen(fd, "w")
    json.dump(checkpoint_new, fd)
    fd.close()
    os.rename(tmpname, "checkpoint.json")

    return last_completion


class ListenAndBunch(multiprocessing.Process):
    """
    Listens to incoming items on a queue and puts bunches of items
    to an outgoing queue

    n_expected is the expected number of agents writing to the
    queue. Necessary for knowing when to shut down.
    """
    def __init__(self, input_queue, output_queue,
                 n_expected,
                 starttime,
                 bunch_size=5000,
                 report_every=50000):
        super(ListenAndBunch, self).__init__()
        self.input_queue = input_queue
        self.output_queue = output_queue
        logging.warning("Bunching records for AMQP in sizes of %d" % bunch_size)
        self.bunch_size = bunch_size
        self.report_every = report_every
        self.n_expected = n_expected
        self.starttime = starttime

        self.buffer = []
        self.tracker = []
        self.n_processed = 0
        self.count_in = 0 # number of added docs

        self.start()

    def run(self):
        since_last_report = 0
        while True:
            next_batch = self.input_queue.get(timeout=time_remaining(self.starttime))

            if isinstance(next_batch, basestring):
                schedd_name = str(next_batch)
                try:
                    # We were already processing this sender,
                    # this is the signal that it's done sending.
                    self.tracker.remove(schedd_name)
                    self.n_processed += 1
                except ValueError:
                    # This is a new sender
                    self.tracker.append(schedd_name)
    
                if self.n_processed == self.n_expected:
                    # We finished processing all expected senders.
                    assert(len(self.tracker) == 0)
                    self.close()
                    return
                continue

            self.count_in += len(next_batch)
            since_last_report += len(next_batch)
            self.buffer.extend(next_batch)

            if since_last_report > self.report_every:
                logging.debug("Processed %d docs" % self.count_in)
                since_last_report = 0

            # If buffer is full, send the docs and clear the buffer
            if len(self.buffer) >= self.bunch_size:
                self.output_queue.put(self.buffer[:self.bunch_size], timeout=time_remaining(self.starttime))
                self.buffer = self.buffer[self.bunch_size:]

    def close(self):
        """Clear the buffer, send a poison pill and the total number of docs"""
        if self.buffer:
            self.output_queue.put(self.buffer, timeout=time_remaining(self.starttime))
            self.buffer = []

        logging.warning("Closing listener, received %d documents total" % self.count_in)
        self.output_queue.put(None, timeout=time_remaining(self.starttime)) # send back a poison pill
        self.output_queue.put(self.count_in, timeout=time_remaining(self.starttime)) # send the number of total docs


def process_histories(schedd_ads, starttime, pool, args):
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
                                      args) )
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

    logging.warning("Processing time for history: %.2f mins" % (
                    (time.time()-starttime)/60.))


def process_queues(schedd_ads, starttime, pool, args):
    my_start = time.time()
    if time_remaining(starttime) < 0:
        logging.warning("No time remaining to process queues")
        return

    mp_manager = multiprocessing.Manager()
    input_queue = mp_manager.Queue(maxsize=10)
    output_queue = mp_manager.Queue(maxsize=2)
    listener = ListenAndBunch(input_queue=input_queue,
                              output_queue=output_queue,
                              n_expected=len(schedd_ads),
                              starttime=starttime,
                              bunch_size=5000)
    futures = []

    upload_pool = multiprocessing.Pool(processes=3)

    for schedd_ad in schedd_ads:
        future = pool.apply_async(process_schedd_queue,
                                  args=(starttime, schedd_ad, input_queue, args) )
        futures.append((schedd_ad['Name'], future))

    def _callback_amq(result):
        sent, received, elapsed = result
        logging.info("Uploaded %d/%d docs to StompAMQ in %d seconds" % (sent, received, elapsed))

    total_processed = 0
    while True:
        if args.dry_run or len(schedd_ads) == 0:
            break

        if time_remaining(starttime) < -10:
            logging.warning("Listener did not shut down properly; terminating.")
            listener.terminate()
            break

        bunch = output_queue.get(timeout=time_remaining(starttime))
        if bunch is None: # swallow the poison pill
            total_processed = int(output_queue.get(timeout=time_remaining(starttime)))
            break

        if args.feed_amq and not args.read_only:
            amq_bunch = [(id_, convert_dates_to_millisecs(dict_ad)) for id_,dict_ad in bunch]
            future = upload_pool.apply_async(htcondor_es.amq.post_ads,
                                             args=(amq_bunch,),
                                             callback=_callback_amq)
            futures.append(("UPLOADER_AMQ", future))

        if args.feed_es_for_queues and not args.read_only:
            es_bunch = [(id_, json.dumps(dict_ad)) for id_,dict_ad in bunch]
            ## FIXME: Why are we determining the index from one ad?
            es_handle = htcondor_es.es.get_server_handle(args)
            idx = htcondor_es.es.get_index(bunch[0][1].get("QDate", int(time.time())),
                                           template=args.es_index_template,
                                           update_es=(args.feed_es and not args.read_only))

            future = upload_pool.apply_async(htcondor_es.es.post_ads_nohandle,
                                             args=(idx, es_bunch, args))
            futures.append(("UPLOADER_ES", future))

        max_in_progress = 3
        count = len(futures)
        while count > max_in_progress:
           if time_remaining(starttime) < 0:
               break
           for future in futures:
               if future[1].ready():
                   count -= 1
           if count > max_in_progress:
               break
           for future in futures:
               future.wait(time_remaining(starttime) + 10)
               break
           count = len(futures)

    listener.join()

    timed_out = False
    total_sent = 0
    total_upload_time = 0
    total_queried = 0
    for name, future in futures:
        if time_remaining(starttime) > -10:
            try:
                count = future.get(time_remaining(starttime)+10)
                if name == "UPLOADER_AMQ":
                    total_sent += count[0]
                    total_upload_time += count[2]
                elif name == "UPLOADER_ES":
                    total_sent += count
                else:
                    total_queried += count
            except multiprocessing.TimeoutError:
                message = "Schedd %s queue timed out; ignoring progress." % name
                logging.error(message)
                send_email_alert(args.email_alerts,
                                 "spider_cms queue timeout warning",
                                 message)
        else:
            timed_out = True
            break

    if timed_out:
        pool.terminate()
        upload_pool.terminate()

    if not total_queried == total_processed:
        logging.warning("Number of queried docs not equal to number of processed docs.")    

    logging.warning("Processing time for queues: %.2f mins, %d/%d docs sent in %.2f min of total upload time"
                      % ((time.time()-my_start)/60., total_sent, total_queried, total_upload_time/60.))

    upload_pool.close()
    upload_pool.join()


def main(args):
    starttime = time.time()

    # Get all the schedd ads
    schedd_ads = get_schedds()
    logging.warning("&&& There are %d schedds to query." % len(schedd_ads))

    pool = multiprocessing.Pool(processes=5)

    if not args.skip_history:
        process_histories(schedd_ads=schedd_ads,
                          starttime=starttime,
                          pool=pool,
                          args=args)

    # Now that we have the fresh history, process the queues themselves.
    if args.process_queue:
        process_queues(schedd_ads=schedd_ads,
                       starttime=starttime,
                       pool=pool,
                       args=args)

    pool.close()
    pool.join()

    logging.warning("@@@ Total processing time: %.2f mins" % ((time.time()-starttime)/60.))

    return 0


if __name__ == "__main__":
    parser = ArgumentParser()
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
    parser.add_argument("--reduce_running_data", action='store_true',
                        dest="reduce_running_data",
                        help="Drop all but some fields for running jobs.")
    parser.add_argument("--bunching", default=250,
                        type=int, dest="bunching",
                        help=("Send docs in bunches of this number "
                              "[default: %(default)d]"))
    parser.add_argument("--query_queue_batch_size", default=50,
                        type=int, dest="query_queue_batch_size",
                        help=("Send docs to listener in batches of this number "
                              "[default: %(default)d]"))

    parser.add_argument("--es_hostname", default='es-cms.cern.ch',
                        type=str, dest="es_hostname",
                        help="Hostname of the elasticsearch instance to be used [default: %(default)s]")
    parser.add_argument("--es_port", default=9203,
                        type=int, dest="es_port",
                        help="Port of the elasticsearch instance to be used [default: %(default)d]")
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
                        help="Log level (CRITICAL/ERROR/WARNING/INFO/DEBUG) [default: %(default)s]")
    parser.add_argument("--email_alerts", default=[], action='append',
                        dest="email_alerts",
                        help="Email addresses for alerts [default: none]")

    args = parser.parse_args()
    set_up_logging(args)

    # --dry_run implies read_only
    args.read_only = args.read_only or args.dry_run

    main(args)

