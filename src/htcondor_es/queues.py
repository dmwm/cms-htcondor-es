"""
Process the jobs in queue for given set of schedds.
"""

import time
import logging
import resource
import traceback
import queue
import multiprocessing

import htcondor

import htcondor_es.es
from htcondor_es.utils import send_email_alert, time_remaining, TIMEOUT_MINS
from htcondor_es.convert_to_json import convert_to_json
from htcondor_es.convert_to_json import convert_dates_to_millisecs
from htcondor_es.convert_to_json import unique_doc_id


class ListenAndBunch(multiprocessing.Process):
    """
    Listens to incoming items on a queue and puts bunches of items
    to an outgoing queue

    n_expected is the expected number of agents writing to the
    queue. Necessary for knowing when to shut down.
    """

    def __init__(
        self,
        input_queue,
        output_queue,
        n_expected,
        starttime,
        bunch_size=5000,
        report_every=50000,
    ):
        super(ListenAndBunch, self).__init__()
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.bunch_size = bunch_size
        self.report_every = report_every
        self.n_expected = n_expected
        self.starttime = starttime

        self.buffer = []
        self.tracker = []
        self.n_processed = 0
        self.count_in = 0  # number of added docs

        self.start()

    def run(self):
        since_last_report = 0
        while True:
            try:
                next_batch = self.input_queue.get(
                    timeout=time_remaining(self.starttime - 5)
                )
            except queue.Empty:
                logging.warning("Closing listener before all schedds were processed")
                self.close()
                return

            if isinstance(next_batch, str):
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
                    assert len(self.tracker) == 0
                    self.close()
                    return
                continue

            self.count_in += len(next_batch)
            since_last_report += len(next_batch)
            self.buffer.extend(next_batch)

            if since_last_report > self.report_every:
                logging.debug("Processed %d docs", self.count_in)
                since_last_report = 0

            # If buffer is full, send the docs and clear the buffer
            if len(self.buffer) >= self.bunch_size:
                self.output_queue.put(
                    self.buffer[: self.bunch_size],
                    timeout=time_remaining(self.starttime),
                )
                self.buffer = self.buffer[self.bunch_size :]

    def close(self):
        """Clear the buffer, send a poison pill and the total number of docs"""
        if self.buffer:
            self.output_queue.put(self.buffer, timeout=time_remaining(self.starttime))
            self.buffer = []

        logging.warning("Closing listener, received %d documents total", self.count_in)
        # send back a poison pill
        self.output_queue.put(None, timeout=time_remaining(self.starttime))
        # send the number of total docs
        self.output_queue.put(self.count_in, timeout=time_remaining(self.starttime))


def query_schedd_queue(starttime, schedd_ad, queue, args):
    my_start = time.time()
    pool_name = schedd_ad.get("CMS_Pool", "Unknown")
    logging.info("Querying %s queue for jobs.", schedd_ad["Name"])
    if time_remaining(starttime) < 10:
        message = (
            "No time remaining to run queue crawler on %s; "
            "exiting." % schedd_ad["Name"]
        )
        logging.error(message)
        send_email_alert(args.email_alerts, "spider_cms queue timeout warning", message)
        return

    count_since_last_report = 0
    count = 0
    cpu_usage = resource.getrusage(resource.RUSAGE_SELF).ru_utime
    queue.put(schedd_ad["Name"], timeout=time_remaining(starttime))

    schedd = htcondor.Schedd(schedd_ad)
    sent_warnings = False
    batch = []
    # Query for a snapshot of the jobs running/idle/held,
    # but only the completed that had changed in the last period of time.
    _completed_since = starttime - (TIMEOUT_MINS + 1) * 60
    query = ("""
         (JobStatus < 3 || JobStatus > 4 
         || EnteredCurrentStatus >= %(completed_since)d
         || CRAB_PostJobLastUpdate >= %(completed_since)d
         ) && (CMS_Type != "DONOTMONIT")
         """
        % {"completed_since": _completed_since}
    )
    try:
        query_iter = schedd.xquery(requirements=query) if not args.dry_run else []
        for job_ad in query_iter:
            dict_ad = None
            try:
                dict_ad = convert_to_json(
                    job_ad, return_dict=True, reduce_data=not args.keep_full_queue_data, pool_name=pool_name
                )
            except Exception as e:
                message = "Failure when converting document on %s queue: %s" % (
                    schedd_ad["Name"],
                    str(e),
                )
                logging.warning(message)
                if not sent_warnings:
                    send_email_alert(
                        args.email_alerts,
                        "spider_cms queue document conversion error",
                        message,
                    )
                    sent_warnings = True

            if not dict_ad:
                continue

            batch.append((unique_doc_id(dict_ad), dict_ad))
            count += 1
            count_since_last_report += 1

            if not args.dry_run and len(batch) == args.query_queue_batch_size:
                if time_remaining(starttime) < 10:
                    message = (
                        "Queue crawler on %s has been running for "
                        "more than %d minutes; exiting"
                        % (schedd_ad["Name"], TIMEOUT_MINS)
                    )
                    logging.error(message)
                    send_email_alert(
                        args.email_alerts, "spider_cms queue timeout warning", message
                    )
                    break
                queue.put(batch, timeout=time_remaining(starttime))
                batch = []
                if count_since_last_report >= 1000:
                    cpu_usage_now = resource.getrusage(resource.RUSAGE_SELF).ru_utime
                    cpu_usage = cpu_usage_now - cpu_usage
                    processing_rate = count_since_last_report / cpu_usage
                    cpu_usage = cpu_usage_now
                    logging.info(
                        "Processor for %s has processed %d jobs "
                        "(%.1f jobs per CPU-second)",
                        schedd_ad["Name"],
                        count,
                        processing_rate,
                    )
                    count_since_last_report = 0

            if args.max_documents_to_process and count > args.max_documents_to_process:
                logging.warning(
                    "Aborting after %d documents (--max_documents_to_process option)"
                    % args.max_documents_to_process
                )
                break

    except RuntimeError as e:
        logging.error(
            "Failed to query schedd %s for jobs: %s", schedd_ad["Name"], str(e)
        )
    except Exception as e:
        message = "Failure when processing schedd queue query on %s: %s" % (
            schedd_ad["Name"],
            str(e),
        )
        logging.error(message)
        send_email_alert(
            args.email_alerts, "spider_cms schedd queue query error", message
        )
        traceback.print_exc()

    if batch:  # send remaining docs
        queue.put(batch, timeout=time_remaining(starttime))
        batch = []

    queue.put(schedd_ad["Name"], timeout=time_remaining(starttime))
    total_time = (time.time() - my_start) / 60.0
    logging.warning(
        "Schedd %-25s queue: response count: %5d; " "query time %.2f min; ",
        schedd_ad["Name"],
        count,
        total_time,
    )

    return count


def process_queues(schedd_ads, starttime, pool, args, metadata=None):
    """
    Process all the jobs in all the schedds given.
    """
    my_start = time.time()
    if time_remaining(starttime) < 10:
        logging.warning("No time remaining to process queues")
        return

    metadata = metadata or {}
    metadata["spider_source"] = "condor_queue"

    mp_manager = multiprocessing.Manager()
    input_queue = mp_manager.Queue()
    output_queue = mp_manager.Queue()
    listener = ListenAndBunch(
        input_queue=input_queue,
        output_queue=output_queue,
        n_expected=len(schedd_ads),
        starttime=starttime,
    )
    futures = []

    upload_pool = multiprocessing.Pool(processes=args.upload_pool_size)

    for schedd_ad in schedd_ads:
        future = pool.apply_async(
            query_schedd_queue, args=(starttime, schedd_ad, input_queue, args)
        )
        futures.append((schedd_ad["Name"], future))

    total_processed = 0
    while True:
        if args.dry_run or len(schedd_ads) == 0:
            break

        if time_remaining(starttime) < 5:
            logging.warning("Listener did not shut down properly; terminating.")
            listener.terminate()
            break

        bunch = output_queue.get(timeout=time_remaining(starttime))
        if bunch is None:  # swallow the poison pill
            total_processed = int(output_queue.get(timeout=time_remaining(starttime)))
            break

        if args.feed_es_for_queues and not args.read_only:
            ## Note that these bunches are sized according to --amq_bunch_size
            ## FIXME: Why are we determining the index from one ad?
            idx = htcondor_es.es.get_index(
                bunch[0][1].get("QDate", int(time.time())),
                template=args.es_index_template,
                update_es=(args.feed_es and not args.read_only),
            )

            future = upload_pool.apply_async(
                htcondor_es.es.post_ads_nohandle, args=(idx, bunch, args, metadata)
            )
            futures.append(("UPLOADER_ES", future))

        logging.info("Starting new uploader, %d items in queue" % output_queue.qsize())

    listener.join()

    timed_out = False
    total_sent = 0
    total_upload_time = 0
    total_queried = 0
    for name, future in futures:
        if time_remaining(starttime, positive=False) > -20:
            try:
                count = future.get(time_remaining(starttime) + 10)
                if name == "UPLOADER_ES":
                    total_sent += count
                else:
                    try:
                        total_queried += count
                    except TypeError:
                        pass
            except multiprocessing.TimeoutError:
                message = "Schedd %s queue timed out; ignoring progress." % name
                logging.error(message)
                send_email_alert(
                    args.email_alerts, "spider_cms queue timeout warning", message
                )
        else:
            timed_out = True
            break

    if timed_out:
        logging.error("Timed out when retrieving uploaders. Upload count incomplete.")
        pool.terminate()
        upload_pool.terminate()

    if not total_queried == total_processed:
        logging.warning("Number of queried docs not equal to number of processed docs.")

    logging.warning(
        "Processing time for queues: %.2f mins, %d/%d docs sent in %.2f min "
        "of total upload time",
        (time.time() - my_start) / 60.0,
        total_sent,
        total_queried,
        total_upload_time / 60.0,
    )

    upload_pool.close()
    upload_pool.join()
