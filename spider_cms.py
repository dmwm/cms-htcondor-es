#!/usr/bin/python

import os
import sys
import json
import time
import random
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

TIMEOUT_MINS = 11

try:
    import htcondor_es.es
    import htcondor_es.amq
    from htcondor_es.convert_to_json import convert_to_json
    from htcondor_es.convert_to_json import convert_dates_to_millisecs
    from htcondor_es.convert_to_json import get_data_collection_time
except ImportError:
    if os.path.exists("src/htcondor_es/__init__.py") and "src" not in sys.path:
        sys.path.append("src")
        import htcondor_es.es
        import htcondor_es.amq
        from htcondor_es.convert_to_json import convert_to_json
        from htcondor_es.convert_to_json import convert_dates_to_millisecs
        from htcondor_es.convert_to_json import get_data_collection_time
    else:
        raise

now = time.time()
now_ns = int(time.time())*int(1e9)

def time_remaining(starttime, timeout=TIMEOUT_MINS*60):
    """
    Return the remaining time (in seconds) until starttime + timeout
    """
    elapsed = time.time() - starttime
    return timeout - elapsed

def send_email_alert(recipients, subject, message):
    if not recipients:
        return
    try:
        import smtplib, getpass
        from email.mime.text import MIMEText
        msg = MIMEText(message)
        msg['Subject'] = "%s - %sh: %s" % (socket.gethostname(),
                                          time.strftime("%b %d, %H:%M"),
                                          subject)

        domain = socket.getfqdn()
        if not 'cern.ch' in domain:
            domain = '%s.unl.edu' % socket.gethostname()
        msg['From'] = '%s@%s' % (getpass.getuser(), domain)
        msg['To'] = recipients[0]

        s = smtplib.SMTP('localhost')
        s.sendmail(msg['From'], recipients, msg.as_string())
        s.quit()

    except ImportError:
        logging.warning("Email notification failed: ImportError")
    except Exception, e:
        logging.warning("Email notification failed: %s" % str(e))


def get_schedds():
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

    return schedd_ads


def clean_old_jobs(starttime, name, es):
    collection_date = get_data_collection_time()
    idx = htcondor_es.es.get_index(time.time(), update_es=True)
    body = {"query": {
      "bool": {
        "must" : [
                  {"term": {"Status": "running"}},
                  {"match": {"ScheddName": name.split(".")[0]}},
                  {"range": {"DataCollection": {"lt": collection_date-1200}}},
                 ],
        #"should": [
        #           {"bool": {"must_not": {"exists": {"field": "ScheddName"}}}},
        #           {"bool": {"must_not": {"exists": {"field": "DataCompletion"}}}},
        #          ],
        #"minimum_should_match": 2,
      }
    }}
    body = json.dumps(body)
    print "Old job query:", body
    while True:
        print "Querying for stale jobs from", name
        results = es.search(index="cms-2016*", size=1000, body=body, _source=False)['hits']['hits']
        #print results
        if not results:
            print "No invalid results found for %s." % name
            break
        print "Invalidating %d old results from %s" % (len(results), name)
        bulk = ''
        for result in results:
            bulk += json.dumps({"update": {"_id": result["_id"], "_type": "job", "_index": result["_index"]}}) + "\n"
            bulk += '{"doc": {"Status": "unknown"}}\n'

        try:
            es.bulk(body=bulk)
        except Exception, e:
            print "Failure when updating stale documents:", str(e)
            raise
        if time_remaining(starttime) < 0:
            print "Out of time for cleanup; exiting."
            break


def create_job_keys(schedd_name, job_ad, json_ad):
    if 'CRAB_Id' in job_ad:  # Analysis job
        subtask = 'Analysis'
        task = job_ad.get("CRAB_Workflow", "UNKNOWN").split(":", 1)[-1]
    else:
        subtask = json_ad.get('WMAgent_TaskType', 'Unknown')
        task = json_ad.get('Campaign', 'Unknown')
    job_ad.setdefault('RequestMemory', 2000)
    job_ad.setdefault('RequestDisk', 2000000)
    job_ad.setdefault('MaxWallTimeMins', 1440)
    base_key = 'schedd=%s,memory=%d,disk=%d,walltime=%d,desired_sites=%d,type=%s,task_type=%s,task=%s,subtask=%s,campaign=%s,workflow=%s,priority=%d' % \
       (schedd_name,
        int(job_ad.eval('RequestMemory')),
        int(job_ad.eval('RequestDisk')),
        int(job_ad.eval('MaxWallTimeMins')),
        int(json_ad.get('DesiredSiteCount', 0)),
        json_ad.get('Type', 'Unknown'),
        json_ad.get('TaskType', 'Unknown'),
        task,
        subtask,
        json_ad.get('Campaign', 'Unknown'),
        json_ad.get('Workflow', 'Unknown'),
        int(job_ad.get('JobPrio', 0))
       )
    keys = []
    if job_ad['JobStatus'] == 2:
        site_info = json_ad['Site'].split("_", 2)
        if len(site_info) != 3:
            tier = 'Unknown'
            country = 'Unknown'
        else:
            tier = site_info[0]
            country = site_info[1]
        keys.append("%s,site=%s,tier=%s,country=%s" % (base_key, json_ad['Site'], tier, country))
    elif job_ad['JobStatus'] == 1:
        for site in json_ad['DESIRED_Sites']:
            if site:
                site_info = site.split("_", 2)
                if len(site_info) != 3:
                    tier = 'Unknown'
                    country = 'Unknown'
                else:
                    tier = site_info[0]
                    country = site_info[1]
                keys.append("%s,site=%s,tier=%s,country=%s" % (base_key, site, tier, country))
    return base_key, keys


def job_info_to_text(measurement, key, running, coresrunning, idle, coresidle):
    return "%s,%s running=%d,cores_running=%d,idle=%d,cores_idle=%d %d" % \
       (measurement,
        key,
        running,
        coresrunning,
        idle,
        coresidle,
        now_ns
       )


g_influx_socket = None
def get_influx_socket():
    global g_influx_socket
    if g_influx_socket:
        return g_influx_socket
    g_influx_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return g_influx_socket


def report_site_jobs_influx(sites_jobs_running, sites_jobs_coresrunning, sites_jobs_idle, sites_jobs_coresidle):
    count = 0
    text = ''
    all_keys = set()
    all_keys.update(sites_jobs_running.keys())
    all_keys.update(sites_jobs_coresrunning.keys())
    all_keys.update(sites_jobs_idle.keys())
    all_keys.update(sites_jobs_coresidle.keys())
    influx = get_influx_socket()
    for key in all_keys:
        text += job_info_to_text('site_jobs', key, sites_jobs_running[key],
                                                   sites_jobs_coresrunning[key],
                                                   sites_jobs_idle[key],
                                                   sites_jobs_coresidle[key])
        text += '\n'
        count += 1
        if count == 20:
            influx.sendto(text, ('127.0.0.1', 8089))
            count = 0
            text = ''
    if text:
        influx.sendto(text, ('127.0.0.1', 8089))


def report_global_jobs_influx(global_jobs_running, global_jobs_coresrunning, global_jobs_idle, global_jobs_coresidle):
    count = 0
    text = ''
    all_keys = set()
    all_keys.update(global_jobs_running.keys())
    all_keys.update(global_jobs_coresrunning.keys())
    all_keys.update(global_jobs_idle.keys())
    all_keys.update(global_jobs_coresidle.keys())
    influx = get_influx_socket()
    for key in all_keys:
        text += job_info_to_text('global_jobs', key, global_jobs_running[key],
                                                   global_jobs_coresrunning[key],
                                                   global_jobs_idle[key],
                                                   global_jobs_coresidle[key])
        text += '\n'
        count += 1
        if count == 20:
            influx.sendto(text, ('127.0.0.1', 8089))
            count = 0
            text = ''
    if text:
        influx.sendto(text, ('127.0.0.1', 8089))


def process_collector(args):
    logging.debug("Querying collector for data")
    if args.dry_run: return
    try:
        coll = htcondor.Collector("cmssrv221.fnal.gov:9620")
        ads = coll.query(htcondor.AdTypes.Startd,
                         'DynamicSlot=!=true',
                         ['TotalSlotCpus', 'SlotType', 'Cpus', 'Memory',
                          'State', 'GLIDEIN_ToRetire', 'GLIDEIN_CMSSite',
                          'GLIDEIN_Site', 'GLIDEIN_Entry_Name', 'GLIDEIN_Factory'])
    except RuntimeError, e:
        logging.debug("Failed to query collector: %s" % str(e))
        return
    info = {}

    for ad in ads:
        slots = int(ad.get('TotalSlotCpus', ad.get('Cpus', 1)))
        if ad.get('SlotType', 'Unknown') == 'Partitionable':
            payloads = slots - int(ad.get('Cpus', 0))
        elif ad.get('State') == 'Claimed':
            payloads = ad.get('Cpus', 1)
        else:
            payloads = 0
        site_info = ad.get('GLIDEIN_CMSSite', 'Unknown').split("_", 2)
        if len(site_info) != 3:
            tier = 'Unknown'
            country = 'Unknown'
        else:
            tier = site_info[0]
            country = site_info[1]

        key = ("tier=%s,country=%s,cms_site=%s,site=%s,slot_type=%s,"
               "cpus=%d,payloads=%d,memory=%d,entry_point=%s,factory=%s") % (
            tier,
            country,
            ad.get('GLIDEIN_CMSSite', 'Unknown'),
            ad.get('GLIDEIN_Site', 'Unknown'),
            ad.get('SlotType', 'Unknown'),
            slots,
            payloads,
            int(ad.get('Memory', 2500)),
            ad.get('GLIDEIN_Entry_Name', 'Unknown'),
            ad.get('GLIDEIN_Factory', 'Unknown'),
           )

        values = info.setdefault(key, collections.defaultdict(float))
        values['count'] += 1
        if ad.get('SlotType', 'Unknown') == 'Partitionable':
            values['claimed_cores'] += slots - int(ad.get('Cpus', 0))
            if now > ad.get('GLIDEIN_ToRetire', 0):
                values['retiring_cores'] += int(ad.get('Cpus', 0))
            else:
                values['unclaimed_cores'] += int(ad.get('Cpus', 0))
        elif ad.get('State') == 'Claimed':
            values['claimed_cores'] += ad.get('Cpus', 1)
        elif now > ad.get('GLIDEIN_ToRetire', 0):
            values['retiring_cores'] += ad.get('Cpus', 1)
        else:
            values['unclaimed_cores'] += ad.get('Cpus', 1)

    text = ''
    count = 0
    influx = get_influx_socket()
    for key, values in info.items():
        value_info = ",".join(["%s=%s" % (i[0], i[1]) for i in values.items()])
        text += 'slots,%s %s %d\n' % (key, value_info, now_ns)
        count += 1
        if count == 10:
            influx.sendto(text, ('127.0.0.1', 8089))
            count = 0
            text = ''
    if text:
        influx.sendto(text, ('127.0.0.1', 8089))


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
    queue.put(schedd_ad['Name'])

    sites_jobs_running       = collections.defaultdict(int)
    sites_jobs_idle          = collections.defaultdict(int)
    sites_jobs_coresrunning  = collections.defaultdict(int)
    sites_jobs_coresidle     = collections.defaultdict(int)
    global_jobs_running      = collections.defaultdict(int)
    global_jobs_coresrunning = collections.defaultdict(int)
    global_jobs_idle         = collections.defaultdict(int)
    global_jobs_coresidle    = collections.defaultdict(int)

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

            if args.feed_influxdb:
                global_key, job_keys = create_job_keys(schedd_ad['Name'], job_ad, dict_ad)
                if job_keys and ('JobStatus' in job_ad):
                    for job_key in job_keys:
                        if job_ad['JobStatus'] == 2:
                            sites_jobs_running[job_key] += 1
                            sites_jobs_coresrunning[job_key] += job_ad.get('RequestCpus', 1)
                        elif job_ad['JobStatus'] == 1:
                            sites_jobs_idle[job_key]    += 1
                            sites_jobs_coresidle[job_key]    += job_ad.get('RequestCpus', 1)
                    if job_ad['JobStatus'] == 2:
                        global_jobs_running[global_key] += 1
                        global_jobs_coresrunning[global_key] += job_ad.get('RequestCpus', 1)
                    elif job_ad['JobStatus'] == 1:
                        global_jobs_idle[global_key] += 1
                        global_jobs_coresidle[global_key] += job_ad.get('RequestCpus', 1)

            batch.append((job_ad["GlobalJobId"], dict_ad))
            count += 1

            if not args.read_only and len(batch) == args.query_queue_batch_size:
                queue.put(batch)
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
            queue.put(batch)
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

    queue.put(schedd_ad['Name'])
    total_time = (time.time() - my_start)/60.
    logging.warning(("Schedd %-25s queue: response count: %5d; "
                     "query time %.2f min; ") % (
                           schedd_ad["Name"], count, total_time))
    # clean_old_jobs(starttime, schedd_ad["Name"], es) # FIXME

    try:
        if not had_error and args.feed_influxdb:
            report_site_jobs_influx(sites_jobs_running, sites_jobs_coresrunning, sites_jobs_idle, sites_jobs_coresidle)
            report_global_jobs_influx(global_jobs_running, global_jobs_coresrunning, global_jobs_idle, global_jobs_coresidle)
    except Exception, e:
        logging.error("Failure when uploading InfluxDB results: %s" % str(e))
        traceback.print_exc()

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


def set_up_logging(args):
    """Configure root logger with rotating file handler"""
    logger = logging.getLogger()

    log_level = getattr(logging, args.log_level.upper(), None)
    if not isinstance(log_level, int):
        raise ValueError('Invalid log level: %s' % log_level)
    logger.setLevel(log_level)

    if not os.path.isdir(args.log_dir):
        os.system('mkdir -p %s' % args.log_dir)

    log_file = os.path.join(args.log_dir, 'spider_cms.log')
    filehandler = RotatingFileHandler(log_file, maxBytes=100000)
    filehandler.setFormatter(
        logging.Formatter('%(asctime)s : %(name)s:%(levelname)s - %(message)s'))

    logger.addHandler(filehandler)


class ListenAndBunch(multiprocessing.Process):
    """
    Listens to incoming items on a queue and puts bunches of items
    to an outgoing queue

    n_expected is the expected number of agents writing to the
    queue. Necessary for knowing when to shut down.
    """
    def __init__(self, input_queue, output_queue,
                 n_expected,
                 bunch_size=1000,
                 report_every=50000):
        super(ListenAndBunch, self).__init__()
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.bunch_size = bunch_size
        self.report_every = report_every
        self.n_expected = n_expected

        self.buffer = []
        self.tracker = []
        self.n_processed = 0
        self.count_in = 0 # number of added docs

        self.start()

    def run(self):
        while True:
            next_batch = self.input_queue.get()

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
            self.buffer.extend(next_batch)

            if self.count_in%self.report_every == 0:
                logging.debug("Processed %d docs" % self.count_in)

            # If buffer is full, send the docs and clear the buffer
            if len(self.buffer) >= self.bunch_size:
                self.output_queue.put(self.buffer[:self.bunch_size])
                self.buffer = self.buffer[self.bunch_size:]

    def close(self):
        """Clear the buffer, send a poison pill and the total number of docs"""
        if self.buffer:
            self.output_queue.put(self.buffer)
            self.buffer = []

        logging.warning("Closing listener, received %d documents total" % self.count_in)
        self.output_queue.put(None) # send back a poison pill
        self.output_queue.put(self.count_in) # send the number of total docs


def process_histories(schedd_ads, starttime, pool, args):
    try:
        checkpoint = json.load(open("checkpoint.json"))
    except:
        checkpoint = {}

    futures = []

    if args.feed_influxdb:
        future = pool.apply_async(process_collector, (args,))
        futures.append(('collector', future))

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
    input_queue = mp_manager.Queue()
    output_queue = mp_manager.Queue()
    listener = ListenAndBunch(input_queue=input_queue,
                              output_queue=output_queue,
                              n_expected=len(schedd_ads),
                              bunch_size=10000)
    futures = []

    for schedd_ad in schedd_ads:
        future = pool.apply_async(process_schedd_queue,
                                  args=(starttime, schedd_ad, input_queue, args) )
        futures.append((schedd_ad['Name'], future))

    def _callback_amq(result):
        sent, received, elapsed = result
        logging.info("Uploaded %d/%d docs to StompAMQ in %d seconds" % (sent, received, elapsed))

    total_processed = 0
    while True:
        if args.read_only:
            break

        if time_remaining(starttime) < 0:
            logging.warning("Listener did not shut down properly; terminating.")
            listener.terminate()
            break

        bunch = output_queue.get()
        if bunch is None: # swallow the poison pill
            total_processed = int(output_queue.get())
            break

        if args.feed_amq:
            amq_bunch = [(id_, convert_dates_to_millisecs(dict_ad)) for id_,dict_ad in bunch]
            future = pool.apply_async(htcondor_es.amq.post_ads,
                                  args=(amq_bunch,),
                                  callback=_callback_amq)
            futures.append(("UPLOADER_AMQ", future))

        if args.feed_es_for_queues:
            es_bunch = [(id_, json.dumps(dict_ad)) for id_,dict_ad in bunch]
            ## FIXME: Why are we determining the index from one ad?
            es_handle = htcondor_es.es.get_server_handle(args)
            idx = htcondor_es.es.get_index(bunch[0][1].get("QDate", int(time.time())),
                                           template=args.es_index_template,
                                           update_es=(args.feed_es and not args.read_only))

            future = pool.apply_async(htcondor_es.es.post_ads_nohandle,
                                      args=(idx, es_bunch, args))
            futures.append(("UPLOADER_ES", future))

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

    if not total_queried == total_processed:
        logging.warning("Number of queried docs not equal to number of processed docs.")    

    logging.warning("Processing time for queues: %.2f mins, %d/%d docs sent in %.2f min of total upload time"
                      % ((time.time()-my_start)/60., total_sent, total_queried, total_upload_time/60.))


def main(args):
    starttime = time.time()

    # Get all the schedd ads
    schedd_ads = get_schedds()
    logging.warning("&&& There are %d schedds to query." % len(schedd_ads))

    pool = multiprocessing.Pool(processes=5)

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
    parser.add_argument("--feed_influxdb", action='store_true',
                        dest="feed_influxdb",
                        help="Feed also to InfluxDB")
    parser.add_argument("--feed_es", action='store_true',
                        dest="feed_es",
                        help="Feed to Elasticsearch")
    parser.add_argument("--feed_es_for_queues", action='store_true',
                        dest="feed_es_for_queues",
                        help="Feed queue data also to Elasticsearch")
    parser.add_argument("--feed_amq", action='store_true',
                        dest="feed_amq",
                        help="Feed to CERN AMQ")
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
    parser.add_argument("--query_queue_batch_size", default=5,
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

