#!/usr/bin/python

import os
import sys
import json
import time
import random
import socket
import classad
import htcondor
import datetime
import tempfile
import traceback
import collections
import multiprocessing


TIMEOUT_MINS = 11

try:
    import htcondor_es.es
    import htconodor_es.convert_to_json
except ImportError:
    if os.path.exists("src/htcondor_es/__init__.py") and "src" not in sys.path:
        sys.path.append("src")
        import htcondor_es.es
        import htcondor_es.convert_to_json
    else:
        raise

now = time.time()
now_ns = int(time.time())*int(1e9)

def get_schedds():
    schedd_query = classad.ExprTree('!isUndefined(CMSGWMS_Type)')
    coll = htcondor.Collector("cmssrv221.fnal.gov:9620")
    schedd_ads1 = coll.query(htcondor.AdTypes.Schedd, schedd_query, projection=["MyAddress", "ScheddIpAddr", "Name"])
    coll2 = htcondor.Collector("cmsgwms-collector-tier0.cern.ch:9620")
    schedd_ads2 = coll.query(htcondor.AdTypes.Schedd, schedd_query, projection=["MyAddress", "ScheddIpAddr", "Name"])
    schedd_ads = {}
    for ad in schedd_ads1 + schedd_ads2:
         if 'Name' not in ad:
             continue
         schedd_ads[ad['Name']] = ad
    schedd_ads = schedd_ads.values()
    random.shuffle(schedd_ads)
    return schedd_ads


def clean_old_jobs(starttime, name, es):
    collection_date = htcondor_es.convert_to_json.get_data_collection_time()
    idx = htcondor_es.es.get_index(time.time())
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
        if time.time()-starttime > TIMEOUT_MINS*60:
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


def process_collector():
    print "Querying collector for data"
    try:
        coll = htcondor.Collector("cmssrv221.fnal.gov")
        ads = coll.query(htcondor.AdTypes.Startd, 'DynamicSlot=!=true', ['TotalSlotCpus', 'SlotType', 'Cpus', 'Memory', 'State', 'GLIDEIN_ToRetire', 'GLIDEIN_CMSSite', 'GLIDEIN_Site', 'GLIDEIN_Entry_Name', 'GLIDEIN_Factory'])
    except RuntimeError, e:
        print "Failed to query collector:", str(e)
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
        key = "tier=%s,country=%s,cms_site=%s,site=%s,slot_type=%s,cpus=%d,payloads=%d,memory=%d,entry_point=%s,factory=%s" % \
           (tier,
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


def process_schedd_queue(starttime, schedd_ad):
    my_start = time.time()
    print "Querying %s for jobs." % schedd_ad["Name"]
    buffered_ads = {}
    schedd = htcondor.Schedd(schedd_ad)
    if time.time() - starttime > TIMEOUT_MINS*60:
        print "Crawler has been running for more than %d minutes; exiting." % TIMEOUT_MINS
        return
    count = 0
    total_upload = 0

    sites_jobs_running = collections.defaultdict(int)
    sites_jobs_idle    = collections.defaultdict(int)
    sites_jobs_coresrunning = collections.defaultdict(int)
    sites_jobs_coresidle    = collections.defaultdict(int)
    global_jobs_running = collections.defaultdict(int)
    global_jobs_coresrunning = collections.defaultdict(int)
    global_jobs_idle = collections.defaultdict(int)
    global_jobs_coresidle = collections.defaultdict(int)

    had_error = True
    try:
        es = htcondor_es.es.get_server_handle()
        query_iter = schedd.xquery()
        json_ad = '{}'
        for job_ad in query_iter:
            json_ad, dict_ad = htcondor_es.convert_to_json.convert_to_json(job_ad, return_dict=True)
            if not json_ad:
                continue
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
            idx = htcondor_es.es.get_index(job_ad["QDate"])
            ad_list = buffered_ads.setdefault(idx, [])
            ad_list.append((job_ad["GlobalJobId"], json_ad))
            if len(ad_list) == 250:
                st = time.time()
                htcondor_es.es.post_ads(es, idx, ad_list)
                total_upload += time.time() - st
                buffered_ads[idx] = []
            count += 1
            if time.time() - starttime > TIMEOUT_MINS*60:
                print "Crawler has been running for more than %d minutes; exiting." % TIMEOUT_MINS
                break
        had_error=False
    except RuntimeError:
        print "Failed to query schedd for jobs:", schedd_ad["Name"]
    except Exception, e:
        print "Failure when processing schedd query:", str(e)
        traceback.print_exc()

    for idx, ad_list in buffered_ads.items():
        if ad_list:
            htcondor_es.es.post_ads(es, idx, ad_list)
    buffered_ads.clear()

    total_time = (time.time() - my_start)/60.
    total_upload = total_upload / 60.
    print "Schedd %s total response count: %d; total query time %.2f min; total upload time %.2f min" % (schedd_ad["Name"], count, total_time-total_upload, total_upload)
    clean_old_jobs(starttime, schedd_ad["Name"], es)

    try:
        if not had_error:
            report_site_jobs_influx(sites_jobs_running, sites_jobs_coresrunning, sites_jobs_idle, sites_jobs_coresidle)
            report_global_jobs_influx(global_jobs_running, global_jobs_coresrunning, global_jobs_idle, global_jobs_coresidle)
    except Exception, e:
        print "Failure when uploading InfluxDB results:", str(e)
        traceback.print_exc()


def process_schedd(starttime, last_completion, schedd_ad):
    buffered_ads = {}
    my_start = time.time()
    schedd = htcondor.Schedd(schedd_ad)
    history_query = classad.ExprTree("EnteredCurrentStatus >= %d" % last_completion)
    if time.time() - starttime > TIMEOUT_MINS*60:
        print "Crawler has been running for more than %d minutes; exiting." % TIMEOUT_MINS
        return last_completion
    print "Querying %s for history: %s.  %.1f minutes of ads" % (schedd_ad["Name"], history_query, (time.time()-last_completion)/60.)
    count = 0
    total_upload = 0
    es = htcondor_es.es.get_server_handle()
    try:
        history_iter = schedd.history(history_query, [], 10000)
        json_ad = '{}'
        for job_ad in history_iter:
            json_ad = htcondor_es.convert_to_json.convert_to_json(job_ad)
            if not json_ad:
                continue
            idx = htcondor_es.es.get_index(job_ad["QDate"])
            ad_list = buffered_ads.setdefault(idx, [])
            ad_list.append((job_ad["GlobalJobId"], json_ad))
            if len(ad_list) == 250:
                st = time.time()
                htcondor_es.es.post_ads(es, idx, ad_list)
                total_upload += time.time() - st
                buffered_ads[idx] = []
            count += 1
            job_completion = job_ad.get("EnteredCurrentStatus")
            if job_completion > last_completion:
                last_completion = job_completion
            if time.time() - starttime > TIMEOUT_MINS*60:
                print "Crawler has been running for more than %d minutes; exiting." % TIMEOUT_MINS
                break
    except RuntimeError:
        print "Failed to query schedd for history:", schedd_ad["Name"]
    except Exception, e:
        print "Failure when processing schedd:", str(e)

    for idx, ad_list in buffered_ads.items():
        if ad_list:
            htcondor_es.es.post_ads(es, idx, ad_list)

    total_time = (time.time() - my_start) / 60.
    total_upload /= 60.
    print "Schedd %s total response count: %d; last completion %s; total query time %.2f min; total upload time %.2f min" % \
        (schedd_ad["Name"],
         count,
         datetime.datetime.fromtimestamp(last_completion).strftime("%Y-%m-%d %H:%M:%S"),
         total_time - total_upload,
         total_upload)

    try:
        checkpoint_new = json.load(open("checkpoint.json"))
    except:
        checkpoint_new = {}

    if (schedd_ad["Name"] not in checkpoint_new) or (checkpoint_new[schedd_ad["Name"]] < last_completion):
        checkpoint_new[schedd_ad["Name"]] = last_completion

    fd, tmpname = tempfile.mkstemp(dir=".", prefix="checkpoint.json.new")
    fd = os.fdopen(fd, "w")
    json.dump(checkpoint_new, fd)
    fd.close()
    os.rename(tmpname, "checkpoint.json")

    # Now that we have the fresh history, process the queues themselves.
    domain = socket.getfqdn().split(".", 1)[-1]
    if domain != 'cern.ch':
        process_schedd_queue(starttime, schedd_ad)
    return last_completion


def main():

    try:
        checkpoint = json.load(open("checkpoint.json"))
    except:
        checkpoint = {}

    starttime = time.time()

    pool = multiprocessing.Pool(processes=10)
    future = pool.apply_async(get_schedds)
    schedd_ads = future.get(TIMEOUT_MINS*60)
    print "There are %d schedds to query." % len(schedd_ads)

    futures = []
    future = pool.apply_async(process_collector)
    futures.append(('collector', future))

    for schedd_ad in schedd_ads:
        name = schedd_ad["Name"]
        #if name != "vocms0309.cern.ch": continue
        last_completion = checkpoint.get(name, 0)
        if name.startswith("crab") and last_completion == 0:
            last_completion = time.time()-12*3600
        future = pool.apply_async(process_schedd, (starttime, last_completion, schedd_ad))
        futures.append((name, future))
        #break

    pool.close()

    timed_out = False
    for name, future in futures:
        time_remaining = TIMEOUT_MINS*60+10 - (time.time() - starttime)
        if time_remaining > 0:
            try:
                last_completion = future.get(time_remaining)
                if name:
                    checkpoint[schedd_ad["name"]] = last_completion
            except multiprocessing.TimeoutError:
                print "Schedd %s timed out; ignoring progress." % name
        else:
            timed_out = True
            break
    if timed_out:
        pool.terminate()
    pool.join()


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

    print "Total processing time: %.2f mins" % ((time.time()-starttime)/60.)


if __name__ == "__main__":
    main()

