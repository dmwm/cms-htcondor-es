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


def get_schedds_factory(factory="gfactory-1.t2.ucsd.edu:9614"):
    coll = htcondor.Collector(factory)
    schedd_ads = coll.query(htcondor.AdTypes.Schedd, "true", projection=["MyAddress", "ScheddIpAddr", "Name"])
    return schedd_ads

def get_schedds():
    schedd_ads = get_schedds_factory("gfactory-1.t2.ucsd.edu:9614")
    schedd_ads += get_schedds_factory("glidein.grid.iu.edu")
    schedd_ads += get_schedds_factory("cmsgwms-factory.fnal.gov")
    schedd_ads += get_schedds_factory("vocms0305.cern.ch")
    schedd_ads += get_schedds_factory("vocms0342.cern.ch")
    random.shuffle(schedd_ads)
    return schedd_ads

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
            json_ad = htcondor_es.convert_to_json.convert_to_json(job_ad, cms=False)
            if not json_ad:
                continue
            idx = htcondor_es.es.get_index(job_ad["QDate"], template='glidein')
            if not idx.startswith("glidein"): raise Exception("Failed to correctly index.")
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
        checkpoint_new = json.load(open("checkpoint.factory.json"))
    except:
        checkpoint_new = {}

    if (schedd_ad["Name"] not in checkpoint_new) or (checkpoint_new[schedd_ad["Name"]] < last_completion):
        checkpoint_new[schedd_ad["Name"]] = last_completion

    fd, tmpname = tempfile.mkstemp(dir=".", prefix="checkpoint.factory.json.new")
    fd = os.fdopen(fd, "w")
    json.dump(checkpoint_new, fd)
    fd.close()
    os.rename(tmpname, "checkpoint.factory.json")

    return last_completion


def main():

    try:
        checkpoint = json.load(open("checkpoint.factory.json"))
    except:
        checkpoint = {}

    starttime = time.time()

    pool = multiprocessing.Pool(processes=10)
    future = pool.apply_async(get_schedds)
    schedd_ads = future.get(TIMEOUT_MINS*60)
    print "There are %d schedds to query." % len(schedd_ads)

    futures = []

    for schedd_ad in schedd_ads:
        name = schedd_ad["Name"]
        last_completion = checkpoint.get(name, 0)
        future = pool.apply_async(process_schedd, (starttime, last_completion, schedd_ad))
        futures.append((name, future))

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
        checkpoint_new = json.load(open("checkpoint.factory.json"))
    except:
        checkpoint_new = {}

    for key, val in checkpoint.items():
        if (key not in checkpoint_new) or (val > checkpoint_new[key]):
            checkpoint_new[key] = val

    fd, tmpname = tempfile.mkstemp(dir=".", prefix="checkpoint.factory.json.new")
    fd = os.fdopen(fd, "w")
    json.dump(checkpoint_new, fd)
    fd.close()
    os.rename(tmpname, "checkpoint.factory.json")

    print "Total processing time: %.2f mins" % ((time.time()-starttime)/60.)


if __name__ == "__main__":
    main()

