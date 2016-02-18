#!/usr/bin/python

import os
import sys
import json
import time
import random
import classad
import htcondor
import multiprocessing

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

TIMEOUT_MINS = 4

def get_schedds():
    #schedd_query = classad.ExprTree('CMSGWMS_Type =?="prodschedd" && Name =!= "vocms001.cern.ch" && Name =!= "vocms047.cern.ch" && Name=!="vocms015.cern.ch"')
    schedd_query = classad.ExprTree('!isUndefined(CMSGWMS_Type)')
    coll = htcondor.Collector("cmssrv221.fnal.gov:9620")
    schedd_ads = coll.query(htcondor.AdTypes.Schedd, schedd_query, projection=["MyAddress", "ScheddIpAddr", "Name"])
    random.shuffle(schedd_ads)
    return schedd_ads


def clean_old_jobs(name, es):
    collection_date = htcondor_es.convert_to_json.get_data_collection_time()
    idx = htcondor_es.es.get_index(time.time())
    body = {"query": {
      "bool": {
        "must" : [
                  {"term": {"Status": "running"}},
                  {"match": {"ScheddName": name}},
                  {"range": {"DataCollection": {"lt": collection_date-600}}},
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
        results = es.search(index="_all", size=200, body=body, _source=False)['hits']['hits']
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


def process_schedd(starttime, schedd_ad):
    my_start = time.time()
    print "Querying %s for jobs." % schedd_ad["Name"]
    buffered_ads = {}
    schedd = htcondor.Schedd(schedd_ad)
    if time.time() - starttime > TIMEOUT_MINS*60:
        print "Crawler has been running for more than %d minutes; exiting." % TIMEOUT_MINS
        return
    count = 0
    total_upload = 0
    try:
        es = htcondor_es.es.get_server_handle()
        query_iter = schedd.xquery()
        json_ad = '{}'
        for job_ad in query_iter:
            #print "Processing ad %s." % job_ad.get("GlobalJobId")
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
            #print es.index(index=idx, doc_type="job", body=json_ad, id=job_ad["GlobalJobId"])
            count += 1
            if time.time() - starttime > TIMEOUT_MINS*60:
                print "Crawler has been running for more than %d minutes; exiting." % TIMEOUT_MINS
                break
        #print "Sample ad for", job_ad["GlobalJobId"]
        #json_ad = json.loads(json_ad)
        #keys = json_ad.keys()
        #keys.sort()
        #for key in keys:
        #    print key, "=", json_ad[key]
    except RuntimeError:
        print "Failed to query schedd for jobs:", schedd_ad["Name"]
    except Exception, e:
        print "Failure when processing schedd query:", str(e)

    for idx, ad_list in buffered_ads.items():
        if ad_list:
            htcondor_es.es.post_ads(es, idx, ad_list)
    buffered_ads.clear()

    total_time = (time.time() - my_start)/60.
    total_upload = total_upload / 60.
    print "Schedd %s total response count: %d; total query time %.2f min; total upload time %.2f min" % (schedd_ad["Name"], count, total_time-total_upload, total_upload)
    clean_old_jobs(schedd_ad["Name"], es)

def main():

    starttime = time.time()

    pool = multiprocessing.Pool(processes=8)
    future = pool.apply_async(get_schedds)
    schedd_ads = future.get(TIMEOUT_MINS*60)
    print "There are %d schedds to query." % len(schedd_ads)

    futures = []
    for schedd_ad in schedd_ads:
        future = pool.apply_async(process_schedd, (starttime, schedd_ad))
        futures.append(future)

    timed_out = False
    for future in futures:
        time_remaining = TIMEOUT_MINS*60+10 - (time.time() - starttime)
        if time_remaining > 0:
            future.wait(time_remaining)
        else:
            timed_out = True
            break
    if timed_out:
        pool.terminate()
    else:
        pool.close()
    pool.join()

    print "Total runtime: %.2f minutes" % ((time.time()-starttime)/60.)


if __name__ == "__main__":
    main()

