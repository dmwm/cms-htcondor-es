#!/usr/bin/python

import os
import sys
import json
import time
import classad
import htcondor

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


def main():
    schedd_query = classad.ExprTree('CMSGWMS_Type =?="prodschedd" && Name =!= "vocms001.cern.ch" && Name =!= "vocms047.cern.ch" && Name=!="vocms015.cern.ch"')
    coll = htcondor.Collector("vocms099.cern.ch")

    es = htcondor_es.es.get_server_handle()
    print htcondor_es.es.get_index(time.time())

    try:
        checkpoint = json.load(open("checkpoint2.json"))
    except:
        checkpoint = {}

    starttime = time.time()
    buffered_ads = {}
    for schedd_ad in coll.query(htcondor.AdTypes.Schedd, schedd_query, projection=["MyAddress", "ScheddIpAddr", "Name"]):
        schedd = htcondor.Schedd(schedd_ad)
        last_completion = int(checkpoint.get(schedd_ad["Name"], 0))
        history_query = classad.ExprTree("CompletionDate >= %d" % last_completion)
        if time.time() - starttime > 9*60:
            print "Crawler has been running for more than 9 minutes; exiting."
            break
        print "Querying %s for history." % schedd_ad["Name"]
        count = 0
        try:
            history_iter = schedd.history(history_query, [], -1)
            json_ad = '{}'
            for job_ad in history_iter:
                json_ad = htcondor_es.convert_to_json.convert_to_json(job_ad)
                idx = htcondor_es.es.get_index(job_ad["QDate"])
                ad_list = buffered_ads.setdefault(idx, [])
                ad_list.append((job_ad["GlobalJobId"], json_ad))
                if len(ad_list) == 250:
                    htcondor_es.es.post_ads(es, idx, ad_list)
                    buffered_ads[idx] = []
                #print es.index(index=idx, doc_type="job", body=json_ad, id=job_ad["GlobalJobId"])
                count += 1
                job_completion = job_ad.get("CompletionDate")
                if job_completion > last_completion:
                    last_completion = job_completion
                if time.time() - starttime > 9*60:
                    print "Crawler has been running for more than 9 minutes; exiting."
                    break
            print "Sample ad for", job_ad["GlobalJobId"]
            json_ad = json.loads(json_ad)
            keys = json_ad.keys()
            keys.sort()
            for key in keys:
                print key, "=", json_ad[key]
        except RuntimeError:
            print "Failed to query schedd for history:", schedd_ad["Name"]
            continue

        for idx, ad_list in buffered_ads.items():
            if ad_list:
                htcondor_es.es.post_ads(es, idx, ad_list)
        buffered_ads.clear()

        print "Schedd total response count:", count
        checkpoint[schedd_ad["Name"]] = last_completion
        fd = open("checkpoint.json.new", "w")
        json.dump(checkpoint, fd)
        fd.close()
        os.rename("checkpoint.json.new", "checkpoint.json")

        if time.time() - starttime > 9*60:
            break

    fd = open("checkpoint2.json.new", "w")
    json.dump(checkpoint, fd)
    fd.close()
    os.rename("checkpoint2.json.new", "checkpoint2.json")


if __name__ == "__main__":
    main()

