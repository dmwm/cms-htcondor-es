#!/usr/bin/env python
# Author: Ceyhun Uzunoglu <ceyhunuzngl AT gmail DOT com>
# Debug script with required utils to query condor schedds. Created for personal use.

# Read main function documentation for more explanations.
#   Be careful, it does not include any default limit/restriction, so put limits as much as possible. Don't cut yourself ;)


import json
import sys
import time

import classad
import click
import htcondor


@click.command()
@click.option("--collectors", default=[], help="collectors json")
@click.option("--projection", default="Name",
              help="comma seperated return fields) of schedd results. I,e,: MyAddress,ScheddIpAddr,Name")
@click.option("--schedd-filter", default=None, help="Schedd filter, i.e. xxx.cern.ch,yyy.cern.ch")
@click.option("--get-schedds", is_flag=True, help="get schedds")
@click.option("--nmins", default=12, type=int, help="Query last N minutes")
@click.option("--out-format", default=None, help="Query output format. Default dict, 'raw' prints ClassAd format")
@click.option("--limit", default=1, help="Query output limit")
@click.option("--query-const", default=None, help="Condor query constraint")
@click.option("--query-q", is_flag=True, help="get queue results")
@click.option("--query-h", is_flag=True, help="get history queue results")
def main(collectors, projection, schedd_filter, get_schedds, nmins, out_format, limit, query_const, query_q, query_h):
    """
    Debug script with required utils. Created for personal use.
    Be careful, it does not include any default limit/restriction, so put limits as much as possible. Don't cut yourself ;)

    \b
    Requirements:
      - collectors json file. Format: {"Volunteer":["xxxx.cern.ch"], "ITB":["yyyy.cern.ch"]}

    \b
    Usage:
    # Get all schedd names of provided collectors
        - python debugcli.py --collectors ../etc/collectors.json --get-schedds
    # Get all schedds with extra info
        - python debugcli.py --collectors ../etc/collectors.json --get-schedds --projection "Name,MyAddress"
    # Query queue of vomcs0828 shcedd for last 12 minutes data and get 1 document in raw(ClassAd) format. Default output format is dict
        - python debugcli.py --collectors ../etc/collectors.json --schedd-filter "vocms0828.cern.ch" --query-q --nmins 12 --out-format raw --limit 1
    # Query queue with 'query-const' constraint.
        - python debugcli.py --collectors ../etc/collectors.json --schedd-filter "crab3@vocms0155.cern.ch" --query-q --nmins 12 --limit 1 --query-const 'crab_workflow=="xxxxxxxx"'
    # Query queue with multiple condor constraints
        - python debugcli.py --collectors ../etc/collectors.json --schedd-filter "crab3@vocms0155.cern.ch" --query-q --nmins 12 --limit 1 --query-const 'crab_workflow=="xx" && GlobalJobId=="xxx@xxx.cern.ch#111.0#111"'
    # Query history queue with multiple condor constraints
        - python debugcli.py --collectors ../etc/collectors.json --schedd-filter "crab3@vocms0155.cern.ch" --query-h --nmins 12 --limit 1 --query-const 'crab_workflow=="xx" && GlobalJobId=="xxx@xxx.cern.ch#111.0#111"'
    """
    if "," in projection:
        projection = [x.strip() for x in projection.split(',')]
    else:
        projection = [projection]

    if get_schedds:
        print(debug_get_schedds_from_file(collectors, projection, schedd_filter))
    elif query_q:
        query_queue(collectors, nmins, schedd_filter, out_format, limit, query_const)
    elif query_h:
        query_hist(collectors, nmins, schedd_filter, out_format, limit, query_const)


def debug_get_schedds(collectors=None, projection=None, schedd_filter=None):
    """Return a list of schedd ads representing all the schedds in the pool.

    Example usage: debug_get_schedds(["x.cern.ch"], ["Name"])
    """
    if collectors is None:
        print("Please give list of collector hosts")
        sys.exit(0)
    if projection is None:
        # projection = ["MyAddress", "ScheddIpAddr", "Name"]
        projection = ["Name"]
    print("Projections:", projection)
    schedd_query = classad.ExprTree("!isUndefined(CMSGWMS_Type)")
    schedds_list = []
    for host in collectors:
        try:
            coll = htcondor.Collector(host)
            _schedds = [dict(x.items()) for x in
                        coll.query(htcondor.AdTypes.Schedd, schedd_query, projection=projection)]
            # Filter and return only selected schedds
            if schedd_filter:
                _schedds = [s for s in _schedds if s["Name"] in schedd_filter.split(",")]
            schedds_list.extend(_schedds)
        except Exception as e:
            print("Could not query schedd. I don't know, probably collector has a problem. More: ", str(e))
    return schedds_list


def debug_get_schedds_from_file(collectors_file, projection, schedd_filter):
    """Return a list of schedd ads representing all the schedds in the pool.

    Example usage: debug_get_schedds_from_file(collectors.json)
    $ cat collectors.json
        { "ITB": ["cmsgwms-collector-itb.cern.ch"] }
    """
    schedds = []
    with open(collectors_file) as f:
        pools = json.loads(f.read())
    for pool in pools:
        print(pool)
        _pool_schedds = debug_get_schedds(collectors=pools[pool], projection=projection, schedd_filter=schedd_filter)
        schedds.extend(_pool_schedds)
    # Make schedd list unique since a schedd can be on more than 1 collector
    schedd_ads = {}
    for schedd in schedds:
        try:
            schedd_ads[schedd["Name"]] = schedd
        except KeyError:
            pass
    return schedd_ads.values()


def query_queue(collectors_file, nmins, schedd_filter, out_format, limit, query_const):
    """Query queue

    query_const: crab_workflow=="200626_131321:kpenaloc_crab_WJets_1_v3"
    """
    if not nmins:
        nmins = 12
    print("Last N minutes:", nmins)
    start_time = time.time()
    _completed_since = start_time - (nmins + 1) * 60

    query_const = "&& (" + query_const + ")" if query_const else ""
    query = """
            (JobUniverse == 5) && (CMS_Type != "DONOTMONIT")
             &&
             (
                 JobStatus < 3 || JobStatus > 4
                 || EnteredCurrentStatus >= %(completed_since)d
                 || CRAB_PostJobLastUpdate >= %(completed_since)d
             )%(query_const)s
             """ % \
            {"completed_since": _completed_since, "query_const": query_const}
    print("Query:", query)
    schedd_ad_list = debug_get_schedds_from_file(collectors_file, ["MyAddress", "ScheddIpAddr", "Name"], schedd_filter)
    print("Schedds list:", schedd_ad_list)
    for schedd_ad in schedd_ad_list:
        schedd = htcondor.Schedd(classad.ClassAd(schedd_ad))
        query_iter = schedd.xquery(requirements=query, limit=limit)
        for job_ad in query_iter:
            print('-' * 40)
            if out_format == "raw":
                print(job_ad)
            else:
                print(dict(job_ad))


def query_hist(collectors_file, nmins, schedd_filter, out_format, limit, query_const):
    """Query history queue

    query_const: crab_workflow=="200626_131321:kpenaloc_crab_WJets_1_v3"
    """
    if not nmins:
        nmins = 12
    print("Last N minutes:", nmins)
    start_time = time.time()
    _completed_since = start_time - (nmins + 1) * 60

    query_const = " && (" + query_const + ")" if query_const else ""
    query = """
        (JobUniverse == 5) && (CMS_Type != "DONOTMONIT")
        &&
        (
            EnteredCurrentStatus >= %(last_completion)d
            || CRAB_PostJobLastUpdate >= %(last_completion)d
        )%(query_const)s
        """ % \
            {"last_completion": _completed_since, "query_const": query_const}
    print("Query:", query)
    schedd_ad_list = debug_get_schedds_from_file(collectors_file, ["MyAddress", "ScheddIpAddr", "Name"], schedd_filter)
    print("Schedd list:", schedd_ad_list)
    for schedd_ad in schedd_ad_list:
        schedd = htcondor.Schedd(classad.ClassAd(schedd_ad))
        query_iter = schedd.history(query, [], match=limit)
        for job_ad in query_iter:
            print('-' * 40)
            if out_format == "raw":
                print(job_ad)
            else:
                print(dict(job_ad))


if __name__ == "__main__":
    main()
