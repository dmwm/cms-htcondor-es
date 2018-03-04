#!/usr/bin/python

import os
import json
import requests
import multiprocessing

subscriptions_url = "https://cmsweb.cern.ch/phedex/datasvc/json/prod/subscriptions"
blockreplicas_url = "https://cmsweb.cern.ch/phedex/datasvc/json/prod/blockreplicas"
nodes_url = "https://cmsweb.cern.ch/phedex/datasvc/json/prod/nodes"

eu_countries = ['AT', 'BE', 'CH', 'DE', 'ES', 'FR', 'IT', 'UK']

def get_session():
    session = requests.Session()
    ca_dir = os.environ.get("X509_CERT_DIR", "/etc/grid-security/certificates")
    if os.path.exists(ca_dir):
        session.verify = ca_dir

    return session


def list_all_nodes(session):
    nodes_json = session.get(nodes_url).json()
    for node in nodes_json['phedex']['node']:
        yield str(node['name'])


def get_node_subscriptions_cache(node):
    with get_session() as session:
        for entry in get_node_subscriptions(node, session):
            pass
    print "Done with subscription cache-loading."


def get_node_blockreplicas_cache(node):
    with get_session() as session:
        for entry in get_node_blockreplicas(node, session):
            pass
    print "Done with block replicas cache-loading."

def get_node_subscriptions(node, session = None):
    cache_file = "tmp/%s.json" % node
    print "Getting node subscriptions for", node
    if os.path.exists(cache_file):
        print "(using cache file for node %s subscription request)" % node
        with open(cache_file, "r") as fp:
            subscriptions_json = json.load(fp)
    else:
        subscriptions_json = session.get(subscriptions_url, params={"node": node}).text
        with open(cache_file, "w") as fp:
            fp.write(subscriptions_json)
        subscriptions_json = json.loads(subscriptions_json)
    for dataset in subscriptions_json['phedex']['dataset']:
        yield str(dataset['name']), dataset['bytes']


def get_node_blockreplicas(node, session = None):
    cache_file = "tmp/blockreplicas_%s.json" % node
    print "Getting node block replicas for", node
    if os.path.exists(cache_file):
        print "(using cache file for node %s block replicas request)" % node
        with open(cache_file, "r") as fp:
            blocks_json = json.load(fp)
    else:
        blocks_json = session.get(blockreplicas_url, params={"node": node}).text
        with open(cache_file, "w") as fp:
            fp.write(blocks_json)
        blocks_json = json.loads(blocks_json)
    for dataset in blocks_json['phedex']['block']:
        yield str(dataset['name']), dataset['bytes']


def main():
    with get_session() as session:
        nodes = list(list_all_nodes(session))
    nodes.sort()

    total_disk_miniaod_bytes = {}
    total_us_miniaod_bytes = {}
    total_eu_miniaod_bytes = {}
    total_euus_miniaod_bytes = {}
    filtered_nodes = []
    for node in nodes:
        if ('_Buffer' in node) or ('_MSS' in node):
            print "Skipping non-disk node", node
            continue
        if node.startswith("T3_"):
            print "Skipping T3 site", node
            continue
        print "Querying node %s for subscription/block data" % node
        filtered_nodes.append(node)

    pool = multiprocessing.Pool(5)
    pool.map(get_node_subscriptions_cache, filtered_nodes)
    pool.map(get_node_blockreplicas_cache, filtered_nodes)
    print "Done with map"

    for node in filtered_nodes:
        #sub_iter = get_node_subscriptions(node, session)
        sub_iter = get_node_blockreplicas(node, session)
        is_us = '_US_' in node
        if is_us:
            print "(US regional site)"
        is_eu = False
        for country in eu_countries:
            if ('_%s_' % country) in node:
                is_eu = True
                break
        skip_count = 0
        count = 0
        for name, replica_size in sub_iter:
            count += 1
            tier = name.split("/")[-1]
            if not tier.startswith("MINIAOD"):
            #if 'AOD' not in tier:
                skip_count += 1
                continue
            prior_size = total_disk_miniaod_bytes.get(name, 0)
            dataset_size = max(replica_size, prior_size)
            total_disk_miniaod_bytes[name] = dataset_size
            if is_us:
                total_us_miniaod_bytes[name] = dataset_size
            if is_eu:
                total_eu_miniaod_bytes[name] = dataset_size
            if is_us or is_eu:
                total_euus_miniaod_bytes[name] = dataset_size
        print "Total MINIAOD subscriptions processed", count
        print "Total subscriptions skipped", skip_count

    print "Total dataset size: %.2f" % (sum(total_disk_miniaod_bytes.values())*1e-12)
    print "Total US dataset size: %.2f" % (sum(total_us_miniaod_bytes.values())*1e-12)
    print "Total EU dataset size: %.2f" % (sum(total_eu_miniaod_bytes.values())*1e-12)
    print "Total EU/US dataset size: %.2f" % (sum(total_euus_miniaod_bytes.values())*1e-12)


if __name__ == '__main__':
    main()

