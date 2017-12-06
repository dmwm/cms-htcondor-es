#!/usr/bin/python

import os
import json
import sqlite3
import requests
import multiprocessing

blockreplicas_url = "https://cmsweb.cern.ch/phedex/datasvc/json/prod/blockreplicas"
nodes_url = "https://cmsweb.cern.ch/phedex/datasvc/json/prod/nodes"


_g_session = None
def get_session():
    global _g_session
    if _g_session is not None:
        return _g_session
    session = requests.Session()
    ca_dir = os.environ.get("X509_CERT_DIR", "/etc/grid-security/certificates")
    if os.path.exists(ca_dir):
        session.verify = ca_dir
    _g_session = session
    return session


def create_db(dbname = "popdb.sqlite"):
    conn = sqlite3.connect(dbname)
    conn.isolation_level = None
    return conn


def get_blockreplicas(node):
    print "Fetching block replicas for %s" % node
    session = get_session()
    blocks_json = session.get(blockreplicas_url, params={"complete": "y", "node": node}).json()
    curs = create_db().cursor()
    curs.execute("BEGIN")
    curs.execute("DELETE FROM disk_replicas WHERE site=?", (node,))
    for block in blocks_json['phedex']['block']:
        name = str(block['name'])
        size = block['bytes']
        curs.execute("INSERT INTO disk_replicas (block, site, size_bytes) VALUES (?, ?, ?)", (name, node, size))
    curs.execute("COMMIT")
    print "Finished block replicas for %s" % node


def list_all_nodes():
    session = get_session()
    nodes_json = session.get(nodes_url).json()
    for node in nodes_json['phedex']['node']:
        name = str(node['name'])
        if not (name.endswith("_MSS") or name.endswith("_Buffer") or name.endswith("_Export")):
            yield name


def main():
    pool = multiprocessing.Pool(1)

    nodes = list_all_nodes()

    dataset_replica_sizes = {}
    counter = 0
    node_counter = 0
    list(pool.imap_unordered(get_blockreplicas, nodes))
    pool.join()

if __name__ == '__main__':
    main()
