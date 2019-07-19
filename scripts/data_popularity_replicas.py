#!/usr/bin/python

import os
import json
import ijson
import collections
import sqlite3
import requests
import multiprocessing

blockreplicas_url = "https://cmsweb.cern.ch/phedex/datasvc/json/prod/blockreplicas"
nodes_url = "https://cmsweb.cern.ch/phedex/datasvc/json/prod/nodes"


class ResponseFileAdaptor(object):
    """
    Given a requests Response object, provide a file-like adaptor.

    Inspired by this solution:
    https://stackoverflow.com/questions/12593576/adapt-an-iterator-to-behave-like-a-file-like-object-in-python
    """

    def __init__(self, response):
        self._iter = response.iter_content(chunk_size=8192)
        self._next_chunks = []
        self._bytes_ready = 0

    def _consume(self):
        next_chunk = next(self._iter)
        self._bytes_ready += len(next_chunk)
        self._next_chunks.append(next_chunk)

    def read(self, n):
        if (self._next_chunks is None) or (n == 0):
            return ""
        try:
            while self._bytes_ready < n:
                self._consume()
            result = "".join(self._next_chunks[:-1])
            bytes_remaining = n - len(result)
            result += self._next_chunks[-1][:bytes_remaining]
            remaining_chunk = self._next_chunks[-1][bytes_remaining:]
            if remaining_chunk:
                self._next_chunks = [remaining_chunk]
                self._bytes_ready = len(remaining_chunk)
            else:
                self._next_chunks = []
                self._bytes_ready = 0
            assert len(result) == n
            return result
        except StopIteration:
            result = "".join(self._next_chunks)
            self._next_chunks = None
            assert len(result) < n
            return result


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


def create_db(dbname="popdb.sqlite"):
    conn = sqlite3.connect(dbname)
    conn.isolation_level = None
    return conn


def get_blockreplicas(node):
    print("Fetching block replicas for %s" % node)
    session = get_session()
    response = session.get(blockreplicas_url, params={"complete": "y", "node": node})
    response_fp = ResponseFileAdaptor(response)
    replicas = collections.defaultdict(int)
    block_counter = 0
    for block in ijson.items(response_fp, "phedex.block.item"):
        block_counter += 1
        name = str(block["name"]).split("#")[0]
        size = block["bytes"]
        replicas[name] += size
    print(
        "Total of %d block replicas in %d datasets returned for %s"
        % (block_counter, len(replicas), node)
    )
    curs = create_db().cursor()
    curs.execute("BEGIN")
    curs.execute("DELETE FROM disk_replicas WHERE site=?", (node,))
    for name, size in list(replicas.items()):
        curs.execute(
            "INSERT INTO disk_replicas (dataset, site, size_bytes) VALUES (?, ?, ?)",
            (name, node, size),
        )
    curs.execute("COMMIT")
    print("Finished block replicas for %s" % node)


def list_all_nodes():
    session = get_session()
    nodes_json = session.get(nodes_url).json()
    for node in nodes_json["phedex"]["node"]:
        name = str(node["name"])
        if not (
            name.endswith("_MSS")
            or name.endswith("_Buffer")
            or name.endswith("_Export")
        ):
            yield name


def main():
    nodes = sorted(list_all_nodes())

    if False:  # Left to help with easier debugging of get_blockreplicas
        for node in nodes:
            get_blockreplicas(node)
    else:
        pool = multiprocessing.Pool(5)
        pool.imap_unordered(get_blockreplicas, nodes)
        pool.close()
        pool.join()


if __name__ == "__main__":
    main()
