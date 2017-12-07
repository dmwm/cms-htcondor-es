#!/usr/bin/python

import re
import os
import sys
import json
import time
import sqlite3
import calendar
import datetime

from argparse import ArgumentParser

import elasticsearch
import elasticsearch.helpers

if os.path.exists("src"):
    sys.path.append("src")
from htcondor_es.es import get_server_handle


now = int(time.time())

def create_if_not_exist(curs, table_name, definition, index):
    curs.execute("BEGIN")
    curs.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name, ))
    if not curs.fetchone():
        curs.execute(definition)
        if index:
            curs.execute(index)
    curs.execute("COMMIT")

def create_db(dbname = "popdb.sqlite"):
    conn = sqlite3.connect(dbname)
    conn.isolation_level = None
    curs = conn.cursor()
    create_if_not_exist(curs, "dataset_popularity", "CREATE TABLE dataset_popularity (dataset text, datatier text, primaryds text, processedds text, date text, month integer, crab_task text, user text, read_bytes integer, cpu_hours real, core_hours real, committed_hours real, file_opens integer, events integer, job_count integer, has_events integer, success integer)",
        "CREATE UNIQUE INDEX IF NOT EXISTS dataset_popularity_index ON dataset_popularity (dataset, month, crab_task, has_events, success)")
    create_if_not_exist(curs, "dataset_size", "CREATE TABLE dataset_size (dataset text, size_bytes integer, events integer, creation_date integer)", "CREATE UNIQUE INDEX IF NOT EXISTS dataset_size_index ON dataset_size (dataset)")
    create_if_not_exist(curs, "disk_replicas", "CREATE TABLE disk_replicas (dataset text, site text, size_bytes integer)", "CREATE UNIQUE INDEX IF NOT EXISTS disk_replicas_index ON disk_replicas (dataset, site)")
    create_if_not_exist(curs, "index_progress", "CREATE TABLE index_progress (index_name text, completed integer)", None)
    return conn

popdb = {}

def record_usage(result, conn):
    result_doc = {}
    for key, val in result['_source'].items():
        result_doc[key] = val

    required_attrs = ['CMSPrimaryPrimaryDataset', 'CMSPrimaryProcessedDataset', 'CMSPrimaryDataTier', 'CRAB_UserHN', 'Workflow',
                      'CoreHr', 'CpuTimeHr', 'CommittedCoreHr']
    for attr in required_attrs:
        if attr not in result_doc:
            return

    dataset = '/%s/%s/%s' % (result_doc['CMSPrimaryPrimaryDataset'], result_doc['CMSPrimaryProcessedDataset'], result_doc['CMSPrimaryDataTier'])
    has_events = result_doc.get('ChirpCMSSWEvents', 0) > 0
    success = int(result_doc.get('ExitCode', 1)) == 0

    ts = time.strptime(result['_index'], "cms-%Y-%m-%d")
    date="%s-%s-%s" % (ts.tm_year, ts.tm_mon, ts.tm_mday)
    month = ts.tm_year*100 + ts.tm_mon

    key = (dataset, result_doc['CMSPrimaryDataTier'], result_doc['CMSPrimaryPrimaryDataset'], result_doc['CMSPrimaryProcessedDataset'], date, month, result_doc['Workflow'], result_doc['CRAB_UserHN'], has_events, success)
    read_bytes, cpu_hours, core_hours, committed_hours, file_opens, events, job_count = popdb.setdefault(key, (0, 0.0, 0.0, 0.0, 0, 0, 0))
    popdb[key] = (read_bytes + result_doc.get('ChirpCMSSWReadBytes', 0),
                  cpu_hours + result_doc['CpuTimeHr'],
                  core_hours + result_doc['CoreHr'],
                  committed_hours + result_doc['CommittedCoreHr'],
                  file_opens + result_doc.get('ChirpCMSSWFiles', 0),
                  events + result_doc.get('ChirpCMSSWEvents', 0),
                  job_count + 1)

    return


def record_db(conn):
    curs = conn.cursor()
    curs.execute("BEGIN")

    global popdb
    for key, val in popdb.items():
        dataset, datatier, primary, processed, date, month, workflow, user, has_events, success = key
        curs.execute("SELECT read_bytes, cpu_hours, core_hours, committed_hours, file_opens, events, job_count FROM dataset_popularity WHERE dataset=? AND month=? AND crab_task=? AND user=? AND has_events=? AND success=?", (dataset, month, workflow, user, has_events, success))
        row = curs.fetchone()

        if row:
            read_bytes, cpu_hours, core_hours, committed_hours, file_opens, events, job_count = row

            curs.execute("UPDATE dataset_popularity SET read_bytes=?, cpu_hours=?, core_hours=?, committed_hours=?, file_opens=?, events=?, job_count=? WHERE dataset=? AND month=? AND crab_task=? AND user=? AND has_events=? AND success=?", (read_bytes+val[0], cpu_hours+val[1], core_hours+val[2], committed_hours+val[3], file_opens+val[4], events+val[5], job_count+val[6], dataset, month, workflow, user, has_events, success))
        else:
            curs.execute("INSERT INTO dataset_popularity (dataset, datatier, primaryds, processedds, date, month, crab_task, user, read_bytes, cpu_hours, core_hours, committed_hours, file_opens, events, job_count, has_events, success) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (dataset, datatier, primary, processed, date, month, workflow, user, val[0], val[1], val[2], val[3], val[4], val[5], val[6], has_events, success))

    curs.execute("COMMIT")
    popdb = {}


def check_index(index, conn):
    curs = conn.cursor()
    curs.execute("BEGIN")
    # CREATE TABLE index_progress (index_name text, completed integer)
    curs.execute("SELECT completed FROM index_progress WHERE index_name=?", (index,))
    row = curs.fetchone()
    retval = 0
    if row:
        if row[0] > 0: # index was already done.
            retval = 0
        else: # Partial index.
            ts = time.strptime(index, "cms-%Y-%m-%d")
            month = ts.tm_year*100 + ts.tm_mon
            curs.execute("DELETE FROM dataset_popularity WHERE month=?", (month,))
            retval = 1
    else: # No indexing started.
        retval = 2
    curs.execute("COMMIT")
    return retval

def start_index(index, conn):
    curs = conn.cursor()
    curs.execute("BEGIN")
    curs.execute("INSERT INTO index_progress VALUES (?, ?)", (index, 0))
    curs.execute("COMMIT")

def reset_index(index, conn):
    curs = conn.cursor()
    curs.execute("BEGIN")
    curs.execute("DELETE FROM index_progress WHERE index_name=?", (index,))
    curs.execute("COMMIT")

def finish_index(index, conn):
    curs = conn.cursor()
    curs.execute("BEGIN")
    curs.execute("UPDATE index_progress SET completed=1 WHERE index_name=?", (index,))
    curs.execute("COMMIT")


def do_index(args, index, conn):
    now = time.time()

    print "Index wildcard:", index

    client = get_server_handle(args)

    fields = ['CMSPrimaryPrimaryDataset', 'CMSPrimaryProcessedDataset', 'CMSPrimaryDataTier', 'CRAB_UserHN', 'Workflow',
             'CoreHr', 'CpuTimeHr', 'CommittedCoreHr', 'ChirpCMSSWReadBytes', 'ChirpCMSSWEvents', 'ExitCode', 'ChirpCMSSWFiles']

    body = {}
    body['_source'] = fields
    body['query'] = {"term": {"Type": "analysis"}}

    count = 0
    retry = True
    while retry:
        retry = False
        try:
            for result in elasticsearch.helpers.scan(client.handle, query=body, doc_type="job", index=index, scroll="10s"):
                record_usage(result, conn)
                count += 1
                if count % 10000 == 0:
                    print "%s: Indexed %i records" % (time.ctime(), count)
                    record_db(conn)
        except Exception, ex:
            #raise
            retry = True
            print "Hit exception: ", str(ex), "Sleeping for 10s..."
            time.sleep(10)

    print "Modifications took %.1f minutes" % ((time.time()-now)/60.)


def main():

    parser = ArgumentParser()
    parser.add_argument("--es_hostname", default='es-cms.cern.ch',
                        type=str, dest="es_hostname",
                        help="Hostname of the elasticsearch instance to be used [default: %(default)s]")
    parser.add_argument("--es_port", default=9203,
                        type=int, dest="es_port",
                        help="Port of the elasticsearch instance to be used [default: %(default)d]")
    parser.add_argument("--es_index_template", default='cms',
                        type=str, dest="es_index_template",
                        help="Trunk of index pattern [default: %(default)s]")
    parser.add_argument("--log_dir", default='log/',
                        type=str, dest="log_dir",
                        help="Directory for logging information [default: %(default)s]")
    parser.add_argument("--log_level", default='WARNING',
                        type=str, dest="log_level",
                        help="Log level (CRITICAL/ERROR/WARNING/INFO/DEBUG) [default: %(default)s]")
    args = parser.parse_args()

    #for month in [(2016, 9), (2016, 10), (2016, 11), (2016, 12), (2017, 01), (2017, 02)]:
    #for month in [(2017, 02)]:
    conn = create_db()
    for month in [(2016, 9), (2016, 10), (2016, 11), (2016, 12), (2017, 1), (2017, 2), (2017, 3), (2017, 4), (2017, 5), (2017, 6), (2017, 7), (2017, 8), (2017, 9), (2017, 10), (2017, 11)]:
        days = calendar.monthrange(month[0], month[1])[-1]
        needs_month_scan = False
        for day in range(days):
            index = "cms-%02d-%02d-%02d" % (month[0], month[1], day+1)
            retval = check_index(index, conn)
            if retval == 1:
                needs_month_scan = True
                print "Month statistics were reset - will do full re-scan"
                break
        if needs_month_scan:
            for day in range(days):
                index = "cms-%02d-%02d-%02d" % (month[0], month[1], day+1)
                reset_index(index, conn)
        for day in range(days):
            index = "cms-%02d-%02d-%02d" % (month[0], month[1], day+1)
            retval = check_index(index, conn)
            if needs_month_scan or retval != 0:
                start_index(index, conn)
                do_index(args, index, conn)
                finish_index(index, conn)
            else:
                print "Skipping re-scan of index %s" % index
                continue
            if os.path.exists("stopfile"):
                print "Exiting due to presence of 'stopfile'"
                sys.exit(2)

if __name__ == '__main__':
    main()

