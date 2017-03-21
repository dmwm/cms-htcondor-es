#!/usr/bin/python

import re
import os
import sys
import json
import time
import sqlite3
import calendar
import datetime

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
    create_if_not_exist(curs, "dataset_popularity", "CREATE TABLE dataset_popularity (dataset text, datatier text, primaryds text, processedds text, date text, month integer, crab_task text, user text, read_bytes integer, cpu_hours real, core_hours real, committed_hours real, events integer, job_count integer, has_events integer, success integer)",
        "CREATE UNIQUE INDEX IF NOT EXISTS dataset_popularity_index ON dataset_popularity (dataset, month, crab_task, has_events, success)")
    create_if_not_exist(curs, "dataset_size", "CREATE TABLE dataset_size (dataset text, size_bytes integer, events integer)", None)
    return conn

popdb = {}

def record_usage(result, conn):
    result_doc = {}
    for key, val in result['fields'].items():
        result_doc[key] = val[0]

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
    read_bytes, cpu_hours, core_hours, committed_hours, events, job_count = popdb.setdefault(key, (0, 0.0, 0.0, 0.0, 0, 0))
    popdb[key] = (read_bytes+result_doc.get('ChirpCMSSWReadBytes', 0), cpu_hours+result_doc['CpuTimeHr'], core_hours+result_doc['CoreHr'], committed_hours+result_doc['CommittedCoreHr'], events+result_doc.get('ChirpCMSSWEvents', 0), job_count+1)

    return


def record_db(conn):
    curs = conn.cursor()
    curs.execute("BEGIN")

    global popdb
    for key, val in popdb.items():
        dataset, datatier, primary, processed, date, month, workflow, user, has_events, success = key
        curs.execute("SELECT read_bytes, cpu_hours, core_hours, committed_hours, events, job_count FROM dataset_popularity WHERE dataset=? AND month=? AND crab_task=? AND user=? AND has_events=? AND success=?", (dataset, month, workflow, user, has_events, success))
        row = curs.fetchone()

        if row:
            read_bytes, cpu_hours, core_hours, committed_hours, events, job_count = row

            curs.execute("UPDATE dataset_popularity SET read_bytes=?, cpu_hours=?, core_hours=?, committed_hours=?, events=?, job_count=? WHERE dataset=? AND month=? AND crab_task=? AND user=? AND has_events=? AND success=?", (read_bytes+val[0], cpu_hours+val[1], core_hours+val[2], committed_hours+val[3], events+val[4], job_count+1, dataset, month, workflow, user, has_events, success))
        else:
            curs.execute("INSERT INTO dataset_popularity (dataset, datatier, primaryds, processedds, date, month, crab_task, user, read_bytes, cpu_hours, core_hours, committed_hours, events, job_count, has_events, success) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (dataset, datatier, primary, processed, date, month, workflow, user, val[0], val[1], val[2], val[3], val[4], 1, has_events, success))

    curs.execute("COMMIT")
    popdb = {}


def do_index(index):
    now = time.time()

    print "Index wildcard:", index

    client = get_server_handle()

    fields = ['CMSPrimaryPrimaryDataset', 'CMSPrimaryProcessedDataset', 'CMSPrimaryDataTier', 'CRAB_UserHN', 'Workflow',
             'CoreHr', 'CpuTimeHr', 'CommittedCoreHr', 'ChirpCMSSWReadBytes', 'ChirpCMSSWEvents', 'ExitCode']

    body = {}
    body['fields'] = fields
    body['query'] = {"term": {"Type": "analysis"}}

    conn = create_db()

    count = 0
    retry = True
    while retry:
        retry = False
        try:
            for result in elasticsearch.helpers.scan(client, query=body, doc_type="job", index=index, scroll="10s"):
                record_usage(result, conn)
                count += 1
                if count % 10000 == 0:
                    print "%s: Indexed %i records" % (time.ctime(), count)
                    record_db(conn)
        except Exception, ex:
            print ex
            retry = True
            time.sleep(10)

    print "Modifications took %.1f minutes" % ((time.time()-now)/60.)


def main():
    #for month in [(2016, 9), (2016, 10), (2016, 11), (2016, 12), (2017, 01), (2017, 02)]:
    for month in [(2017, 02)]:
        days = calendar.monthrange(month[0], month[1])[-1]
        for day in range(days):
            do_index("cms-%02d-%02d-%02d" % (month[0], month[1], day+1))

if __name__ == '__main__':
    main()

