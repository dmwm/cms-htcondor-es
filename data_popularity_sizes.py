
import time
import sqlite3
import traceback
import multiprocessing

import dbs.apis.dbsClient

global_url = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
popdb = 'popdb.sqlite'

api = None

def fetch_summary(dataset):
    global api
    if not api:
        api = dbs.apis.dbsClient.DbsApi(url=global_url)
    try:
        summary = api.listBlockSummaries(dataset=dataset[0])[0]
        summary['dataset'] = dataset[0]
    except:
        traceback.print_exc()
        raise
    return summary

def main():
    conn = sqlite3.connect(popdb)
    conn.isolation_level = None
    curs = conn.cursor()

    pool = multiprocessing.Pool(4)

    iterable = [(row[0],) for row in curs.execute("select distinct(dp.dataset) from dataset_popularity as dp left outer join dataset_size as ds on (dp.dataset = ds.dataset) WHERE ds.size_bytes is null AND NOT (dp.dataset LIKE '%/USER')")]
    print "There are %i datasets left to query." % len(iterable)

    count = 0
    curs.execute("BEGIN")
    for info in pool.imap_unordered(fetch_summary, iterable):
        print info
        curs.execute("INSERT INTO dataset_size (dataset, size_bytes, events) VALUES (?, ?, ?)", (info['dataset'], info['file_size'], info['num_event']))
        count += 1
        if count % 50 == 0:
            print time.ctime(), "Finished", count, "dataset records"
            curs.execute("COMMIT")
            curs.execute("BEGIN")
    curs.execute("COMMIT")

    pool.close()
    pool.join()

if __name__ == '__main__':
    main()

