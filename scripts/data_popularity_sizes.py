import time
import sqlite3
import traceback
import multiprocessing

import dbs.apis.dbsClient

global_url = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
popdb = "popdb.sqlite"

api = None


def fetch_summary(dataset):
    global api
    if dataset[0].startswith("/PhEDEx_Debug/"):
        print("Skipping debug dataset", dataset[0])
        return None
    if not api:
        api = dbs.apis.dbsClient.DbsApi(url=global_url)
    try:
        summary = api.listBlockSummaries(dataset=dataset[0])[0]
        summary["dataset"] = dataset[0]
    except:
        print(dataset[0])
        traceback.print_exc()
        raise
    try:
        dataset_info = api.listDatasets(dataset=dataset[0], detail=True)
    except:
        print(dataset[0])
        traceback.print_exc()
        raise
    if dataset_info:
        summary["creation_date"] = dataset_info[0]["creation_date"]
    else:
        # print "WARNING: no dataset returned for %s; checking production datasets" % dataset[0]
        dataset_info = api.listDatasets(
            dataset=dataset[0], detail=True, dataset_access_type="PRODUCTION"
        )
        if dataset_info:
            summary["creation_date"] = dataset_info[0]["creation_date"]
        else:
            # print "WARNING: no dataset returned for %s; checking invalid datasets" % dataset[0]
            dataset_info = api.listDatasets(
                dataset=dataset[0], detail=True, dataset_access_type="INVALID"
            )
            if dataset_info:
                summary["creation_date"] = dataset_info[0]["creation_date"]
            else:
                print(
                    "ERROR: dataset %s is not INVALID, VALID or in PRODUCTION"
                    % dataset[0]
                )
                summary["creation_date"] = 0
    return summary


def main():
    conn = sqlite3.connect(popdb)
    conn.isolation_level = None
    curs = conn.cursor()

    pool = multiprocessing.Pool(4)

    iterable = [
        (row[0],)
        for row in curs.execute(
            "select distinct(dp.dataset) from dataset_popularity as dp left outer join dataset_size as ds on (dp.dataset = ds.dataset) WHERE ds.size_bytes is null AND NOT (dp.dataset LIKE '%/USER')"
        )
    ]
    print("There are %i datasets left to query based on CRAB jobs." % len(iterable))
    query_datasets(curs, pool, iterable)

    # iterable = [(row[0],) for row in curs.execute("select distinct(dr.dataset) from disk_replicas AS dr LEFT OUTER JOIN dataset_size AS ds on (dr.dataset = ds.dataset) WHERE (ds.size_bytes IS null OR ds.creation_date is 0) AND NOT (dr.dataset LIKE '%/USER')")]
    iterable = [
        (row[0],)
        for row in curs.execute(
            "select distinct(dr.dataset) from disk_replicas AS dr LEFT OUTER JOIN dataset_size AS ds on (dr.dataset = ds.dataset) WHERE (ds.size_bytes IS null) AND NOT (dr.dataset LIKE '%/USER')"
        )
    ]
    print("There are %i datasets left to query based on replicas." % len(iterable))
    query_datasets(curs, pool, iterable)

    pool.close()
    pool.join()


def query_datasets(curs, pool, iterable):
    count = 0
    curs.execute("BEGIN")
    for info in pool.imap_unordered(fetch_summary, iterable):
        if not info:
            continue
        # print info
        curs.execute(
            "INSERT INTO dataset_size (dataset, size_bytes, events, creation_date) VALUES (?, ?, ?, ?)",
            (
                info["dataset"],
                info["file_size"],
                info["num_event"],
                info["creation_date"],
            ),
        )
        count += 1
        if count % 50 == 0:
            print(time.ctime(), "Finished", count, "dataset records")
            curs.execute("COMMIT")
            curs.execute("BEGIN")
    curs.execute("COMMIT")


if __name__ == "__main__":
    main()
