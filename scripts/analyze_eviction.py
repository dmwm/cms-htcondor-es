import sys
import sqlite3

conn = sqlite3.connect("popdb.sqlite")
curs = conn.cursor()

tier = "/%s" % sys.argv[1]
eviction_months = 3

datasets = {}
for row in curs.execute("select dataset, size_bytes, events from dataset_size"):
    datasets[row[0]] = {"size": row[1], "unique_events": row[2]}

for row in curs.execute(
    "select core_hours, events, read_bytes, dataset, crab_task, month from dataset_popularity"
):
    if row[3].endswith("/USER"):
        continue
    if not row[3].endswith(tier):
        continue
    info = datasets[row[3]]
    core_hours = info.setdefault("core_hours", 0)
    events = info.setdefault("events", 0)
    read_bytes = info.setdefault("read_bytes", 0)
    info.setdefault("crab_tasks", set()).add(row[4])
    info["core_hours"] = core_hours + row[0]
    info["events"] = events + row[1]
    info["read_bytes"] = read_bytes + row[2]
    monthly = info.setdefault(
        "monthly",
        {
            201609: False,
            201610: False,
            201611: False,
            201612: False,
            201701: False,
            201702: False,
        },
    )
    monthly[row[5]] = True

months = [201609, 201610, 201611, 201612, 201701, 201702]
eviction_bytes = 0
evicted_datasets = 0
fetch_bytes = 0
fetched_datasets = 0
for dataset, info in list(datasets.items()):
    if "monthly" not in info:
        continue
    unused_count = 0
    first_use = False
    for month in months:
        if not first_use:
            if info["monthly"][month]:
                first_use = True
            continue
        if not info["monthly"][month]:
            unused_count += 1
            if unused_count == eviction_months:
                eviction_bytes += info["size"]
                evicted_datasets += 1
        elif unused_count >= eviction_months:
            fetch_bytes += info["size"]
            unused_count = 0
            fetched_datasets += 1

print(
    "Eviction policy of %d months would have resulted in %.2f TB (%d datasets) evicted from disk and %.2fTB (%d datasets) fetched from tape."
    % (
        eviction_months,
        eviction_bytes / 1e12,
        evicted_datasets,
        fetch_bytes / 1e12,
        fetched_datasets,
    )
)
