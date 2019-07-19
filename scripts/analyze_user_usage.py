import sys
import sqlite3

conn = sqlite3.connect("popdb.sqlite")
curs = conn.cursor()

tier = "/%s" % sys.argv[1]

datasets = {}
for row in curs.execute("select dataset, size_bytes, events from dataset_size"):
    datasets[row[0]] = {"size": row[1], "unique_events": row[2]}

for row in curs.execute(
    "select core_hours, events, read_bytes, dataset, user from dataset_popularity"
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

by_crab_tasks = {}
for dataset, info in list(datasets.items()):
    if "crab_tasks" not in info:
        continue
    num_tasks = len(info["crab_tasks"])
    # print info
    cinfo = by_crab_tasks.setdefault(num_tasks, {})
    tot_bytes = cinfo.setdefault("bytes", 0)
    cinfo["bytes"] = tot_bytes + info["size"]
    tot_hours = cinfo.setdefault("core_hours", 0)
    cinfo["core_hours"] = tot_hours + info["core_hours"]
    read_bytes = cinfo.setdefault("read_bytes", 0)
    cinfo["read_bytes"] = read_bytes + info["read_bytes"]
    events = cinfo.setdefault("events", 0)
    cinfo["events"] = events + info["events"]

print("Number of CRAB3 tasks:")
total_hours = 0
for i in range(1, 15):
    hours = (
        by_crab_tasks.setdefault(
            i, {"events": 0, "core_hours": 0, "read_bytes": 0, "bytes": 0}
        )["core_hours"]
        / 1e6
    )
    total_hours += hours
    print(
        i,
        "%.1fTB in dataset" % (by_crab_tasks[i]["bytes"] / 1e12),
        ", %.2fM hours of analysis" % hours,
        "%.1fTB read" % (by_crab_tasks[i]["read_bytes"] / 1e12),
        "%.1fB events read" % (by_crab_tasks[i]["events"] / 1e9),
    )

overflow_bin = 15
hours = (
    sum(
        val["core_hours"]
        for key, val in list(by_crab_tasks.items())
        if key > overflow_bin
    )
    / 1e6
)
total_hours += hours
read_bytes = (
    sum(
        val["read_bytes"]
        for key, val in list(by_crab_tasks.items())
        if key > overflow_bin
    )
    / 1e12
)
tot_bytes = (
    sum(val["bytes"] for key, val in list(by_crab_tasks.items()) if key >= overflow_bin)
    / 1e12
)
events = (
    sum(val["events"] for key, val in list(by_crab_tasks.items()) if key > overflow_bin)
    / 1e9
)
print(
    "overflow %.2fTB" % tot_bytes,
    "%.2fM hours" % hours,
    "%.1fTB read" % read_bytes,
    "%.1fB events read" % events,
)
print("Total hours %.1fM" % total_hours)
overflow = dict(
    [
        (key, val["bytes"])
        for key, val in list(by_crab_tasks.items())
        if key >= overflow_bin
    ]
)
# print overflow
print(
    "Weighted (by bytes) average value of overflow bin: %.1f"
    % (
        sum([key * val for key, val in list(overflow.items())])
        / float(sum(overflow.values()))
    )
)
