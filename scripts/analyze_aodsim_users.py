import sqlite3

conn = sqlite3.connect("popdb.sqlite")
curs = conn.cursor()

tier = "/AODSIM"

datasets = {}
for row in curs.execute("select dataset, size_bytes, events from dataset_size"):
    datasets[row[0]] = {"size": row[1], "unique_events": row[2]}

usage = {}
for row in curs.execute(
    "select core_hours, events, read_bytes, dataset, user from dataset_popularity"
):
    if not row[3].endswith(tier):
        continue
    info = datasets[row[3]]
    user = row[4]
    core_hours = usage.setdefault(user, 0)
    usage[user] = core_hours + row[0]

topn = int(len(usage) * 0.2)
hours = list(usage.values())
print(sum(hours))
hours.sort()
print(hours[-topn:])
print(sum(hours[-topn:]))
print(sum(hours[-topn:]) / sum(hours))
print(len(usage))
