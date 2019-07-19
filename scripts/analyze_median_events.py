import sys
import sqlite3

conn = sqlite3.connect("popdb.sqlite")
curs = conn.cursor()

term = "/%s" % sys.argv[1]

events = []
size = []
for row in curs.execute("select dataset, events, size_bytes from dataset_size"):
    if not row[0].endswith(term):
        continue

    events.append(row[1])
    size.append(row[2])

events.sort()
size.sort()

print(
    "Of %d datasets in tier %s, median size was %dk events and %.1fGB"
    % (
        len(events),
        sys.argv[1],
        events[len(events) / 2] / 1000,
        size[len(size) / 2] / 1e9,
    )
)
