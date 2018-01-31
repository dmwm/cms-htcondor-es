
import sys
import calendar
import fnmatch
import datetime
import collections
import sqlite3

# See https://stackoverflow.com/questions/3424899/whats-the-simplest-way-to-subtract-a-month-from-a-date-in-python
def monthdelta(date, delta):
    m, y = (date.month+delta) % 12, date.year + ((date.month)+delta-1) // 12
    if not m: m = 12
    d = min(date.day, calendar.monthrange(y, m)[1])
    return date.replace(day=d,month=m, year=y)

conn = sqlite3.connect("popdb.sqlite")
curs = conn.cursor()

tier_pattern = sys.argv[1]

stop_month = int(sys.argv[2])
stop_month_dt = datetime.datetime(stop_month / 100, stop_month % 100, 1)
start_month_dt = monthdelta(stop_month_dt, -int(sys.argv[3]))
start_month = start_month_dt.year * 100 + start_month_dt.month
old_cutoff = (start_month_dt - datetime.datetime(1970, 1, 1)).total_seconds()
print "Start month: %d, Stop month: %d" % (start_month, stop_month)

datasets = {}
for row in curs.execute("select dataset, size_bytes, events, creation_date from dataset_size"):
    datasets[row[0]] = {"size": row[1], "unique_events": row[2], "creation_date": row[3], 'replica_bytes': 0}

replicas = collections.defaultdict(int)
unique_tiers = set()
for row in curs.execute("SELECT dataset, size_bytes, site FROM disk_replicas"):
    if row[2].endswith("_MSS") or row[2].endswith("_Buffer") or row[2].endswith("_Export"): continue
    if '#' in row[0]: continue
    row_tier = row[0].rsplit("/", 1)[-1]
    if not fnmatch.fnmatch(row_tier, tier_pattern): continue
    unique_tiers.add(row_tier)
    replicas[row[0]] += row[1]
print list(unique_tiers)

for row in curs.execute("SELECT core_hours, events, read_bytes, dataset, crab_task FROM dataset_popularity WHERE month > ? AND month <= ?", (start_month, stop_month)):
    if row[3].endswith("/USER"): continue
    row_tier = row[3].rsplit("/", 1)[-1]
    if not fnmatch.fnmatch(row_tier, tier_pattern): continue
    info = datasets[row[3]]
    core_hours = info.setdefault("core_hours", 0)
    events = info.setdefault("events", 0)
    read_bytes = info.setdefault("read_bytes", 0)
    info.setdefault("crab_tasks", set()).add(row[4])
    info['core_hours'] = core_hours + row[0]
    info['events'] = events + row[1]
    info['read_bytes'] = read_bytes + row[2]

for dataset, size_bytes in replicas.items():
    info = datasets[dataset]
    info['replica_bytes'] = size_bytes

by_crab_tasks = {}
total_bytes = 0
total_unique_events = 0
for dataset, info in datasets.items():
    if 'crab_tasks' not in info: continue
    num_tasks = len(info['crab_tasks'])
    cinfo = by_crab_tasks.setdefault(num_tasks, {})
    tot_bytes = cinfo.setdefault('bytes', 0)
    cinfo['bytes'] = tot_bytes + info['size']
    tot_hours = cinfo.setdefault('core_hours', 0)
    cinfo['core_hours'] = tot_hours + info['core_hours']
    read_bytes = cinfo.setdefault('read_bytes', 0)
    cinfo['read_bytes'] = read_bytes + info['read_bytes']
    events = cinfo.setdefault('events', 0)
    cinfo['events'] = events + info['events']
    unique_events = cinfo.setdefault('unique_events', 0)
    cinfo['unique_events'] = unique_events + info['unique_events']
    replica_bytes = cinfo.setdefault('replica_bytes', 0)
    cinfo['replica_bytes'] = replica_bytes + info['replica_bytes']
    total_bytes += info['size']
    total_unique_events += info['unique_events']

unaccessed_bytes = 0
unaccessed_old_bytes = 0
tot_replica_bytes = 0
for dataset, size_bytes in replicas.items():
    info = datasets[dataset]
    tot_replica_bytes += size_bytes
    crab_task_count = len(info.get('crab_tasks', []))
    if not crab_task_count:
        if info['creation_date'] < old_cutoff:
            unaccessed_old_bytes += size_bytes
        else:
            unaccessed_bytes += size_bytes


print "Total replica bytes: %.1f" % (tot_replica_bytes/1e12)
print "Number of CRAB3 tasks:"
total_hours = 0
for i in range(1, 15):
    hours = by_crab_tasks.setdefault(i, {'events': 0, 'core_hours': 0, 'read_bytes': 0, 'bytes': 0, 'replica_bytes': 0})['core_hours']/1e6
    total_hours += hours
    print i, '%.1fTB in dataset' % (by_crab_tasks[i]['replica_bytes']/1e12), ', %.2fM hours of analysis' % hours, '%.1fTB read' % (by_crab_tasks[i]['read_bytes']/1e12), '%.1fB events read' % (by_crab_tasks[i]['events']/1e9)

overflow_bin = 15
hours = (sum(val['core_hours'] for key, val in by_crab_tasks.items() if key > overflow_bin)/1e6)
total_hours += hours
read_bytes = (sum(val['read_bytes'] for key, val in by_crab_tasks.items() if key > overflow_bin)/1e12)
#tot_bytes = sum(val['bytes'] for key, val in by_crab_tasks.items() if key >= overflow_bin)/1e12
tot_bytes = sum(val['replica_bytes'] for key, val in by_crab_tasks.items() if key >= overflow_bin)/1e12
events = (sum(val['events'] for key, val in by_crab_tasks.items() if key > overflow_bin)/1e9)
unique_events = (sum(val['unique_events'] for key, val in by_crab_tasks.items() if key > overflow_bin)/1e9)
print "overflow %.2fTB" % tot_bytes, "%.1fB unique events" % unique_events, '%.2fM hours' % hours, '%.1fTB read' % read_bytes, '%.1fB events read' % events
print "Total hours %.1fM" % total_hours
overflow = dict([(key, val['replica_bytes']) for key, val in by_crab_tasks.items() if key >= overflow_bin])
#print overflow
print "Weighted (by bytes) average value of overflow bin: %.1f" % (sum([key*val for key, val in overflow.items()])/float(sum(overflow.values())))
print "Total accesses by CRAB: %.1fB unique events, %.1fTB read" % (total_unique_events/1e9, total_bytes/1e12)
print "Current on-disk replicas not accessed: %.1fTB created before this time period, %.1fTB created during this time period" % (unaccessed_old_bytes/1e12, unaccessed_bytes/1e12)

print "%.1f" % (unaccessed_old_bytes/1e12)
print "%.1f" % (unaccessed_bytes/1e12)
for i in range(1, 15):
    print '%.1f' % (by_crab_tasks[i]['replica_bytes']/1e12)
print "%.1f" % tot_bytes
