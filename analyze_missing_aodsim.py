
import sqlite3

conn = sqlite3.connect("popdb.sqlite")
curs = conn.cursor()

datasets = {}
for row in curs.execute("select dataset, events from dataset_size"):
  info = row[0].split("/")
  small_ds = "/%s/%s" % (info[1], info[3])
  events = datasets.setdefault(small_ds, 0)
  datasets[small_ds] = events + row[1]

print len(datasets)

total_events = 0
missing_aodsim_events = 0
for dataset in datasets:
    info = dataset.split("/")
    if info[-1] != 'MINIAODSIM': continue
    info[-1] = 'AODSIM'
    aodsim_dataset = "/".join(info)
    total_events += datasets[dataset]
    if aodsim_dataset not in datasets:
        print aodsim_dataset
        missing_aodsim_events += datasets[dataset]
    #print aodsim_dataset
    #break

print total_events / 1e9
print missing_aodsim_events / 1e9

