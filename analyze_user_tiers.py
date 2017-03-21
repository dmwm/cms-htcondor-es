
import sys
import sqlite3

conn = sqlite3.connect("popdb.sqlite")
curs = conn.cursor()

users = {}
for row in curs.execute("select user, datatier from dataset_popularity"):
  if row[1] not in ["AOD", "AODSIM", "MINIAOD", "MINIAODSIM"]: continue
   
  users.setdefault(row[0], set()).add(row[1])

tiers = {}
for user, tier_set in users.items():
    tiers_list = list(tier_set)
    tiers_list.sort()
    key = ",".join(tiers_list)
    count = tiers.setdefault(key, 0)
    tiers[key] = count + 1

keys = tiers.keys()
keys.sort()
for key in keys:
    print "%s: %d" % (key, tiers[key])

