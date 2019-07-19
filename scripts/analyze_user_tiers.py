import sys
import sqlite3

conn = sqlite3.connect("popdb.sqlite")
curs = conn.cursor()

users = {}
for row in curs.execute("select user, datatier from dataset_popularity"):
    if row[1] not in ["AOD", "AODSIM", "MINIAOD", "MINIAODSIM"]:
        continue

    users.setdefault(row[0], set()).add(row[1])

tiers = {}
for user, tier_set in list(users.items()):
    tiers_list = sorted(tier_set)
    key = ",".join(tiers_list)
    count = tiers.setdefault(key, 0)
    tiers[key] = count + 1

keys = sorted(list(tiers.keys()))
for key in keys:
    print("%s: %d" % (key, tiers[key]))
