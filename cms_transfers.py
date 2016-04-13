#!/usr/bin/python

import os
import json
import time
import urllib
import urllib2
import pprint
import socket
import collections

import influxdb

if not os.path.exists('transfer_progress_cache'):
    os.makedirs('transfer_progress_cache')

transfer_statuses_url = "https://cmst2.web.cern.ch/cmst2/unified/transfer_statuses.json"

request_url = "https://cmsweb.cern.ch/phedex/datasvc/json/prod/transferrequests"

subscription_url = "https://cmsweb.cern.ch/phedex/datasvc/json/prod/subscriptions"

statuses = json.load(urllib2.urlopen(transfer_statuses_url))

subs = set()
for key in statuses:
   subs.add(key) # subscription ID
subs_list = list(subs)

while subs_list:
    loop_subs = subs_list[:20]
    subs_list = subs_list[20:]
    xfer_reqs = urllib.urlencode([("request", i) for i in loop_subs if not os.path.exists("transfer_progress_cache/request_%s" % i)])
    if not xfer_reqs:
        #print "All requests cached... skipping"
        continue
    print request_url + "?" + xfer_reqs
    reqs = json.load(urllib2.urlopen(request_url + "?" + xfer_reqs))

    for req in reqs['phedex']['request']:
        fd = open("transfer_progress_cache/request_%s" % req["id"], "w")
        json.dump(req, fd)

full_datasets = {}
subs_list = list(subs)
counter = 0

def good_sub(id, oldest):
    fname = "transfer_progress_cache/subscription_%s" % id
    if not os.path.exists(fname):
        return False
    fd = open(fname)
    try:
        sub_info = json.load(fd)
    except:
        return False
    return sub_info['time'] > oldest

while subs_list:
    oldest = time.time() - 1800
    loop_subs = subs_list[:20]
    subs_list = subs_list[20:]
    min_req = time.time()
    for reqid in loop_subs:
        try:
            req = json.load(open("transfer_progress_cache/request_%s" % reqid))
        except:
            print "Corrupted cache file found:", "transfer_progress_cache/request_%s" % reqid
            raise
        if min_req > req['time_create']:
            min_req = req['time_create']
    min_req = int(min_req-10)

    loop_subs = [i for i in loop_subs if not good_sub(i, oldest)]
    if not loop_subs:
        #print "Skipping fully cached subscription list."
        continue
    counter += 1
    subs_query = urllib.urlencode([("request", i) for i in loop_subs] + [("create_since", min_req), ("suspended", "n")])
    #print subs_query
    subs_result = json.load(urllib2.urlopen(subscription_url + "?" + subs_query))
    print "%d queries done; %d subscriptions remaining." % (counter, len(subs_list))
    cache_info = {'time': time.time(), 'info': subs_result['phedex']['dataset']}
    for id in loop_subs:
        json.dump(cache_info, open("transfer_progress_cache/subscription_%s" % id, 'w'))

seen_reqs = set()
for req_id in subs:
    if req_id in seen_reqs:
        continue
    info = json.load(open("transfer_progress_cache/subscription_%s" % req_id))['info']
    for dataset in info:
        dataset_info = full_datasets.setdefault(dataset['name'], {'bytes': dataset['bytes'], 'name': dataset['name']})
        if 'block' in dataset:
            block_list = dataset_info.setdefault('block', [])
            for block in dataset['block']:
                # Try to find the corresponding block info in the block list;
                # if not present, generate a new one.
                block_info = None
                for block_tmp in block_list:
                    if block_tmp['name'] == block['name']:
                        block_info = block_tmp
                        break
                if not block_info:
                    block_info = {'name': block['name'], 'bytes': block['bytes'], 'subscription': []}
                    block_list.append(block_info)
                block_info['subscription'] += block['subscription']
                seen_reqs.update([str(i['request']) for i in block['subscription']])
        else:
            subscription_list = dataset_info.setdefault('subscription', [])
            subscription_list += dataset['subscription']
            seen_reqs.update([str(i['request']) for i in dataset['subscription']])
    #print seen_reqs
    #if counter == 2: break

def not_tape(node):
    return not node.endswith('MSS') and not node.endswith('Buffer')

site_completion = {}
for dataset in full_datasets.values():
    #print dataset
    full_bytes = dataset['bytes']
    # Block level subscription
    if 'block' in dataset:
        #pprint.pprint(dataset['block'])
        # The histogram logic is a bit funky - we want to bin by percent complete of the dataset at any given site.
        block_xfer = 0
        for block in dataset['block']:
            max_xfer = max([subscription['node_bytes'] for subscription in block['subscription'] if not_tape(subscription['node'])])
            block_xfer += max_xfer
        histo_bin = 10*int(10*block_xfer/float(full_bytes))
        for block in dataset['block']:
            block_full_bytes = block['bytes']
            for subscription in block['subscription']:
                node_info = site_completion.setdefault(subscription['node'], {'done_histo': collections.defaultdict(int), 'remains_histo': collections.defaultdict(int), 'done': 0, 'remains': 0})
                node_info['remains'] += block_full_bytes - subscription['node_bytes']
                node_info['done'] += subscription['node_bytes']
                node_info['done_histo'][histo_bin] += subscription['node_bytes']
                node_info['remains_histo'][histo_bin] += block_full_bytes - subscription['node_bytes']
                #if subscription['node'] == 'T2_US_Nebraska':
                #    print block['name'], subscription['node_bytes']/1e12, subscription
    # dataset level sub
    else:
        max_xfer = max([subscription['node_bytes'] for subscription in dataset['subscription'] if not_tape(subscription['node'])])
        histo_bin = 10*int(10*max_xfer/float(full_bytes))
        for subscription in dataset['subscription']:
            node_info = site_completion.setdefault(subscription['node'], {'done_histo': collections.defaultdict(int), 'remains_histo': collections.defaultdict(int), 'done': 0, 'remains': 0})
            node_info['remains'] += full_bytes - subscription['node_bytes']
            node_info['done'] += subscription['node_bytes']
            node_info['done_histo'][histo_bin] += subscription['node_bytes']
            node_info['remains_histo'][histo_bin] += full_bytes - subscription['node_bytes']
            #if subscription['node'] == 'T2_US_Nebraska':
            #    print dataset['name'], subscription['node_bytes']/1e12

pprint.pprint(site_completion)
json.dump(site_completion, open("transfer_progress_cache/current_subs", "w"))

#sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
#now_ns = int(time.time())*int(1e9)
#for site, info in site_completion.items():
#    data = ''
#    for idx in range(0, 11):
#        data += 'staging,site=%s,avail=%d done=%d,remaining=%d %d\n' % (site, idx*10, info['done_histo'].get(idx, 0), info['remains_histo'].get(idx, 0), now_ns)
#    sock.sendto(data, ('127.0.0.1', 8089))

client = influxdb.InfluxDBClient('localhost', 8086, 'root', 'root', 'cms')
now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
data = []
#print site_completion['T2_US_Wisconsin']
for site, info in site_completion.items():
    for idx in range(0, 11):
        json = {"measurement": "staging",
                "tags": {"site": site, "avail": str(idx*10)},
                "time": now,
                "fields": {"done": float(info['done_histo'].get(10*idx, 0)), "remaining": float(info['remains_histo'].get(10*idx, 0))}
               }
        #if site == 'T2_US_Wisconsin':
        #    print json
        data.append(json)
client.write_points(data)

