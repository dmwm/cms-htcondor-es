#!/usr/bin/python

import os
import re
import json
import time
import datetime
import classad
import datetime
import htcondor
import socket
import elasticsearch
import htcondor_es.convert_to_json

def filter_name(keys):
    for key in keys:
        if key.startswith("MATCH_EXP_JOB_"):
            key = key[len("MATCH_EXP_JOB_"):]
        if key.endswith("_RAW"):
            key = key[:-len("_RAW")]
        yield key


def make_mappings():
    mappings = {}
    for name in filter_name(htcondor_es.convert_to_json.int_vals):
        mappings[name] = {"type": "long"}
    for name in filter_name(htcondor_es.convert_to_json.string_vals):
        if name in htcondor_es.convert_to_json.no_idx:
            mappings[name] = {"type": "string", "index": "no"}
        elif name in htcondor_es.convert_to_json.no_analysis:
            mappings[name] = {"type": "string", "index": "not_analyzed"}
        else:
            mappings[name] = {"type": "string", "analyzer": "analyzer_keyword"}
    for name in filter_name(htcondor_es.convert_to_json.date_vals):
        mappings[name] = {"type": "date", "format": "epoch_second"}
    for name in filter_name(htcondor_es.convert_to_json.bool_vals):
        mappings[name] = {"type": "boolean"}
    mappings["Args"]["index"] = "no"
    mappings["Cmd"]["index"] = "no"
    mappings["StartdPrincipal"]["index"] = "no"
    mappings["StartdIpAddr"]["index"] = "no"
    mappings["x509UserProxyFQAN"]["analyzer"] = "standard"
    mappings["x509userproxysubject"]["analyzer"] = "standard"
    #print mappings
    return mappings


def make_settings():
    settings = {"analysis": {"analyzer": \
        {"analyzer_keyword": { \
            "tokenizer": "keyword",
            "filter": "lowercase",
        }
    }}}
    return settings


_es_handle = None
def get_server_handle():
    global _es_handle
    if not _es_handle:
        # TODO: config file.
        domain = socket.getfqdn().split(".", 1)[-1]
        if domain == 'cern.ch':
            passwd = ''
            username = ''
            regex = re.compile("^([A-Za-z]+):\s(.*)")
            for line in open("es.conf"):
                m = regex.match(line)
                if m:
                    key, val = m.groups()
                    if key == 'User':
                        username = val
                    elif key == 'Pass':
                        passwd = val
            _es_handle = elasticsearch.Elasticsearch([{"host": "es-cms.cern.ch", "http_auth":username+":"+passwd,"port":9203}],
                verify_certs=True,
                use_ssl=True,
                ca_certs='/etc/pki/tls/certs/ca-bundle.trust.crt')
        else:
            _es_handle = elasticsearch.Elasticsearch()
    return _es_handle


def fix_mapping(idx, template="cms"):
    _es_handle = get_server_handle()
    idx_clt = elasticsearch.client.IndicesClient(_es_handle)
    mappings = make_mappings()
    custom_mappings = {"CMSPrimaryDataTier": mappings["CMSPrimaryDataTier"],
                       "CMSPrimaryPrimaryDataset": mappings["CMSPrimaryPrimaryDataset"],
                       "CMSPrimaryProcessedDataset": mappings["CMSPrimaryProcessedDataset"]}
    print idx_clt.put_mapping(doc_type="job", index=idx, body=json.dumps({"properties": custom_mappings}), ignore=400)


def make_mapping(idx, template="cms"):
    _es_handle = get_server_handle()
    idx_clt = elasticsearch.client.IndicesClient(_es_handle)
    mappings = make_mappings()
    #print idx_clt.put_mapping(doc_type="job", index=idx, body=json.dumps({"properties": mappings}), ignore=400)
    settings = make_settings()
    #print idx_clt.put_settings(index=idx, body=json.dumps(settings), ignore=400)
    body = json.dumps({"mappings": {"job": {"properties": mappings} },
                       "settings": {"index": settings},
                      })
    result = _es_handle.indices.create(index=idx, body=body, ignore=400)
    if result.get("status") != 400:
        print "Creation of index %s: %s" % (idx, str(result))


_index_cache = set()
def get_index(timestamp, template="cms"):
    _es_handle = get_server_handle()

    idx = time.strftime("%s-%%Y-%%m-%%d" % template, datetime.datetime.utcfromtimestamp(timestamp).timetuple())
    if idx in _index_cache:
        return idx
    make_mapping(idx, template=template)
    _index_cache.add(idx)
    return idx


def post_ads(es, idx, ads):
    body = ''
    for id, ad in ads:
        body += json.dumps({"index": {"_id": id}}) + "\n"
        body += ad + "\n"
    es.bulk(body=body, doc_type="job", index=idx)


