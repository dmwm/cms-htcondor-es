#!/usr/bin/python

import os
import re
import json
import time
import datetime
import classad
import datetime
import htcondor
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
        _es_handle = elasticsearch.Elasticsearch()
    return _es_handle


def fix_mapping(idx, template="cms"):
    _es_handle = get_server_handle()
    idx_clt = elasticsearch.client.IndicesClient(_es_handle)
    mappings = make_mappings(template=template)
    custom_mappings = {"RecordTime": mappings["RecordTime"]}
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

    idx = time.strftime("cms-%Y-%m-%d", datetime.datetime.utcfromtimestamp(timestamp).timetuple())
    if idx in _index_cache:
        return idx
    make_mapping(idx, template=template)
    _index_cache.add(idx)
    return idx


def post_ads(es, idx, ads):
    body = ''
    for id, ad in ads:
        body += json.dumps({"index": {"_index": idx, "_type": "job", "_id": id}}) + "\n"
        body += ad + "\n"
    es.bulk(body=body)


