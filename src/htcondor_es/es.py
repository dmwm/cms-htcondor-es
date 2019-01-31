#!/usr/bin/python

import os
import re
import json
import time
import datetime
import classad
import datetime
import logging
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
    props = {}
    for name in filter_name(htcondor_es.convert_to_json.int_vals):
        props[name] = {"type": "long"}
    for name in filter_name(htcondor_es.convert_to_json.string_vals):
        if name in htcondor_es.convert_to_json.no_idx:
            props[name] = {"type": "text", "index": "no"}
        elif name in htcondor_es.convert_to_json.no_analysis:
            props[name] = {"type": "text", "index": "not_analyzed"}
        # else:
        #     props[name] = {"type": "keyword"} #, "analyzer": "analyzer_keyword"}
    for name in filter_name(htcondor_es.convert_to_json.date_vals):
        props[name] = {"type": "date", "format": "epoch_second"}
    for name in filter_name(htcondor_es.convert_to_json.bool_vals):
        props[name] = {"type": "boolean"}
    props["Args"]["index"] = "no"
    props["Cmd"]["index"] = "no"
    props["StartdPrincipal"]["index"] = "no"
    props["StartdIpAddr"]["index"] = "no"
    # props["x509UserProxyFQAN"]["analyzer"] = "standard"
    # props["x509userproxysubject"]["analyzer"] = "standard"
    props["metadata"] = {"properties":{"spider_runtime" : {"type": "date", "format": "epoch_millis"}}}

    dynamic_string_template = {
        "strings_as_keywords" : {
            "match_mapping_type" : "string",
            "mapping" : {
                "type" : "keyword",
                "norms" : "false",
                "ignore_above" : 256
            }
        }
    }

    mappings = {
        "job": {
            "dynamic_templates": [
                dynamic_string_template
            ],
            "properties": props
        }
    }
    return mappings


def make_settings():
    settings = {"analysis": {
                    "analyzer": {
                        "analyzer_keyword": {
                            "tokenizer": "keyword",
                            "filter": "lowercase",
                            }
                        }
                    },
                "mapping.total_fields.limit": 2000}
    return settings


_es_handle = None
def get_server_handle(args=None):
    global _es_handle
    if not _es_handle:
        if not args:
            logging.error("Call get_server_handle with args first to create ES interface instance")
            return _es_handle
        _es_handle = ElasticInterface(hostname=args.es_hostname, port=args.es_port)
    return _es_handle

class ElasticInterface(object):
    """Interface to elasticsearch"""
    def __init__(self, hostname="es-cms.cern.ch", port=9203):
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
            self.handle = elasticsearch.Elasticsearch([{"host": hostname, "port":port, 
                                                        "http_auth":username+":"+passwd}],
                                                        verify_certs=True,
                                                        use_ssl=True,
                                                        ca_certs='/etc/pki/tls/certs/ca-bundle.trust.crt')
        else:
            self.handle = elasticsearch.Elasticsearch()


    def fix_mapping(self, idx, template="cms"):
        idx_clt = elasticsearch.client.IndicesClient(self.handle)
        mappings = make_mappings()
        custom_mappings = {"CMSPrimaryDataTier": mappings["job"]["properties"]["CMSPrimaryDataTier"],
                           "CMSPrimaryPrimaryDataset": mappings["job"]["properties"]["CMSPrimaryPrimaryDataset"],
                           "CMSPrimaryProcessedDataset": mappings["job"]["properties"]["CMSPrimaryProcessedDataset"]}
        logging.info(idx_clt.put_mapping(doc_type="job", index=idx, body=json.dumps({"properties": custom_mappings}), ignore=400))

    def make_mapping(self, idx, template="cms"):
        idx_clt = elasticsearch.client.IndicesClient(self.handle)
        mappings = make_mappings()
        #print idx_clt.put_mapping(doc_type="job", index=idx, body=json.dumps({"properties": mappings}), ignore=400)
        settings = make_settings()
        #print idx_clt.put_settings(index=idx, body=json.dumps(settings), ignore=400)

        body = json.dumps({"mappings": mappings,
                           "settings": {"index": settings},
                          })

        with open('last_mappings.json', 'w') as jsonfile:
            json.dump(json.loads(body), jsonfile, indent=2, sort_keys=True)

        result = self.handle.indices.create(index=idx, body=body, ignore=400)
        if result.get("status") != 400:
            logging.warning("Creation of index %s: %s" % (idx, str(result)))
        elif 'already exists' not in result.get("error","").get("reason",""):
            logging.error("Creation of index %s failed: %s" % (idx, str(result.get("error", ""))))


_index_cache = set()
def get_index(timestamp, template="cms", update_es=True):
    global _index_cache
    idx = time.strftime("%s-%%Y-%%m-%%d" % template, datetime.datetime.utcfromtimestamp(timestamp).timetuple())

    if update_es:
        if idx in _index_cache:
            return idx

        _es_handle = get_server_handle()
        _es_handle.make_mapping(idx, template=template)
        _index_cache.add(idx)
    
    return idx


def make_es_body(ads, metadata=None):
    metadata = metadata or {}
    body = ''
    for id_, ad in ads:
        if metadata:
            ad.setdefault('metadata', {}).update(metadata)

        body += json.dumps({"index": {"_id": id_}}) + "\n"
        body += json.dumps(ad) + "\n"

    return body


def parse_errors(result):
    from collections import Counter
    reasons = [d.get('index', {}).get('error', {}).get('reason', None) for d in result['items']]
    counts = Counter(filter(None, reasons))
    n_failed = sum(counts.values())
    logging.error("Failed to index %d documents to ES: %s" % (n_failed, str(counts.most_common(3))))
    return n_failed


def post_ads(es, idx, ads, metadata=None):
    body = make_es_body(ads, metadata)
    res = es.bulk(body=body, doc_type="job", index=idx, request_timeout=60)
    if res.get('errors'):
        return parse_errors(res)


def post_ads_nohandle(idx, ads, args, metadata=None):
    es = get_server_handle(args).handle
    body = make_es_body(ads, metadata)
    res = es.bulk(body=body, doc_type="job", index=idx, request_timeout=60)
    if res.get('errors'):
        return parse_errors(res)

    return len(ads)
