#!/usr/bin/python

import datetime
import json
import logging
import os
import socket
import time
from collections import Counter as collectionsCounter

import elasticsearch
from opensearchpy import OpenSearch

import htcondor_es.convert_to_json

_WORKDIR = os.getenv("SPIDER_WORKDIR", "/home/cmsjobmon/cms-htcondor-es")

# Elastic and Open Search credentials
_es_creds_file = os.path.join(_WORKDIR, "etc/es_conf.json")

# After each mapping creation, mapping is stored in the json
_last_index_mapping_json = os.path.join(_WORKDIR, "last_mappings.json")

# Global Elastic and Open Search connections obj
_es_clients = None

# Global index cache, keep tracks of daily indices that are already created with mapping for all clusters
_index_cache = set()


class ElasticAndOpenSearchInterfaces(object):
    """Interface to elasticsearch

    mock_cern_domain is used to run dev test in docker like environments out-side of lxplus

    Supports multiple ElasticSearch and OpenSearch instances. ES/OS instance credentials should be provided with the
    convention that OS host name should end with "/es" and ES config should include "port".

    es_conf.json format:
        [
          { "host": "es-cmsX(ElasticSearch).cern.ch", "port": 9203, "username": "user", "password": "pass" },
          { "host": "es-cmsY(OpenSearch).cern.ch/es", "username": "user", "password": "pass" },
          { "host": "es-cmsZ(OpenSearch).cern.ch/es", "username": "user", "password": "pass" },
          ...
        ]
    """

    def __init__(self, mock_cern_domain=False):
        try:
            self.handles = set()
            self.host_count = None
            domain = socket.getfqdn().split(".", 1)[-1]
            if domain == "cern.ch" or mock_cern_domain:
                with open(_es_creds_file) as f:
                    creds = json.loads(f.read())
                    self.host_count = len(creds)
                    for cred in creds:
                        if cred["host"].endswith("/es"):
                            logging.info("OpenSearch instance is initializing")
                            url = 'https://' + cred["username"] + ':' + cred["password"] + '@' + cred["host"]
                            self.handles.add(
                                OpenSearch(
                                    [url],
                                    verify_certs=True,
                                    use_ssl=True,
                                    ca_certs="/etc/pki/tls/certs/ca-bundle.trust.crt",
                                )
                            )
                        else:
                            logging.info("ElasticSearch instance is initializing")
                            self.handles.add(elasticsearch.Elasticsearch(
                                [{
                                    "host": cred["host"],
                                    "port": cred["port"],
                                    "http_auth": cred["username"] + ":" + cred["password"],
                                }],
                                verify_certs=True,
                                use_ssl=True,
                                ca_certs="/etc/pki/tls/certs/ca-bundle.trust.crt",
                                maxsize=25,  # https://elasticsearch-py.readthedocs.io/en/7.x/#thread-safety
                            ))
                logging.warning("ES instance count: " + str(self.host_count))
            else:
                self.handles.add(elasticsearch.Elasticsearch())
        except Exception as e:
            logging.error("ElasticInterface initialization failed: " + str(e))

    def make_mapping(self, idx):
        """
        Creates mapping of the index and saves index in a json file
        """
        mappings = make_mappings()
        settings = make_settings()
        body = json.dumps({"mappings": mappings, "settings": {"index": settings}})

        with open(_last_index_mapping_json, "w") as jsonfile:
            json.dump(json.loads(body), jsonfile, indent=2, sort_keys=True)

        # Make mappings for all ES OpenSearch instances
        for _handle in self.handles:
            result = _handle.indices.create(index=idx, body=body, ignore=400)
            if result.get("status") != 400:
                logging.warning("Creation of index %s: %s" % (idx, str(result)))
            elif "already exists" not in result.get("error", "").get("reason", ""):
                logging.error("Creation of index %s failed: %s" % (idx, str(result.get("error", ""))))


def get_es_clients(args=None):
    """
    Creates ES and OpenSearch clients

    Use a global ES client and return it if connection still holds. Else create a new connection.
    """
    global _es_clients

    if not _es_clients:
        if not args:
            logging.error("Call get_es_client with args first to create ES interface instance")
            return _es_clients

        _es_clients = ElasticAndOpenSearchInterfaces(mock_cern_domain=args.mock_cern_domain)

    # Since our operations are not that long, we don't need to check connections with ping which takes time
    # else:
    #     try:
    #         # Ping them
    #         for _handle in _es_clients.handles:
    #             if not _handle.ping():
    #                 logging.warning("Elasticsearch client will reconnect:")
    #                 _es_clients = ElasticAndOpenSearchInterfaces(mock_cern_domain=args.mock_cern_domain)
    #                 break
    #     except Exception as e:
    #         logging.warning("Elasticsearch client will reconnect: " + str(e))
    #         _es_clients = ElasticAndOpenSearchInterfaces(mock_cern_domain=args.mock_cern_domain)
    return _es_clients


def get_index(timestamp, template, args, update_es=True):
    """
    Returns daily index string and creates it if it does not exist.

    - If update_es is True, it checks if index mapping is already created by checking _index_cache set.
    - And returns from _index_cache set if index exists
    - Else, it creates the index with mapping which happens in the first batch of the day ideally.
    """
    # TODO check if putting _es_handle here as global will work properly or not
    global _index_cache
    idx = time.strftime(
        "%s-%%Y-%%m-%%d" % template,
        datetime.datetime.utcfromtimestamp(timestamp).timetuple(),
    )

    if update_es:
        if idx in _index_cache:
            return idx
        get_es_clients(args=args).make_mapping(idx=idx)
        _index_cache.add(idx)

    return idx


def post_ads(args, idx, ads, metadata=None):
    """
    Send ads in bulks to all ES OpenSearch instances
    """
    global _es_clients
    _es_clients = get_es_clients(args=args)
    body = make_es_body(ads, metadata)
    result_n_failed = 0
    for _handle in _es_clients.handles:
        res = _handle.bulk(body=body, index=idx, request_timeout=120)
        if res.get("errors"):
            result_n_failed += parse_errors(res)
    return result_n_failed


def filter_name(keys):
    """
    Filters ClassAd fields and removes 'MATCH_EXP_JOB_' prefix and '_RAW' suffix
    """
    for key in keys:
        if key.startswith("MATCH_EXP_JOB_"):
            key = key[len("MATCH_EXP_JOB_"):]
        if key.endswith("_RAW"):
            key = key[: -len("_RAW")]
        yield key


def make_mappings():
    """
    Creates mapping dictionary for the parsed ClassAds

    Set type long for int values, set index:false for no_idx ClassAds, set keyword for no_analysis ClassAds
    Includes special setting for special ClassAds: Args, Cmd, StartdPrincipal, etc..
    """
    props = {}
    for name in filter_name(htcondor_es.convert_to_json.int_vals):
        props[name] = {"type": "long"}
    for name in filter_name(htcondor_es.convert_to_json.string_vals):
        if name in htcondor_es.convert_to_json.no_idx:
            props[name] = {"type": "text", "index": "false"}
        elif name in htcondor_es.convert_to_json.no_analysis:
            props[name] = {"type": "keyword"}
        # else:
        #     props[name] = {"type": "keyword"} #, "analyzer": "analyzer_keyword"}
    for name in filter_name(htcondor_es.convert_to_json.date_vals):
        props[name] = {"type": "date", "format": "epoch_second"}
    for name in filter_name(htcondor_es.convert_to_json.bool_vals):
        props[name] = {"type": "boolean"}
    props["Args"]["index"] = "false"
    props["Cmd"]["index"] = "false"
    props["StartdPrincipal"]["index"] = "false"
    props["StartdIpAddr"]["index"] = "false"
    # props["x509UserProxyFQAN"]["analyzer"] = "standard"
    # props["x509userproxysubject"]["analyzer"] = "standard"
    props["metadata"] = {
        "properties": {"spider_runtime": {"type": "date", "format": "epoch_millis"}}
    }

    dynamic_string_template = {
        "strings_as_keywords": {
            "match_mapping_type": "string",
            "mapping": {"type": "keyword", "norms": "false", "ignore_above": 256},
        }
    }

    mappings = {"dynamic_templates": [dynamic_string_template], "properties": props}
    return mappings


def make_settings():
    """
    Creates settings dictionary part of the index mapping
    """
    settings = {
        "analysis": {
            "analyzer": {
                "analyzer_keyword": {"tokenizer": "keyword", "filter": "lowercase"}
            }
        },
        "mapping.total_fields.limit": 2000,
    }
    return settings


def make_es_body(ads, metadata=None):
    """
    Prepares ES documents for bulk send by adding metadata part, adding _id part and separating with new line
    """
    metadata = metadata or {}
    body = ""
    for id_, ad in ads:
        if metadata:
            ad.setdefault("metadata", {}).update(metadata)

        body += json.dumps({"index": {"_id": id_}}) + "\n"
        body += json.dumps(ad) + "\n"

    return body


def parse_errors(result):
    """
    Parses bulk send result and finds errors to log
    """
    reasons = [
        d.get("index", {}).get("error", {}).get("reason", None) for d in result["items"]
    ]
    counts = collectionsCounter([_f for _f in reasons if _f])
    n_failed = sum(counts.values())
    logging.error(
        "Failed to index %d documents to ES: %s"
        % (n_failed, str(counts.most_common(3)))
    )
    return n_failed
