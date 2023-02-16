#!/usr/bin/python

import datetime
import json
import logging
import os
import socket
import time
from collections import Counter as collectionsCounter

from opensearchpy import OpenSearch

import htcondor_es.convert_to_json

_WORKDIR = os.getenv("SPIDER_WORKDIR", "/home/cmsjobmon/cms-htcondor-es")
# OpenSearch credentials
_opensearch_creds_file = os.path.join(_WORKDIR, "/etc/es_conf.json")
# After each mapping creation, mapping is stored in the json
_last_index_mapping_json = os.path.join(_WORKDIR, "last_mappings.json")


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


# Global OpenSearch connection
_opensearch_client = None


def get_es_client(args=None):
    """
    Creates OpenSearch client

    Use a global ES client and return it if connection still holds. Else create a new connection.
    """
    global _opensearch_client

    if (not _opensearch_client) or (not _opensearch_client.handle.ping()):
        if not args:
            logging.error(
                "Call get_es_client with args first to create OpenSearch interface instance"
            )
            return _opensearch_client
        _opensearch_client = OpenSearchInterface(hostname=args.es_hostname, mock_cern_domain=args.mock_cern_domain)

    return _opensearch_client


class OpenSearchInterface(object):
    """Interface to opensearch

    mock_cern_domain is used to run dev test in docker like environments out-side of lxplus
    """

    def __init__(self, hostname="", mock_cern_domain=False):
        domain = socket.getfqdn().split(".", 1)[-1]
        if domain == "cern.ch" or mock_cern_domain:
            passwd = ""
            username = ""
            for line in open(_opensearch_creds_file):
                if "User" in line:
                    username = line.split(":")[1].strip()
                elif "Pass" in line:
                    passwd = line.split(":")[1].strip()
            url = 'https://' + username + ':' + passwd + '@' + hostname + '/es'
            self.handle = OpenSearch(
                [url],
                verify_certs=True,
                use_ssl=True,
                ca_certs="/etc/pki/tls/certs/ca-bundle.trust.crt",
            )
        else:
            self.handle = OpenSearch()

    def make_mapping(self, idx):
        """
        Creates mapping of the index and saves index in a json file
        """
        mappings = make_mappings()
        settings = make_settings()
        body = json.dumps({"mappings": mappings, "settings": {"index": settings}})

        with open(_last_index_mapping_json, "w") as jsonfile:
            json.dump(json.loads(body), jsonfile, indent=2, sort_keys=True)

        result = self.handle.indices.create(
            index=idx, body=body, ignore=400
        )
        if result.get("status") != 400:
            logging.warning("Creation of index %s: %s" % (idx, str(result)))
        elif "already exists" not in result.get("error", "").get("reason", ""):
            logging.error(
                "Creation of index %s failed: %s" % (idx, str(result.get("error", "")))
            )


# Holds indexes that are already created with mapping.
_opensearch_index_cache = set()


def get_index(timestamp, template, args, update_es=True):
    """
    Returns daily index string and creates it if it does not exist

    - If update_es is True, it checks if index mapping is already created by checking _opensearch_index_cache set.
    - And returns from _opensearch_index_cache set if index exists
    - Else, it creates the index with mapping which happens in the first batch of the day ideally.
    """
    # TODO check if putting _opensearch_handle here as global will work properly or not
    global _opensearch_index_cache
    idx = time.strftime(
        "%s-%%Y-%%m-%%d" % template,
        datetime.datetime.utcfromtimestamp(timestamp).timetuple(),
    )

    if update_es:
        if idx in _opensearch_index_cache:
            return idx

        get_es_client(args=args).make_mapping(idx)
        _opensearch_index_cache.add(idx)

    return idx


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


def post_ads(args, idx, ads, metadata=None):
    """Send ads in bulks"""
    body = make_es_body(ads, metadata)
    res = get_es_client(args=args).handle.bulk(body=body, index=idx, request_timeout=60)
    if res.get("errors"):
        return parse_errors(res)
