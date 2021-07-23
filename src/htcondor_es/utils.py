#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Various helper utilities for the HTCondor-ES integration"""
import email.mime.text
import errno
import json
import logging
import logging.handlers
import os
import pwd
import random
import shlex
import smtplib
import socket
import subprocess
import sys
import time
from itertools import zip_longest

import classad
import htcondor

TIMEOUT_MINS = 11


def get_schedds_from_file(args=None, collectors_file=None):
    """Reads collectors_file to get pools and gets schedds according to pool information.

    Calls `get_schedds` function.

    Notes:
        In previous version, `collectors_file` given as File object in `--collectors_file` arguments which is used in
        `celery_spider_cms.py` and read with `json.load`. This produces some drawbacks regarding proper file closing.
        Currently, instead of `json.load`, file read and converted to json properly. And file name is passed as var.

    Args:
        collectors_file (str): Collectors file name.
        args: Comes from `celery_spider_cms`

    """
    schedds = []
    names = set()
    try:
        with open(collectors_file) as f:  # Takes collectors_file as string
            pools = json.loads(f.read())
        for pool in pools:
            _pool_schedds = get_schedds(args, collectors=pools[pool], pool_name=pool)
            schedds.extend([s for s in _pool_schedds if s.get("Name") not in names])
            names.update([s.get("Name") for s in _pool_schedds])

    except (IOError, json.JSONDecodeError) as e:
        print(f"ERROR: There was a problem opening the collectors file {e}")
        schedds = get_schedds(args)
    return schedds


def get_schedds(args=None, collectors=None, pool_name="Unknown") -> list:
    """Return a list of schedds representing all the schedds in the pool.

    Returns:
        [
            [
                CMS_Pool = "YYY";
                MyAddress = "<x.x.x.x:x?addrs=x&alias=x&y&sock=x>";
                Name = "x";
                ScheddIpAddr = "<x.x.x.x:x?addrs=x&alias=x&y&sock=x>"
            ],
        ]

    """
    collectors = collectors or []
    schedd_query = classad.ExprTree("!isUndefined(CMSGWMS_Type)")

    schedd_ads = {}
    for host in collectors:
        coll = htcondor.Collector(host)
        try:
            schedds = coll.query(
                htcondor.AdTypes.Schedd,
                schedd_query,
                projection=["MyAddress", "ScheddIpAddr", "Name"],
            )
        except IOError as e:
            print("WARNING:", str(e))
            continue

        for schedd in schedds:
            try:
                schedd["CMS_Pool"] = pool_name
                schedd_ads[schedd["Name"]] = schedd
            except KeyError:
                pass

    schedd_ads = list(schedd_ads.values())
    random.shuffle(schedd_ads)

    if args and args.schedd_filter:
        return [s for s in schedd_ads if s["Name"] in args.schedd_filter.split(",")]

    return schedd_ads


def send_email_alert(recipients, subject, message):
    """
    Send a simple email alert (typically of failure).
    """
    if not recipients:
        return
    msg = email.mime.text.MIMEText(message)
    msg["Subject"] = "%s - %sh: %s" % (
        socket.gethostname(),
        time.strftime("%b %d, %H:%M"),
        subject,
    )

    domain = socket.getfqdn()
    uid = os.geteuid()
    pw_info = pwd.getpwuid(uid)
    if "cern.ch" not in domain:
        domain = "%s.unl.edu" % socket.gethostname()
    msg["From"] = "%s@%s" % (pw_info.pw_name, domain)
    msg["To"] = recipients[0]

    try:
        sess = smtplib.SMTP("localhost")
        sess.sendmail(msg["From"], recipients, msg.as_string())
        sess.quit()
    except Exception as exn:  # pylint: disable=broad-except
        print("WARNING: Email notification failed: %s", str(exn))


def time_remaining(starttime, timeout=TIMEOUT_MINS * 60, positive=True):
    """
    Return the remaining time (in seconds) until starttime + timeout
    Returns 0 if there is no time remaining
    """
    elapsed = time.time() - starttime
    if positive:
        return max(0, timeout - elapsed)
    return timeout - elapsed


def set_up_logging(args):
    """Configure root logger with rotating file handler"""
    logger = logging.getLogger()

    log_level = getattr(logging, args.log_level.upper(), None)
    if not isinstance(log_level, int):
        raise ValueError("Invalid log level: %s" % log_level)
    logger.setLevel(log_level)

    if log_level <= logging.INFO:
        logging.getLogger("CMSMonitoring.StompAMQ").setLevel(log_level + 10)
        logging.getLogger("stomp.py").setLevel(log_level + 10)

    try:
        os.makedirs(args.log_dir)
    except OSError as oserr:
        if oserr.errno != errno.EEXIST:
            raise

    log_file = os.path.join(args.log_dir, "spider_cms.log")
    filehandler = logging.handlers.RotatingFileHandler(log_file, maxBytes=100000)
    filehandler.setFormatter(
        logging.Formatter("%(asctime)s : %(name)s:%(levelname)s - %(message)s")
    )
    logger.addHandler(filehandler)

    if os.isatty(sys.stdout.fileno()):
        streamhandler = logging.StreamHandler(stream=sys.stdout)
        logger.addHandler(streamhandler)


def collect_metadata():
    """Returns metadata dictionary.

    Returns:
        {
            "spider_git_hash": b'2fe30f76198e6992d14a85eddf82e55e8cedadbd', # current git commit hash
            "spider_hostname": "spider-worker-669d9bcc49-zd4tb", # worker name
            "spider_username": "spider",
            "spider_runtime": 1621715109553 # current time in msec
        }

    """
    result = {}
    result["spider_git_hash"] = get_githash()
    result["spider_hostname"] = socket.gethostname()
    result["spider_username"] = pwd.getpwuid(os.geteuid()).pw_name
    result["spider_runtime"] = int(time.time() * 1000)
    return result


def get_githash():
    """Returns the git hash of the current commit in the scripts repository"""
    gitwd = os.path.dirname(os.path.realpath(__file__))
    cmd = r"git rev-parse --verify HEAD"
    try:
        call = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, cwd=gitwd)
        out, err = call.communicate()
        return str(out.strip())

    except Exception as e:
        logging.warning(str(e))
        return "unknown"


def grouper(iterable, n, fillvalue=None):
    """Collect data into fixed-length chunks or blocks

    References:
        - https://docs.python.org/3/library/itertools.html#itertools-recipes

    Returns:
        itertools.zip_longest: Iterator

    Examples:
        - list(grouper('ABCDEFG', 3, 'x'))
            `[('A', 'B', 'C'), ('D', 'E', 'F'), ('G', 'x', 'x')]`
        - list(grouper([1,2,4], 10, 'x'))
            `[(1, 2, 4, 'x', 'x', 'x', 'x', 'x', 'x', 'x')]`

    """
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)


def helper_condor_hist_query(collectors_file, schedd_name, query="", limit=1):
    """Get raw ClassAd for testing purpose

    Sometimes we may need to see raw ClassAd. This function helps to print a raw ClassAd with queries.
    You may find all schedds in `schedd_ads` list or "ScheddName" field in condor data in ES/Kibana.

    Args:
        schedd_name (str): Schedd name.
        collectors_file (str): Collectors file name.
        query (str): htcondor constraints query.
        limit (int): Limit of the results.

    Examples:
        custom_condor_hist_query(collectors_file="y",
                         schedd_name="x",
                         query='''MachineAttrCMSProcessingSiteName0 == "zzzz"''',
                         limit=1)
    """
    # "secrets/cms-htcondor-es/collectors"
    schedd_ads = get_schedds_from_file(None, collectors_file=collectors_file)
    print(schedd_ads)
    idx = [i for i, r in enumerate(schedd_ads) if (schedd_name in r.get("Name"))][0]
    schedd = htcondor.Schedd(schedd_ads[idx])
    print(schedd)
    query_iter = schedd.history(constraint=query, projection=[], match=limit)
    for r in query_iter:
        print(r)
