"""
Various helper utilities for the HTCondor-ES integration
"""

import os
import pwd
import sys
import time
import errno
import shlex
import socket
import random
import logging
import smtplib
import subprocess
import email.mime.text
import logging.handlers

import classad
import htcondor

TIMEOUT_MINS = 11


def get_schedds(args=None, collectors=None):
    """
    Return a list of schedd ads representing all the schedds in the pool.
    """
    collectors = collectors or [
        "cmssrv623.fnal.gov:9620",
        "cmsgwms-collector-tier0.cern.ch:9620",
        "cmssrv276.fnal.gov",
        "cmsgwms-collector-itb.cern.ch",
    ]
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
            logging.warning(str(e))
            continue

        for schedd in schedds:
            try:
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
        logging.warning("Email notification failed: %s", str(exn))


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
    """
    Return a dictionary with:
    - hostname
    - username
    - current time (in epoch millisec)
    - hash of current git commit
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
