
"""
Various helper utilities for the HTCondor-ES integration
"""

import os
import pwd
import sys
import time
import errno
import socket
import logging
import smtplib
import email.mime.text


TIMEOUT_MINS = 11


def send_email_alert(recipients, subject, message):
    """
    Send a simple email alert (typically of failure).
    """
    if not recipients:
        return
    msg = email.mime.text.MIMEText(message)
    msg['Subject'] = "%s - %sh: %s" % (socket.gethostname(),
                                       time.strftime("%b %d, %H:%M"),
                                       subject)

    domain = socket.getfqdn()
    uid = os.geteuid()
    pw_info = pwd.getpwuid(uid)
    if 'cern.ch' not in domain:
        domain = '%s.unl.edu' % socket.gethostname()
    msg['From'] = '%s@%s' % (pw_info.pw_name, domain)
    msg['To'] = recipients[0]

    try:
        sess = smtplib.SMTP('localhost')
        sess.sendmail(msg['From'], recipients, msg.as_string())
        sess.quit()
    except Exception as exn:  # pylint: disable=broad-except
        logging.warning("Email notification failed: %s", str(exn))


def time_remaining(starttime, timeout=TIMEOUT_MINS*60):
    """
    Return the remaining time (in seconds) until starttime + timeout
    """
    elapsed = time.time() - starttime
    return timeout - elapsed


def set_up_logging(args):
    """Configure root logger with rotating file handler"""
    logger = logging.getLogger()

    log_level = getattr(logging, args.log_level.upper(), None)
    if not isinstance(log_level, int):
        raise ValueError('Invalid log level: %s' % log_level)
    logger.setLevel(log_level)

    if log_level <= logging.INFO:
        logging.getLogger("htcondor_es.StompAMQ").setLevel(log_level + 10)
        logging.getLogger("stomp.py").setLevel(log_level + 10)

    try:
        os.makedirs(args.log_dir)
    except OSError as oserr:
        if oserr.errno != errno.EEXIST:
            raise

    log_file = os.path.join(args.log_dir, 'spider_cms.log')
    filehandler = logging.handlers.RotatingFileHandler(log_file, maxBytes=100000)
    filehandler.setFormatter(
        logging.Formatter('%(asctime)s : %(name)s:%(levelname)s - %(message)s'))
    logger.addHandler(filehandler)

    if os.isatty(sys.stdout.fileno()):
        streamhandler = logging.StreamHandler(stream=sys.stdout)
        logger.addHandler(streamhandler)
