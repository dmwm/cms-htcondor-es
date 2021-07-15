#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Christian Ariza <christian.ariza AT gmail [DOT] com>, Ceyhun Uzunoglu <cuzunogl AT gmail [DOT] com>

"""Creates connection with AMQ broker and sends converted json data through AMQ with ``condor`` producer

Notes:
    Requirements in k8s:
        - ``CMS_AMQ_USERNAME_FILE``, AMQ username credential, should be provided. In current deployment,
            it is provided by the k8s secret ``amq-username`` and mounted in `/cms_secrets/amq-username` path.
        - ``CMS_AMQ_PASSWORD_FILE``, AMQ password credential, should be provided. In current deployment,
            it is provided by the k8s secret ``amq-password`` and mounted in `/cms_secrets/amq-password` path.
        -  ``CMS_HTCONDOR_PRODUCER``, producer name in MONIT infrastructure, should be provided. In current
            deployment, it's set as `condor-test`, but in production it will be `condor`. Provided as env var.
        - ``CMS_HTCONDOR_TOPIC``, topic name of AMQ broker, should be provided. In current deployment,
            it's set as `/topic/cms.jobmon.condor`, and in production it will be same. Provided as env var.
        - ``CMS_HTCONDOR_BROKER``, AMQ broker endpoint, should be provided. In current deployment,
            it's set as `cms-test-mb.cern.ch`, and in production it will change. Provided as env var.
        - ``61313`` is used as AMQ broker port.

References:
    - All mentioned environment variables in ``Notes`` are used only in:
        -- https://github.com/dmwm/CMSKubernetes/blob/master/kubernetes/spider/deployments/spider-worker.yaml
        -- https://github.com/dmwm/CMSKubernetes/blob/master/kubernetes/spider/cronjobs/spider-cron-queues.yaml

"""
import os
import time
import traceback

from CMSMonitoring.StompAMQ import StompAMQ

_amq_interface = None
# Log once
print("INFO: StompAMQ attributes - producer:{}, topic:{}, broker:{}, port:{}".format(os.getenv("CMS_HTCONDOR_PRODUCER"),
                                                                                     os.getenv("CMS_HTCONDOR_TOPIC"),
                                                                                     os.getenv("CMS_HTCONDOR_BROKER"),
                                                                                     61313))


def get_amq_interface():
    """Creates StompAMQ object, providing credentials and argument, to send data

    Args:
        `_amq_interface` (object): StompAMQ object

    """
    global _amq_interface
    if not _amq_interface:
        username_file, password_file = None, None
        try:
            username_file = os.getenv("CMS_AMQ_USERNAME_FILE", "username")
            password_file = os.getenv("CMS_AMQ_PASSWORD_FILE", "password")
            username = open(username_file, "r").read().strip()
            password = open(password_file, "r").read().strip()
        except IOError:
            print("ERROR: Provide username/password for CERN AMQ")
            print("ERROR: username_file {}\npassword_file {}".format(username_file, password_file))
            traceback.print_exc()
            return []
        _amq_interface = StompAMQ(
            username=username,
            password=password,
            producer=os.getenv("CMS_HTCONDOR_PRODUCER"),  # do not set default value
            topic=os.getenv("CMS_HTCONDOR_TOPIC"),  # do not set default value
            host_and_ports=[(os.getenv("CMS_HTCONDOR_BROKER"), 61313)],  # check whether 61313, or 61323
            validation_schema="JobMonitoring.json",
        )

    return _amq_interface


def amq_post_ads(ads, metadata=None):
    """Sends converted ClassAds through AMQ, using StompAMQ module

    Args:
        ads (list): Lits of converted ClassAds in json format.
        metadata (dict): Metadata of documents. If None defaults is used i.e. `producer`, `ts` set by StompAMQ

    Returns:
        tuple: (successful tasks, total tasks, elapsed time) # i.e. (90, 100, 10.08)

    """
    if not len(ads):
        print("WARNING: No new documents found")
        return

    metadata = metadata or {}
    interface = get_amq_interface()
    list_data = []
    for id_, ad in ads:
        notif, _, _ = interface.make_notification(
            payload=ad,
            docType="htcondor_job_info",
            docId=id_,
            ts=ad["RecordTime"],
            metadata=metadata,
            dataSubfield=None,
        )
        list_data.append(notif)

    starttime = time.time()
    failed_to_send = interface.send(list_data)
    elapsed = round(time.time() - starttime, 2)
    print("INFO: amq.post_ads doc size: {}, failed: {}, elapsed sec: {}".format(len(ads), len(failed_to_send), elapsed))
    return len(ads) - len(failed_to_send), len(ads), elapsed

