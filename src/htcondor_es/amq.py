import os
import time
import logging
from CMSMonitoring.StompAMQ7 import StompAMQ7 as StompAMQ

_amq_interface = None
_WORKDIR = os.getenv("SPIDER_WORKDIR", "/home/cmsjobmon/cms-htcondor-es")


def get_amq_interface():
    global _amq_interface
    if not _amq_interface:
        try:
            username = open(os.path.join(_WORKDIR, "etc/amq_username"), "r").read().strip()
            password = open(os.path.join(_WORKDIR, "etc/amq_password"), "r").read().strip()
        except IOError:
            print("ERROR: Provide username/password for CERN AMQ")
            return []
        _amq_interface = StompAMQ(
            username=username,
            password=password,
            producer=os.getenv("CMS_HTCONDOR_PRODUCER"),
            topic=os.getenv("CMS_HTCONDOR_TOPIC"),
            host_and_ports=[
                (
                    os.getenv("CMS_HTCONDOR_BROKER"),
                    61313
                )
            ],
            validation_schema=os.path.join(_WORKDIR, "JobMonitoring.json"),
        )

    return _amq_interface


def post_ads(ads, metadata=None):
    if not len(ads):
        logging.warning("No new documents found")
        return

    metadata = metadata or {}
    interface = get_amq_interface()
    list_data = []
    for id_, ad in ads:
        notif, _, _ = interface.make_notification(
            payload=ad,
            doc_type=None,  # will be default "metric"
            doc_id=id_,
            ts=ad["RecordTime"],
            metadata=metadata,
            data_subfield=None,
        )
        list_data.append(notif)

    starttime = time.time()
    failed_to_send = interface.send(list_data)
    elapsed = time.time() - starttime
    return len(ads) - len(failed_to_send), len(ads), elapsed
