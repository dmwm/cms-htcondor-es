import os
import time
import logging
import traceback

from CMSMonitoring.StompAMQ import StompAMQ

_amq_interface = None


def get_amq_interface():
    global _amq_interface
    if not _amq_interface:
        try:
            username_file = os.getenv("CMS_AMQ_USERNAME_FILE", "username")
            password_file = os.getenv("CMS_AMQ_PASSWORD_FILE", "password")
            username = open(username_file, "r").read().strip()
            password = open(password_file, "r").read().strip()
        except IOError:
            logging.error("ERROR: Provide username/password for CERN AMQ")
            logging.error(
                "username_file {}\npassword_file {}".format(
                    username_file, password_file
                )
            )
            traceback.print_exc()
            return []
        _amq_interface = StompAMQ(
            username=username,
            password=password,
            producer=os.getenv("CMS_HTCONDOR_PRODUCER", "condor"),
            topic=os.getenv("CMS_HTCONDOR_TOPIC", "/topic/cms.jobmon.condor"),
            host_and_ports=[
                (os.getenv("CMS_HTCONDOR_BROKER", "cms-mb.cern.ch"), 61313)
            ],
            validation_schema="JobMonitoring.json",
        )

    return _amq_interface


def post_ads(ads, metadata=None):
    if not len(ads):
        logging.warning("No new documents found")
        return

    metadata = metadata or {}
    interface = get_amq_interface()
    logging.debug(interface)
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
    elapsed = time.time() - starttime
    return (len(ads) - len(failed_to_send), len(ads), elapsed)
