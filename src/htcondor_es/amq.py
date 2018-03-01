import time
import logging
import multiprocessing
from htcondor_es.StompAMQ import StompAMQ
StompAMQ._version = '0.1.2'

_amq_interface = None
def get_amq_interface():
    global _amq_interface
    if not _amq_interface:
        try:
            username = open('username', 'r').read().strip()
            password = open('password', 'r').read().strip()
        except IOError:
            print "ERROR: Provide username/password for CERN AMQ"
            return []
        _amq_interface = StompAMQ(username=username,
                                    password=password,
                                    topic='/topic/cms.jobmon.condor',
                                    host_and_ports=[('dashb-mb.cern.ch', 61113)])

    return _amq_interface


def post_ads(ads):
    if not len(ads):
        logging.warning("No new documents found")
        return

    interface = get_amq_interface()
    list_data = []
    for id_, ad in ads:
        list_data.append(interface.make_notification(payload=ad,
                                                     id_=id_,
                                                     type_='htcondor_job_info',
                                                     timestamp=ad['RecordTime']))

    starttime = time.time()
    sent_data = interface.send(list_data)
    elapsed = time.time() - starttime
    return (len(sent_data), len(ads), elapsed)
