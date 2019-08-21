# coding=utf8
import os
import time
import traceback
from celery import group
from htcondor_es.celery.tasks import query_schedd
from htcondor_es.utils import get_schedds

if __name__ == '__main__':
    if not os.getenv("CMS_HTCONDOR_BROKER"):
        os.environ["CMS_HTCONDOR_BROKER"] = "cms-test-mb.cern.ch"
        os.environ["CMS_HTCONDOR_PRODUCER"] = "condor-test"
    schedd_ads = get_schedds()
    res = group(query_schedd.s(sched) for sched in schedd_ads).apply_async()
    groups = res.get()
    print([g.collect() for g in groups])