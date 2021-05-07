# -*- coding: utf-8 -*-
# Author: Christian Ariza <christian.ariza AT gmail [DOT] com>
"""
Configuration for the Celery application. 
"""
from celery import Celery
import os

app = Celery(
    "spider_celery",
    broker=os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0"),
    backend=os.getenv("CELERY_RESULT_BACKEND", "redis://redis:6379/1"),
    include=["htcondor_es.celery.tasks"],
)
# We have tasks with very different duration, in particular the
# first tasks are short. Using a prefetch multiplier of 1 we can help to
# load to be evenly distributed on the workers.
app.conf.worker_prefetch_multiplier = 1
# This setting will prevent memory leaks, by replacing the worker each 100 tasks.
app.conf.worker_max_tasks_per_child = 1000
# By default celery will the number of cpus as CELERYD_CONCURRENCY
if os.getenv("CELERY_TEST", None):
    app.conf.worker_concurrency = 1  # Just one worker by container.
app.conf.accept_content = ["pickle"]
app.conf.task_serializer = "pickle"
app.conf.result_serializer = "pickle"
# IMPORTANT: If worker lost, cronjobs waits this amount of time. Default is 1h, caused problems
# -- Test case: In the middle of queue processing (i.e. 2 minutes after cronjob start), redeploy spider-worker(s)
# --- In flower, you'll se some active tasks and these active jobs will be seen in active workers
# --- However, when you check workers in k8s, some of the active ones in flower will not be listed in k8s
# --- So, cronjob will hang 60 minutes and finish with completed status.
# --- More importantly, prospected cronjobs will not run and it results lost of data
app.conf.broker_transport_options = {'visibility_timeout': 600}
