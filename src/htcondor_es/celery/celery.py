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
    backend=os.getenv("CELERY_RESULT_BACKEND", "redis://redis:6379/0"),
    include=["htcondor_es.celery.tasks"],
)
# We have tasks with very different duration, in particular the
# first tasks are short. Using a prefetch multiplier of 1 we can helpt to
# load to be evenly distributed on the workers.
app.conf.worker_prefetch_multiplier = 1
# This setting will prevent memory leaks, by replacing the worker each 100 tasks.
app.conf.worker_max_tasks_per_child = 100
# By default celery will the number of cpus as CELERYD_CONCURRENCY
if os.getenv("CELERY_TEST", None):
    app.conf.worker_concurrency = 1  # Just one worker by container.
app.conf.timezone = "Europe/Zurich"
app.conf.accept_content = ["pickle", "json"]
app.conf.task_serializer = "pickle"
app.conf.result_serializer = "pickle"
