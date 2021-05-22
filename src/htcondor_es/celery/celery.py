#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Christian Ariza <christian.ariza AT gmail [DOT] com>, Ceyhun Uzunoglu <cuzunogl AT gmail [DOT] com>

"""
Configuration for the Celery application.

Notes:
    - ``CELERY_BROKER_URL`` stands for `redis://$(REDIS_SERVICE_HOST):$(REDIS_SERVICE_PORT_6379)/0` endpoint
        which is the `redis` service used for ``celery broker`` and ``result backend``.
        ``REDIS_SERVICE_*`` values represents `redis` deployment[1] and service[2] in k8s cluster.
        Celery broker uses the `redis` service's database id of **0**.
    - ``CELERY_RESULT_BACKEND`` stands for `redis://$(REDIS_SERVICE_HOST):$(REDIS_SERVICE_PORT_6379)/1` endpoint
        which is the `redis` service used for celery broker and result backend. Celery result backend uses
        `redis` service's database id of **1**.
    - These two environment variables should be set in
        `cronjobs/spider-cron-affiliation.yaml`, `cronjobs/spider-cron-queues.yaml`, `deployments/spider-worker.yaml`.

Important:
    Please be aware of the `-Ofair` and `-E` parameters in `spider-worker`[3]
        - `-Ofair` is required to prevent long-running task which may block all future tasks [4]
        - `-E` is required for monitoring: worker_send_task_events

References:
    - https://docs.celeryproject.org/en/stable/userguide/configuration.html
    - https://docs.celeryproject.org/en/stable/userguide/configuration.html#std-setting-broker_transport_options
    - [1] https://github.com/dmwm/CMSKubernetes/blob/master/kubernetes/spider/deployments/spider-redis.yaml
    - [2] https://github.com/dmwm/CMSKubernetes/blob/master/kubernetes/spider/service/spider-redis.yaml
    - [3] https://github.com/dmwm/CMSKubernetes/blob/master/kubernetes/spider/deployments/spider-worker.yaml
    - [4] https://www.lorenzogil.com/blog/2020/03/01/celery-tasks/

"""
from celery import Celery
import os

app = Celery(
    "spider_celery",
    broker=os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0"),
    backend=os.getenv("CELERY_RESULT_BACKEND", "redis://redis:6379/1"),
    include=["htcondor_es.celery.tasks"],
)
# - We have tasks with very different duration, in particular the
# first tasks are short. Using a prefetch multiplier of 1 we can help to
# load to be evenly distributed on the workers.
app.conf.worker_prefetch_multiplier = 1

# This setting will prevent memory leaks, by replacing the worker each 100 tasks.
app.conf.worker_max_tasks_per_child = 1000

# By default celery will use the number of cpus as CELERYD_CONCURRENCY
if os.getenv("CELERY_TEST", None):
    app.conf.worker_concurrency = 1  # Just one worker by container.

app.conf.accept_content = ["pickle"]
app.conf.task_serializer = "pickle"
app.conf.result_serializer = "pickle"

# - If worker lost, cronjobs waits this amount of time. Default is 1h, caused problems
# Test case: In the middle of queue processing (i.e. 2 minutes after cronjob start), redeploy spider-worker(s)
# In flower, you'll se some active tasks and these active jobs will be seen in active workers
# However, when you check workers in k8s, some of the active ones in flower will not be listed in k8s
# So, cronjob will hang 60 minutes and finish with completed status.
# More importantly, prospected cronjobs will not run and it results lost of data.
app.conf.broker_transport_options = {'visibility_timeout': 600}
