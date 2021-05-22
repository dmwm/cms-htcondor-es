#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Ceyhun Uzunoglu <cuzunogl AT gmail [DOT] com>

"""
Used by flower to get broker url. It's a solution of flower==1.0.1 bug and works only Kubernetes.
Even though we give broker url in Kubernetes flower deployment, this file is needed.

Attributes:
    BROKER_URL (str): Celery broker url.
        Consists of ``REDIS_SERVICE_HOST`` and ``REDIS_SERVICE_PORT_6379`` environment variables. They are environment
        variables in k8s deployment, ``REDIS`` is the name of service in k8s cluster.
        - REDIS_SERVICE_HOST (str): Host of redis which is used as celery broker in Kubernetes.
        - REDIS_SERVICE_PORT_6379 (str): Post of redis which is used as celery broker in Kubernetes.

        Both environment variables set in k8s deployment of ``name: redis, namespace: spider``:
            - https://github.com/dmwm/CMSKubernetes/blob/master/kubernetes/spider/deployments/spider-redis.yaml
            - https://github.com/dmwm/CMSKubernetes/blob/master/kubernetes/spider/service/spider-redis.yaml

References:
    - https://github.com/mher/flower/issues/1029#issuecomment-818539042

"""

import os

if os.getenv('REDIS_SERVICE_HOST') and os.getenv('REDIS_SERVICE_PORT_6379'):
    BROKER_URL = 'redis://' + os.getenv('REDIS_SERVICE_HOST') + ':' + os.getenv('REDIS_SERVICE_PORT_6379') + '/0'
else:
    print("'REDIS_SERVICE_HOST' or 'REDIS_SERVICE_PORT_6379' is not defined!",
          "Flower will run without accurate celery broker url!")
    pass
