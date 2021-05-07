# The solution of flower==1.0.1 bug
# Ref: https://github.com/mher/flower/issues/1029#issuecomment-818539042
# Works only Kubernetes

import os

if os.getenv('REDIS_SERVICE_HOST') and os.getenv('REDIS_SERVICE_PORT_6379'):
    BROKER_URL = 'redis://' + os.getenv('REDIS_SERVICE_HOST') + ':' + os.getenv('REDIS_SERVICE_PORT_6379') + '/0'
else:
    pass
