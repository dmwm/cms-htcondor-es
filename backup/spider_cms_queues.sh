#!/bin/bash
WORKDIR=/home/cmsjobmon/cms-htcondor-es/
LOGDIR=$WORKDIR/log/

cd $WORKDIR
source venv/bin/activate

python spider_cms.py --log_dir $LOGDIR --log_level WARNING --feed_amq --skip_history --process_queue --query_queue_batch_size 100 --query_pool_size 16 --upload_pool_size 8 --collectors_file $WORKDIR/etc/collectors.json

#python spider_cms.py --log_dir $LOGDIR --log_level WARNING --feed_amq --email_alerts 'cms-comp-monit-alerts@cern.ch' --skip_history --process_queue --query_queue_batch_size 100 --query_pool_size 16 --upload_pool_size 8 --collectors_file $WORKDIR/etc/collectors.json

# crontab entry (to run every 12 min):
# */12 * * * * /home/cmsjobmon/cms-htcondor-es/spider_cms.sh
