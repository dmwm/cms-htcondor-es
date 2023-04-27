#!/bin/bash
WORKDIR=/home/cmsjobmon/cms-htcondor-es/
LOGDIR=$WORKDIR/log_history/

cd $WORKDIR
source venv/bin/activate

python spider_cms.py --log_dir $LOGDIR --log_level WARNING --feed_amq --feed_es --es_bunch_size 2000 --email_alerts 'cms-comp-monit-alerts@cern.ch' --collectors_file $WORKDIR/etc/collectors.json --es_hostname "es-cms.cern.ch"

# crontab entry (to run every 12 min, starting from 5 min past the hour):
# i.e. at 5,17,29,41,53 past the hour
# 5-59/12 * * * * /home/cmsjobmon/cms-htcondor-es/spider_cms.sh
