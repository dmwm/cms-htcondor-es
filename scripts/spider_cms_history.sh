#!/bin/bash
# Copied from vocms0240
#
export SPIDER_WORKDIR="/home/cmsjobmon/cms-htcondor-es"
export AFFILIATION_DIR_LOCATION="$SPIDER_WORKDIR/.affiliation_dir.json"
export PYTHONPATH="$SPIDER_WORKDIR/src/:$PYTHONPATH"
export CMS_HTCONDOR_TOPIC="/topic/cms.jobmon.condor"

# PROD
export CMS_HTCONDOR_PRODUCER="condor"
export CMS_HTCONDOR_BROKER="cms-mb.cern.ch"
_ES_INDEX_TEMPLATE="cms"

_LOGDIR=$SPIDER_WORKDIR/log_history/
_LOG_LEVEL="WARNING"
_ALERT_EMAILS="cms-comp-monit-alerts@cern.ch"
_ES_BUNCH_SIZE=100
_QUERY_POOL_SIZE=16
_UPLOAD_POOL_SIZE=8

cd $SPIDER_WORKDIR || exit
source "$SPIDER_WORKDIR/venv3_6/bin/activate"

# ./scripts/cronAffiliation.sh # First run

python scripts/spider_cms.py \
    --feed_amq \
    --feed_es \
    --log_dir $_LOGDIR \
    --log_level $_LOG_LEVEL \
    --es_bunch_size $_ES_BUNCH_SIZE \
    --query_pool_size $_QUERY_POOL_SIZE \
    --upload_pool_size $_UPLOAD_POOL_SIZE \
    --email_alerts "$_ALERT_EMAILS" \
    --collectors_file $SPIDER_WORKDIR/etc/collectors.json \
    --es_index_template $_ES_INDEX_TEMPLATE

# crontab entry (to run every 12 min, starting from 5 min past the hour):
# i.e. at 5,17,29,41,53 past the hour
# 5-59/12 * * * * /home/cmsjobmon/cms-htcondor-es/scripts/spider_cms.sh
