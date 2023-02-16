#!/bin/bash
# Query history schedd and send to
#   - cms-test(index: monit_prod_condor_test_raw*) AMQ broker and
#   - es-cms.cern.ch (ES or OpenSearch soon) "cms-test" index.

export SPIDER_WORKDIR="/home/cmsjobmon/cms-htcondor-es"
export AFFILIATION_DIR_LOCATION="$SPIDER_WORKDIR/.affiliation_dir.json"
export PYTHONPATH="$SPIDER_WORKDIR/src/:$PYTHONPATH"
export CMS_HTCONDOR_TOPIC="/topic/cms.jobmon.condor"
_ES_BUNCH_SIZE=2000
_LOGDIR=$SPIDER_WORKDIR/log_history/
_LOG_LEVEL="WARNING"

# ----- : TEST VALUES : -----
export CMS_HTCONDOR_PRODUCER="condor-test"
export CMS_HTCONDOR_BROKER="cms-test-mb.cern.ch"
# please give any name you want. It will be in es-cms.cern.ch.
_ES_INDEX_TEMPLATE="cms-test"
# no email
_ALERT_EMAILS=""
# for test, query last N minutes
last_n_min=20

# should be less than #cpus, upload pool size suggestion is #cpus/2
_QUERY_POOL_SIZE=2
_UPLOAD_POOL_SIZE=1
# ----- : =========== : -----

cd $SPIDER_WORKDIR || exit
source "$SPIDER_WORKDIR/venv3_6/bin/activate"

# Clean test run, remove affiliation and checkpoint files before test run.
#rm -f "$SPIDER_WORKDIR"/.affiliation_dir.json
#rm -f $SPIDER_WORKDIR/checkpoint.json

# Create affiliations json first, otherwise convert_to_json.py will fail
# ./scripts/cronAffiliation.sh

# Read only test
python scripts/spider_cms.py \
    --read_only \
    --log_dir $_LOGDIR \
    --log_level $_LOG_LEVEL \
    --es_bunch_size $_ES_BUNCH_SIZE \
    --query_pool_size $_QUERY_POOL_SIZE \
    --upload_pool_size $_UPLOAD_POOL_SIZE \
    --email_alerts "$_ALERT_EMAILS" \
    --collectors_file $SPIDER_WORKDIR/etc/collectors.json \
    --es_index_template $_ES_INDEX_TEMPLATE \
    --history_query_max_n_minutes $last_n_min \
    --mock_cern_domain \
    --schedd_filter "vocms0258.cern.ch,vocms0267.cern.ch,crab3@vocms0195.cern.ch,crab3@vocms0196.cern.ch,crab3@vocms0194.cern.ch"

# Submit test
#python scripts/spider_cms.py \
#    --feed_amq \
#    --feed_es \
#    --log_dir $_LOGDIR \
#    --log_level $_LOG_LEVEL \
#    --es_bunch_size $_ES_BUNCH_SIZE \
#    --query_pool_size $_QUERY_POOL_SIZE \
#    --upload_pool_size $_UPLOAD_POOL_SIZE \
#    --email_alerts "$_ALERT_EMAILS" \
#    --collectors_file $SPIDER_WORKDIR/etc/collectors.json \
#    --es_index_template $_ES_INDEX_TEMPLATE \
#    --history_query_max_n_minutes $last_n_min \
#    --mock_cern_domain
