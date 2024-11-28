#!/bin/bash
# Query history schedd and send to
#   - cms-test(index: monit_prod_condor_test_raw*) AMQ broker and

export SPIDER_WORKDIR="/home/cmsjobmon/cms-htcondor-es"
export AFFILIATION_DIR_LOCATION="$SPIDER_WORKDIR/.affiliation_dir.json"
export PYTHONPATH="$SPIDER_WORKDIR/src/:$PYTHONPATH"
export CMS_HTCONDOR_TOPIC="/topic/cms.jobmon.condor"
_LOGDIR=$SPIDER_WORKDIR/log/
_LOG_LEVEL="WARNING"
_QUERY_QUEUE_BATCH_SIZE=100

# ----- : TEST VALUES : -----
export CMS_HTCONDOR_PRODUCER="condor-test"
export CMS_HTCONDOR_BROKER="cms-test-mb.cern.ch"

# should be less than #cpus, upload pool size suggestion is #cpus/2
_QUERY_POOL_SIZE=2
_UPLOAD_POOL_SIZE=1
# ----- : =========== : -----

cd $SPIDER_WORKDIR || exit
source "$SPIDER_WORKDIR/venv3_9/bin/activate"

# Clean test run, remove affiliation and checkpoint files before test run.
#rm -f $SPIDER_WORKDIR/.affiliation_dir.json
#rm -f $SPIDER_WORKDIR/checkpoint.json

# Create affiliations json first, otherwise convert_to_json.py will fail
# ./scripts/cronAffiliation.sh

python scripts/spider_cms.py \
    --read_only \
    --log_dir $_LOGDIR \
    --log_level $_LOG_LEVEL \
    --skip_history \
    --process_queue \
    --query_queue_batch_size $_QUERY_QUEUE_BATCH_SIZE \
    --query_pool_size $_QUERY_POOL_SIZE \
    --upload_pool_size $_UPLOAD_POOL_SIZE \
    --collectors_file $SPIDER_WORKDIR/etc/collectors.json \
    --schedd_filter "vocms0258.cern.ch,vocms0267.cern.ch,crab3@vocms0195.cern.ch,crab3@vocms0196.cern.ch,crab3@vocms0194.cern.ch"

# Submit test
#python scripts/spider_cms.py \
#    --feed_amq \
#    --log_dir $_LOGDIR \
#    --log_level $_LOG_LEVEL \
#    --skip_history \
#    --process_queue \
#    --query_queue_batch_size $_QUERY_QUEUE_BATCH_SIZE \
#    --query_pool_size $_QUERY_POOL_SIZE \
#    --upload_pool_size $_UPLOAD_POOL_SIZE \
#    --collectors_file $SPIDER_WORKDIR/etc/collectors.json
