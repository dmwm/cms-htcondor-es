#!/bin/bash

export SPIDER_WORKDIR="/home/cmsjobmon/cms-htcondor-es"
export AFFILIATION_DIR_LOCATION="$SPIDER_WORKDIR/.affiliation_dir.json"
export PYTHONPATH="$SPIDER_WORKDIR/src/:$PYTHONPATH"
export REQUESTS_CA_BUNDLE="/etc/pki/ca-trust/extracted/openssl/ca-bundle.trust.crt"

cd $SPIDER_WORKDIR || exit
source "$SPIDER_WORKDIR/venv3_6/bin/activate"

python scripts/affiliation_cache.py --output "$AFFILIATION_DIR_LOCATION" --days 1
