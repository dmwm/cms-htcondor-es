#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export PYTHONPATH="$DIR/src/:$DIR/../CMSMonitoring/src/python:$PYTHONPATH"
source "$DIR/venv/bin/activate"
export REQUESTS_CA_BUNDLE="/etc/pki/ca-trust/extracted/openssl/ca-bundle.trust.crt"
create=$(cat <<EOF
import os
from htcondor_es.AffiliationManager import AffiliationManager, AffiliationManagerException
try:
    AffiliationManager(recreate_older_days=1, dir_file=os.getenv('AFFILIATION_DIR_LOCATION', AffiliationManager._AffiliationManager__DEFAULT_DIR_PATH))
except AffiliationManagerException as e:
    import traceback
    traceback.print_exc()
    print('There was an error creating the affiliation manager')
EOF
)
python -c "$create"
