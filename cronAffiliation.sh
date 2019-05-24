#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export PYTHONPATH="$DIR/src/:$DIR/../CMSMonitoring/src/python:$PYTHONPATH"
source "$DIR/venv/bin/activate"
create=$(cat <<EOF
from htcondor_es.AffiliationManager import AffiliationManager, AffiliationManagerException
try:
    AffiliationManager(recreate_older_days=1)
except AffiliationManagerException as e:
    import traceback
    traceback.print_exc()
    print('There was an error creating the affiliation manager')
EOF
)
python -c "$create"
