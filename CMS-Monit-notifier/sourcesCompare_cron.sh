#!/bin/bash
# Historical compare scripts for InfluxDB to ES migration
# Author: Christian Ariza Porras
#
# Copied from vocms0240:/home/cmsjobmon/scripts

NOTIFIER_HOME="/home/cmsjobmon/cms-htcondor-es/CMS-Monit-notifier"
source "$NOTIFIER_HOME/.venv/bin/activate"
export PYTHONPATH="$NOTIFIER_HOME/:$PYTHONPATH"

res="$("$NOTIFIER_HOME"/sourcesCompare.sh 2>&1)";python -m monit_notifier.notifier --config_file "$NOTIFIER_HOME/monit_notifier/templates/cmsCompNotification.json" --case "$?" <<<"$res"
