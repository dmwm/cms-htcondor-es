#!/bin/bash
##H Usage:
##H        k8s_affiliation_cache.sh <output_file>
##H Accepts only one parameter which is the output affiliations json file.
##H        Runs affiliation_cache.py which query crics and writes results to affiliation json file.
##H Usage in k8s:
##H        https://github.com/dmwm/CMSKubernetes/blob/master/kubernetes/spider/cronjobs/spider-cron-affiliation.yaml
set -e # exit script if error occurs

# help definition
if [ "$1" == "-h" ] || [ "$1" == "-help" ] || [ "$1" == "--help" ] || [ "$1" == "help" ] || [ "$1" == "" ]; then
    perl -ne '/^##H/ && do { s/^##H ?//; print }' < $0
    exit 1
fi

ofile=$1
if [ -f $ofile ]; then
    rm $ofile
fi
for i in 1 2 3 4 5
do
    python affiliation_cache.py --output=$ofile
    sleep 5s # In case of write buffer wait time or retry
    if [ -f $ofile ]; then # If file exist, break the retry loop
        break
    else
        echo "Unable to create $ofile in $i attempt"
    fi
done
