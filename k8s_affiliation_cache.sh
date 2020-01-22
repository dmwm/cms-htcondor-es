#!/bin/bash
##H Usage: k8s_affiliation_cache.sh <output_file>
##H
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
    if [ -f $ofile ]; then
        break
    else
        echo "Unable to create $ofile in $i attempt"
    fi
done
if [ -f $ofile ]; then
    echo "Create k8s affliations configmap"
    kubectl create configmap affiliations \
        --from-file=$ofile --save-config --dry-run -o yaml | \
        kubectl apply --validate=false -f -
fi
