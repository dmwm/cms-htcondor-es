#!/bin/bash
# Author: Christian Ariza <christian.ariza AT gmail [DOT] com>

api_key=$GRAFANA_VIEWER_TOKEN
base_htcondor="/home/cmsjobmon/cms-htcondor-es"
history_log="$base_htcondor/log_history/spider_cms.log"
queues_log="$base_htcondor/log/spider_cms.log"

unknown=$(
    tail -5000 $history_log
    tail -5000 $queues_log | grep -P -o1 '(?<=key=)([^,]*)' | sort | uniq
)
# Currently not in the json
declare -a missing
while read -r line; do grep -q \""${line}"\" "$base_htcondor/JobMonitoring.json" || missing+=("$line"); done <<<"$unknown"

# echo "Currently missing fields:"
# printf '%s\n' "${missing[@]}"

for i in "${missing[@]}"; do
    esjson=$(curl -s -XGET 'https://monit-grafana.cern.ch/api/datasources/proxy/8787/_search?pretty' -H "Authorization: Bearer ${api_key}" -d "
{
    \"aggs\" : {
       \"find\" : {
            \"terms\" : { \"field\" : \"data.$i\", \"size\":1 }
        }
    },
    \"size\" : 0
}")

    nel=$(echo "$esjson" | jq '.aggregations.find.buckets|length')
    if [[ nel -gt 0 ]]; then
        echo "\"$i\": $(echo "$esjson" | jq '.aggregations.find.buckets[0].key'),"
    else
        (echo >&2 "$i NOT FOUND")
    fi
done
