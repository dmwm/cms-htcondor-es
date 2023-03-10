# PRODUCTION

## Prod VENV

- Current virtual machine has python3.6 and before deployment, new virtual environment should be created with `venv3_6`
  name.
- And requirements should be installed before start.
- To make migrations easy, please use meaningful "venv" names

```
cd /home/cmsjobmon/cms-htcondor-es
python3 -m venv venv3_6
./venv3_6/bin/pip install --no-cache-dir -r requirements.txt
```

## Prod Crontab

```
# For NEW directory structure

*/12 * * * * /home/cmsjobmon/cms-htcondor-es/scripts/spider_cms_queues.sh
5-59/12 * * * * /home/cmsjobmon/cms-htcondor-es/scripts/spider_cms_history.sh
0 3 * * * /bin/bash "/home/cmsjobmon/cms-htcondor-es/scripts/cronAffiliation.sh"
# 0 * * * * /bin/bash "/home/cmsjobmon/scripts/sourcesCompare_cron.sh";
```

```
# For OLD directory structure

*/12 * * * * /home/cmsjobmon/cms-htcondor-es/spider_cms_queues.sh
5-59/12 * * * * /home/cmsjobmon/cms-htcondor-es/spider_cms_history.sh
0 3 * * * /bin/bash "/home/cmsjobmon/cms-htcondor-es/cronAffiliation.sh"
0 * * * * /bin/bash "/home/cmsjobmon/scripts/sourcesCompare_cron.sh";
```

## Prod required directory and file paths

Used environment variables in scripts:

```
export SPIDER_WORKDIR="/home/cmsjobmon/cms-htcondor-es"
export AFFILIATION_DIR_LOCATION="$SPIDER_WORKDIR/.affiliation_dir.json"
```

###### Under HOME `/home/cmsjobmon`

- `SPIDER_WORKDIR`            : `$HOME/cms-htcondor-es`

###### Under SPIDER_WORKDIR

| file                        | parametrized value                      |
|:----------------------------|:----------------------------------------|
| `amq_username`              | `$SPIDER_WORKDIR/etc/amq_username`      |
| `amq_password`              | `$SPIDER_WORKDIR/etc/amq_password`      |
| `.affiliation_dir.json`     | `$SPIDER_WORKDIR/.affiliation_dir.json` |
| `checkpoint.json`           | `$SPIDER_WORKDIR/checkpoint.json`       |
| `collectors.json`           | `$SPIDER_WORKDIR/etc/collectors.json`   |
| `es_conf.json`              | `$SPIDER_WORKDIR/etc/es_conf.json`      |
| `es.conf`                   | `$SPIDER_WORKDIR/es.conf`(deprecated)   |
| `JobMonitoring.json`        | `$SPIDER_WORKDIR/JobMonitoring.json`    |
| `last_mappings.json`        | `$SPIDER_WORKDIR/last_mappings.json`    |
| `log, log_history, log_aff` | `$SPIDER_WORKDIR/log*`                  |
| `venv`                      | `$SPIDER_WORKDIR/venv`(will deprecate)  |
| `venv3_6`                   | `$SPIDER_WORKDIR/venv3_6`               |

## Prod es_conf.json format

**CONVENTIONS:**

- Host name ends with `/es` refers to **OpenSearch** otherwise it is behaved as **ElasticSearch** and port
  should be provided.
- No need to provide port for OpenSearch.
- Supports multiple clusters and post data to all of them.
- Do not put `https://` prefix.

```json
[
  {
    "host": "es-cmsX(ElasticSearch).cern.ch",
    "port": 9203,
    "username": "user",
    "password": "pass"
  },
  {
    "host": "es-cmsY(OpenSearch).cern.ch/es",
    "username": "user",
    "password": "pass"
  }
]
```

---

# MIGRATION AND DISASTER RECOVERY

### Migration

- Prepare all the production requirements: directory/files and venv.
- **Take backup** of `$SPIDER_WORKDIR/checkpoint.json` as `$SPIDER_WORKDIR/checkpoint.json.back`.
- Set new crontab and let them start.

### Disaster Recovery

**Scenario-1: Data send wrongly**

- Rollback to previous commit, directory&file structure and venv.
- Rollback to back-up checkpoint: `mv checkpoint.json.back checkpoint.json`.
- Delete documents from es-cms.cern.ch `cms-*` indices with `metadata.spider_git_hash='problematic_hash'`.
- Ask MONIT to delete documents from monit-opensearch.cern.ch `monit_prod_condor_raw_metrics*` indices
  with `data.metadata.spider_git_hash='problematic_hash'`.
- Inform and announce HDFS users to filter out `metadata.spider_git_hash='problematic_hash'` in their Spark jobs.
    - HDFS data deletion: it is not easy because of Flume data. It can be deleted after a couple of days later. Discuss
      with MONIT.
- Data loss is inevitable(12m or more) for Condor Job Monitoring schedds Queue data (Running, Held, Idle, etc.).
- No data loss for schedds History data (completed, etc.) because of checkpoint.json if required actions are taken
  immediately.

**Scenario-2: not worked at all**

- Rollback to previous commit, directory&file structure and venv.
- Rollback to back-up checkpoint: `mv checkpoint.json.back checkpoint.json`.
- Data loss is inevitable(12m or more) for Condor Job Monitoring schedds Queue data (Running, Held, Idle, etc.).
- No data loss for schedds History data (Completed, etc.) because of checkpoint.json if required actions are taken
  immediately.

**Scenario-3: failed after some time later**

In any case, data loss is inevitable for Condor Job Monitoring schedds Queue data (Running, Held, Idle, etc.)

- If problem is noticed within 12 hours, no action needed. Just fix the problem and continue to run.
- If problem is noticed later than 12 hours, set `--history_query_max_n_minutes=12*60` parameter
  in `spider_cms_history.sh` to a reasonable time window to include last checkpoints of `checkpoint.json` to fill the
  schedds History results. Else, data loss will be in case for schedds History data. It is because of a safety measure:
  even if schedds have previous checkpoint time than `history_query_max_n_minutes`, their checkpoints will be set
  to this max time window. It can be tweaked in disaster scenarios.

---

# Test

Use Dockerfile, test scripts and `test/debugcli.py`. Everything you need is there.

#### Elasticsearch help utils

```
# Get indices
curl -s -XGET -u $user:$pass https://es-cms.cern.ch:9203/_cat/indices/ | grep ceyhun | sort

# Delete one indice
curl -s -XDELETE -u $user:$pass https://es-cms.cern.ch:9203/cms-test-ceyhun-2023-02-21

# Deletion command(NO RUN, just echo) for multiple indices with regex
echo 'curl -XDELETE  -u $user:$pass' https://es-cms.cern.ch:9203/"$(
    curl -s -XGET -u $user:$pass https://es-cms.cern.ch:9203/_cat/indices/cms-test-ceyhun* | sort | awk '{ORS=","; print $3}')"
```

#### Opensearch help utils

```
# Get indices
curl -s -XGET --negotiate -u $user:$pass https://es-cms1.cern.ch/es/_cat/indices | grep ceyhun | sort

# Delete one indice
curl -s -XGET --negotiate -u $user:$pass https://es-cms1.cern.ch/es/cms-test-ceyhun-2023-02-21

# Deletion command(NO RUN, just echo) for multiple indices with regex
echo 'curl -XDELETE --negotiate -u $user:$pass' https://es-cms1.cern.ch/es/"$(
     curl -s -XGET --negotiate -u $user:$pass https://es-cms1.cern.ch/es/_cat/indices/cms-test-ceyhun* | sort | awk '{ORS=","; print $3}'
)"
```

