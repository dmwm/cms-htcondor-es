### Setup

We have set up a logstash/filebeat instance on vocms0240.cern.ch to feed log files from running spider_cms cron job processes to
https://es-cms.cern.ch (alias to es-cms1.cern.ch) `logmon` tenant to monitor. The way it works is that filebeat tails the logfiles and preformats the
harvested messages, adding tags about the hostname, log-path, etc, and then feeds them to logstash. Logstash attempts to
match the messages to a set of known formats, which extracts information from the messages, stores it in fields, and
sets a `message_type` field.

For example, the spider_cms script first logs a message about how many schedds are to be processed, in the following
format:

```
2019-03-26 09:48:02,689 : root:WARNING - &&& There are 61 schedds to query.
```

Logstash does a match, using grok/regex patterns defined in `/etc/logstash/conf.d/logstash.conf`, and extracts
a `@timestamp` field from the timestamp, a `log-source` (`'root'`), a `log-level` (`'WARNING'`),
a `message` (`'&&& There are 61 schedds to query.'`), and a custom `n_schedds` (`61`) field. It also adds
a `message_type` for each format (`'script_start'` in this case).

Filebeat automatically adds information about the host, the source log file, the offset in the file, etc. It also adds a
custom field called `spider_source`, either `'condor_history'` or `'condor_queue'`, for the two files we are currently
watching:

```
/home/cmsjobmon/cms-htcondor-es/log/spider_cms.log
/home/cmsjobmon/cms-htcondor-es/log_history/spider_cms.log
```

***

## Logstash

We manage Logstash manually as a systemctl service. It is installed manually and all production settings can be
found in this repository.

- `systemctl` main service config: `/usr/lib/systemd/system/logstash.service`.
- Historically, you can find service settings in:
    - `/etc/systemd/system/logstash.service`
    - `/etc/systemd/system/multi-user.target.wants/logstash.service`
- Current version: `8.5.2`
- Bin directory: `/usr/share/logstash`
- ALL config directory: `/etc/logstash`
- Log directory: `/var/log/logstash`
- Registry `/var/lib/logstash/`
- OpenSearch user redentials in `secrets/es-cms-opensearch/logmon_spider_tenant_secret`

#### How to install Logstash

```
# Be a root
sudo su
cd /usr/share

# Get OSS version
wget https://artifacts.elastic.co/downloads/logstash/logstash-oss-8.6.2-linux-x86_64.tar.gz
tar xvf logstash-oss-8.6.2-linux-x86_64.tar.gz
mv logstash-8.6.2 logstash

# Prepare config files in /etc/logstash
cp -R ~/cms-htcondor-es/service-logstash/logstash/etc /etc/logastash/

# PROVIDE logmon-spider password which can be found in GitLab secrets/es-cms-opensearch/logmon_spider_tenant_secret
cat /etc/logstash/conf.d/logstash.conf | grep -in "<password>"

# In systemctl config, logstash user is "logstash".
chown -R logstash /etc/logstash

systemctl start logstash

# Follow logs and fix if there is a problem [permission problem, new config in jvm, startup options, etc.]
journalctl -u logstash.service -f

# And final log watch
tail -f /var/log/logstash/logstash-plain.log

```

##### Install Logstash OpenSearch plugin

```
/usr/share/logstash/bin/logstash-plugin install logstash-output-opensearch
```

**Note: start logstash before filebeat.**

## Filebeat

We manage Filebeat manually as a systemctl service. It is installed manually and all production settings can be
found in this repository.

As you can see, after OpenSearch migration, we started to use  **Input type: filestream** instead of  **type: log**

- `systemctl` service config: `/usr/lib/systemd/system/filebeat.service` 
- Historically you can find service settings in
    - /etc/rc.d/init.d/filebeat
    - /etc/systemd/system/multi-user.target.wants/filebeat.service
- Current version: `8.7.0`
- Bin directory: `/usr/share/filebeat`
- ALL config directory: `/etc/filebeat`
- Log directory: `/var/log/filebeat`
- Registry `/var/lib/filebeat/` **[IMPORTANT FOR MIGRATIONS, TAKE BACKUP ALWAYS]**

#### How to install Filebeat

```
# Be a root
sudo su
cd /usr/share

# Get OSS version
wget https://artifacts.elastic.co/downloads/beats/filebeat/filebeat-oss-8.6.2-linux-x86_64.tar.gz
tar xvf filebeat-oss-8.6.2-linux-x86_64.tar.gz
mv filebeat-8.6.2 filebeat

# Prepare config files in /etc/logstash
cp -R ~/cms-htcondor-es/service-logstash/filebeat/etc/filebeat.yml /etc/filebeat/filebeat.yml

systemctl start filebeat

# Follow logs
journalctl -u filebeat.service -f
```

## Debugging

```
# Follow which files and bin directories are used
ps aux | grep logstash

# See your service is ok
systemctl status logstash

# See your service logs, you can solve everything from here
journalctl -u logstash.service -f

``` 
