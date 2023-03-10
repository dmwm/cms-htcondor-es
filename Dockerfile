FROM cern/cc7-base:20230201-1.x86_64 as cern

# Do not use python:alpine because of long build time and some bugs
# Prodcution spider uses 3.6
FROM python:3.6-slim

ENV HOME "/home/cmsjobmon"
ENV PATH "${PATH}:/usr/bin/"
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
ENV CONDOR_CONFIG "/dev/null"
ENV REQUESTS_CA_BUNDLE "/etc/pki/ca-trust/extracted/openssl/ca-bundle.trust.crt"
ENV PYTHONPATH "${PYTHONPATH}:/$HOME/cms-htcondor-es/src"

# Add cmsjobmon user and create its home directory
RUN useradd --system --group root --uid 1414 -m -d $HOME -s /bin/bash cmsjobmon

WORKDIR $HOME/cms-htcondor-es

# Install latest kubectl for using with crons
ARG kubectl_stable_version=https://storage.googleapis.com/kubernetes-release/release/stable.txt

# Requiered for CRIC (fetching affiliations)
COPY --from=cern /etc/pki/ca-trust/extracted/openssl/ca-bundle.trust.crt /etc/pki/ca-trust/extracted/openssl/ca-bundle.trust.crt
COPY --from=cern /etc/pki/tls/certs/CERN-bundle.pem /etc/pki/tls/certs/CERN-bundle.pem
# ES ca_certs
COPY --from=cern /etc/pki/tls/certs/ca-bundle.trust.crt /etc/pki/tls/certs/ca-bundle.trust.crt
COPY . $HOME/cms-htcondor-es

# Create collectors.json for TEST
RUN mkdir $HOME/.globus && \
    mkdir etc && \
    echo '{\n\
    "Global":["cmsgwms-collector-global.fnal.gov:9620"],\n\
    "Volunteer":["vocms0840.cern.ch"],\n\
    "ITB":["cmsgwms-collector-itb.cern.ch"]\n\
}\n'> ./etc/collectors.json

# openssl is required for python requirements module. git procps unzip libaio1 wget are exist to be able to debug k8s pod
RUN apt-get update && apt-get install -y bash apt-utils git curl procps unzip libaio1 wget openssl nano jq vim cron && \
    curl -k -O -L https://storage.googleapis.com/kubernetes-release/release/$(curl -s $kubectl_stable_version)/bin/linux/amd64/kubectl && \
    mv kubectl /usr/bin && \
    chmod +x /usr/bin/kubectl && \
    chown -R cmsjobmon $HOME && \
    python3 -m venv $HOME/cms-htcondor-es/venv3_6 && \
    $HOME/cms-htcondor-es/venv3_6/bin/pip install --no-cache-dir -r $HOME/cms-htcondor-es/requirements.txt

USER cmsjobmon

#  Docker how to:
#  - docker build -t cms-htcondor-es .
#  - docker run --name test -it cms-htcondor-es /bin/bash
#      - In another shell:
#      - docker cp userkey.pem test:/home/cmsjobmon/.globus/
#      - docker cp usercert.pem test:/home/cmsjobmon/.globus/
#      - docker exec -it --user root test chown -R cmsjobmon /home
#      - docker exec -it --user root test cron &
#  - Don't forget to create "es.conf" file
#  - ctrl-p + ctrl+q sequence keeps container running in background
