FROM cern/cc7-base:20210501-2.x86_64 as cern

# Do not use python:alpine because of long build time and some bugs
FROM python:3.9-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

# Install latest kubectl for using with crons
ARG kubectl_stable_version=https://storage.googleapis.com/kubernetes-release/release/stable.txt

# openssl is required for python requirements module. git procps unzip libaio1 wget are exist to be able to debug k8s pod
RUN apt-get update && apt-get install -y apt-utils git curl procps unzip libaio1 wget openssl && \
        curl -k -O -L https://storage.googleapis.com/kubernetes-release/release/$(curl -s $kubectl_stable_version)/bin/linux/amd64/kubectl && \
        mv kubectl /usr/bin && \
        chmod +x /usr/bin/kubectl

ENV PATH "${PATH}:/usr/bin/"

# Requiered for CRIC (fetching affiliations)
COPY --from=cern /etc/pki/ca-trust/extracted/openssl/ca-bundle.trust.crt /etc/pki/ca-trust/extracted/openssl/ca-bundle.trust.crt

ENV REQUESTS_CA_BUNDLE "/etc/pki/ca-trust/extracted/openssl/ca-bundle.trust.crt"

# copy spider codebase
COPY . /cms-htcondor-es
WORKDIR /cms-htcondor-es
RUN  pip install --no-cache-dir -r /cms-htcondor-es/requirements.txt
ENV PYTHONPATH "${PYTHONPATH}:/cms-htcondor-es/src"
RUN useradd --uid 1414 -ms /bin/bash spider && chown -R spider /cms-htcondor-es
USER spider
