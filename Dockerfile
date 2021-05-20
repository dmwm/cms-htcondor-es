FROM cern/cc7-base:20210501-2.x86_64 as cern
FROM python:3.8.10-slim

RUN apt-get update && apt-get install -y apt-utils git curl procps unzip libaio1 wget openssl
ENV PATH "${PATH}:/usr/bin/"
COPY --from=cern /etc/pki/ca-trust/extracted/openssl/ca-bundle.trust.crt /etc/pki/ca-trust/extracted/openssl/ca-bundle.trust.crt
COPY . /cms-htcondor-es
WORKDIR /cms-htcondor-es

ENV REQUESTS_CA_BUNDLE "/etc/pki/ca-trust/extracted/openssl/ca-bundle.trust.crt"
RUN  pip install -r /cms-htcondor-es/requirements.txt
ENV PYTHONPATH "${PYTHONPATH}:/cms-htcondor-es/src"

# Install latest kubectl for using with crons
RUN curl -k -O -L https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl && \
mv kubectl /usr/bin && \
chmod +x /usr/bin/kubectl

# copy spider codebase
RUN useradd --uid 1414 -ms /bin/bash spider && chown -R spider /cms-htcondor-es
USER spider

