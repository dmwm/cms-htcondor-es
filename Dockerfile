FROM cern/cc7-base:20190724
COPY ./requirements.txt /cms-htcondor-es/requirements.txt
RUN yum install -y git python36 python36-virtualenv python36-pip && \
ln -fs /usr/bin/python3 /usr/bin/python && \
ln -fs /usr/bin/pip3.6 /usr/bin/pip &&\
pip install -r /cms-htcondor-es/requirements.txt
COPY . /cms-htcondor-es
RUN useradd --uid 1414 -ms /bin/bash spider &&\
chown -R spider /cms-htcondor-es
USER spider
ENV PYTHONPATH "${PYTHONPATH}:/cms-htcondor-es/src"
ENV REQUESTS_CA_BUNDLE "/etc/pki/ca-trust/extracted/openssl/ca-bundle.trust.crt"
WORKDIR /cms-htcondor-es
ENTRYPOINT ["/usr/bin/python", "/cms-htcondor-es/spider_cms.py"]
