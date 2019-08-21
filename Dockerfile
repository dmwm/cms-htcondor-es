FROM cern/cc7-base:20190724
COPY ./requirements.txt /cms-htcondor-es/requirements.txt
RUN yum install -y git sudo python36 python36-virtualenv python36-pip && \
ln -fs /usr/bin/python3 /usr/bin/python && \
ln -fs /usr/bin/pip3.6 /usr/bin/pip &&\
pip install -r /cms-htcondor-es/requirements.txt
ENV REQUESTS_CA_BUNDLE "/etc/pki/ca-trust/extracted/openssl/ca-bundle.trust.crt"
ENV PYTHONPATH "${PYTHONPATH}:/cms-htcondor-es/src"

RUN curl -k -O -L https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl
RUN chmod +x /usr/bin/kubectl
RUN mv kubectl /usr/bin
COPY . /cms-htcondor-es
RUN useradd --uid 1414 -ms /bin/bash spider &&\
chown -R spider /cms-htcondor-es
USER spider
WORKDIR /cms-htcondor-es