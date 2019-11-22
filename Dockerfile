FROM cern/cc7-base
ADD . /cms-htcondor-es
RUN yum install -y git python36-virtualenv
RUN virtualenv-3 -p python36 /cms-htcondor-es/venv
RUN source /cms-htcondor-es/venv/bin/activate && pip install -r /cms-htcondor-es/requirements.txt

