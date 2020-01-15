FROM cern/cc7-base
ADD . /cms-htcondor-es
RUN yum install -y git python36-virtualenv
RUN virtualenv-3 -p python3 /cms-htcondor-es/venv
RUN source /cms-htcondor-es/venv/bin/activate && pip install -r /cms-htcondor-es/requirements.txt
ENV PYTHONPATH=/cms-htcondor-es/venv/lib/python3.6/site-packages:/cms-htcondor-es/src
ENTRYPOINT ["python3", "/cms-htcondor-es/spider_cms.py"]
#CMD ["--help"]
