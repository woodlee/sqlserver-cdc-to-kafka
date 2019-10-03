FROM python:3.7-buster

RUN pip install Cython==0.29.13
RUN pip install \
  appnope==0.1.0 \
  avro-python3==1.8.2 \
  backcall==0.1.0 \
  certifi==2019.9.11 \
  chardet==3.0.4 \
  confluent-kafka==1.1.0 \
  decorator==4.4.0 \
  fastavro==0.22.5 \
  idna==2.8 \
  ipython==7.8.0 \
  ipython-genutils==0.2.0 \
  jedi==0.15.1 \
  parso==0.5.1 \
  pexpect==4.7.0 \
  pickleshare==0.7.5 \
  prompt-toolkit==2.0.9 \
  ptyprocess==0.6.0 \
  Pygments==2.4.2 \
  pymssql==2.1.4 \
  python-dateutil==2.8.0 \
  requests==2.22.0 \
  six==1.12.0 \
  SQLAlchemy==1.3.8 \
  traitlets==4.3.2 \
  urllib3==1.25.6 \
  wcwidth==0.1.7

COPY cdc_kafka /srv/cdc_kafka
WORKDIR /srv

ENTRYPOINT ["python", "-m", "cdc_kafka"]
