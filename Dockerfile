FROM python:3.7-buster

WORKDIR /srv
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY cdc_kafka cdc_kafka

ENTRYPOINT ["python", "-m", "cdc_kafka"]
