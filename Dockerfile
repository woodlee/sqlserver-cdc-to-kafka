FROM python:3.7-buster

WORKDIR /srv

RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
  && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
  && apt-get update \
  && ACCEPT_EULA=Y apt-get install -y unixodbc-dev msodbcsql17

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY cdc_kafka cdc_kafka

STOPSIGNAL SIGINT

CMD ["python", "-m", "cdc_kafka"]
