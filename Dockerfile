FROM python:3.12-bookworm

WORKDIR /srv

RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
  && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list \
  && apt-get update \
  && ACCEPT_EULA=Y apt-get install -y unixodbc-dev msodbcsql18 dumb-init

COPY requirements.txt .
RUN pip install -r requirements.txt
COPY cdc_kafka cdc_kafka

ENTRYPOINT ["dumb-init", "--rewrite", "15:2", "--"]
CMD ["python", "-m", "cdc_kafka"]
