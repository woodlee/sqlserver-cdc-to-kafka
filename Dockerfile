# WARNING: This codebase may have trouble running on Python versions higher than 3.8, because of new behavior introduced
# by the "fix" in https://github.com/python/cpython/pull/31913. Attempts to run on later Python versions may suffer
# from unpredictable hangs on process exit when the SQLQueryProcessor tries to close/join its multiprocessing.Queues.
FROM python:3.8-bullseye

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
