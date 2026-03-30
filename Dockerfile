FROM python:3.12-slim-trixie

WORKDIR /srv

RUN apt-get update \
  && apt-get install -y --no-install-recommends curl \
  && curl -sSL -O https://packages.microsoft.com/config/debian/$(grep VERSION_ID /etc/os-release | cut -d '"' -f 2 | cut -d '.' -f 1)/packages-microsoft-prod.deb \
  && dpkg -i packages-microsoft-prod.deb \
  && rm packages-microsoft-prod.deb

RUN apt-get update \
  && ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
       unixodbc-dev \
       msodbcsql18 \
       dumb-init \
       python3-dev \
       freetds-dev \
       build-essential \
       gcc \
       g++ \
  && apt-get purge -y --auto-remove curl \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
RUN pip install --no-cache-dir --upgrade pip \
  && pip install --no-cache-dir .[replayer]

COPY cdc_kafka cdc_kafka

ENTRYPOINT ["dumb-init", "--rewrite", "15:2", "--"]
CMD ["python", "-m", "cdc_kafka"]
