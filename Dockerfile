FROM python:3.12-slim-bookworm

WORKDIR /srv

RUN apt-get update \
  && apt-get install -y --no-install-recommends curl gnupg \
  && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
  && echo "deb [signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
  && apt-get update \
  && ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
       unixodbc-dev \
       msodbcsql18 \
       dumb-init \
       python3-dev \
       freetds-dev \
       build-essential \
       gcc \
       g++ \
  && apt-get purge -y --auto-remove curl gnupg \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --shell /bin/bash appuser

COPY pyproject.toml .
RUN pip install --no-cache-dir --upgrade pip \
  && pip install --no-cache-dir .[replayer]

COPY --chown=appuser:appuser cdc_kafka cdc_kafka
COPY --chown=appuser:appuser replayer replayer

# Switch to non-root user
USER appuser

ENTRYPOINT ["dumb-init", "--rewrite", "15:2", "--"]
CMD ["python", "-m", "cdc_kafka"]
