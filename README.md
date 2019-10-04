# sqlserver-cdc-to-kafka

A utility for streaming rows from SQL Server Change Data Capture (CDC) to Kafka topics

This repo is currently a *work in progress* and should not be considered ready for general use.

## Run it

These scenarios presume you are using the provided `docker-compose` file, and that you have pre-provisioned and enable CDC upon a DB called `MyDbName` in the SQL Server DB.

### Locally

```
python -m cdc_kafka \
  --db-conn-string 'mssql+pymssql://sa:TestLocalPassword123@localhost:1433/MyDbName' \
  --kafka-bootstrap-servers localhost:9092 \
  --schema-registry-url http://localhost:8081
```

### Via Docker

```
docker build -t cdc_kafka .

docker run --rm -it \
  --net host  \
  -e DB_CONN_STRING='mssql+pymssql://sa:TestLocalPassword123@localhost:1433/MyDbName' \
  -e KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
  -e SCHEMA_REGISTRY_URL=http://localhost:8081 \
  cdc_kafka 
```
