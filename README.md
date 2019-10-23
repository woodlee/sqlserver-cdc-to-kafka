# sqlserver-cdc-to-kafka

A utility for streaming rows from SQL Server Change Data Capture (CDC) to Kafka topics

This repo is currently a *work in progress* and should not be considered ready for general use.

## Run it

These scenarios presume you are using the provided `docker-compose` file, and that you have pre-provisioned and enabled CDC upon a DB called `MyTestDb` in the SQL Server DB.

You will need to have [installed the Microsoft ODBC Driver for SQL Server](https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver15) for this to work.

### Locally

```
python -m cdc_kafka \
  --db-conn-string 'DRIVER=ODBC Driver 17 for SQL Server; SERVER=localhost; DATABASE=MyTestDb; UID=sa; PWD=TestLocalPassword123' \
  --kafka-bootstrap-servers localhost:9092 \
  --schema-registry-url http://localhost:8081
```

### Via Docker

```
docker build -t cdc_kafka .

docker run --rm -it \
  --net host  \
  -e DB_CONN_STRING='DRIVER=ODBC Driver 17 for SQL Server; SERVER=localhost; DATABASE=MyTestDb; UID=sa; PWD=TestLocalPassword123' \
  -e KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
  -e SCHEMA_REGISTRY_URL=http://localhost:8081 \
  cdc_kafka 
```
