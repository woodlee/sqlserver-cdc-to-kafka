# sqlserver-cdc-to-kafka

This is a utility for streaming rows from SQL Server Change Data Capture (CDC) to Kafka topics. It also optionally streams initial "snapshot state" of the tables into the same topics, allowing you to effectively create a read replica of your SQL Server tables in some other datastore by consuming and applying messages from the topics this process produces.

If you've landed here, you should check out [Debezium](https://debezium.io/). It's a more mature project maintained by many talented folks! This project was initially developed because of some shortcomings that once existed in Debezium which may now be resolved. In particular, Debezium's SQL Server connector did not deal well with cases where a CDC instance does not capture all of a source table's columns. You might prefer this project if you wish to fork and customize such a tool but are more comfortable working with Python (Debezium, like many Kafka-ecosystem tools, is written in Java). Otherwise, check them out first.

This is still a baby project and does not yet have much documentation. If you'd like to try it, check out the options in `cdc_kafka/options.py`.

Integration with [Sentry](https://sentry.io/welcome/) is included. If you have an account with them and want to use it, set environment variable `SENTRY_DSN` (and optionally `SENTRY_ENVIRONMENT` and `SENTRY_RELEASE`) in accordance with their documentation. If you don't, the Sentry integration will be a no-op.

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
