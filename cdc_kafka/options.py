import argparse
import importlib
import os
import socket


def str2bool(v: str) -> bool:
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


def get_options_and_reporters():
    p = argparse.ArgumentParser()

    # Required
    p.add_argument('--db-conn-string',
                   default=os.environ.get('DB_CONN_STRING'),
                   help='ODBC connection string for the DB from which you wish to consume CDC logs')

    p.add_argument('--kafka-bootstrap-servers',
                   default=os.environ.get('KAFKA_BOOTSTRAP_SERVERS'),
                   help='Host and port for your Kafka cluster, e.g. "localhost:9092"')

    p.add_argument('--schema-registry-url',
                   default=os.environ.get('SCHEMA_REGISTRY_URL'),
                   help='URL to your Confluent Schema Registry, e.g. "http://localhost:8081"')

    # Optional
    p.add_argument('--kafka-timeout-seconds',
                   type=int,
                   default=os.environ.get('KAFKA_TIMEOUT_SECONDS', 10),
                   help="Timeout to use when interacting with Kafka during startup for checking topic watermarks and "
                        "pulling messages to check existing progress.")

    p.add_argument('--extra-kafka-consumer-config',
                   default=os.environ.get('EXTRA_KAFKA_CONSUMER_CONFIG'),
                   help="Additional config parameters to be used when instantiating the Kafka consumer (used only for "
                        "checking saved progress upon startup, and when in validation mode). Should be a "
                        "semicolon-separated list of colon-delimited librdkafka config key:value pairs, e.g. "
                        "'queued.max.messages.kbytes:500000;fetch.wait.max.ms:250'")

    p.add_argument('--extra-kafka-producer-config',
                   default=os.environ.get('EXTRA_KAFKA_PRODUCER_CONFIG'),
                   help="Additional config parameters to be used when instantiating the Kafka producer. Should be a "
                        "semicolon-separated list of colon-delimited librdkafka config key:value pairs, e.g. "
                        "'linger.ms:200;retry.backoff.ms:250'")

    p.add_argument('--table-blacklist-regex',
                   default=os.environ.get('TABLE_BLACKLIST_REGEX'),
                   help="A regex used to blacklist any tables that are tracked by CDC in your DB, but for which you "
                        "don't wish to publish data using this tool. Tables names are specified in dot-separated "
                        "'schema_name.table_name' form. Applied after the whitelist, if specified.")

    p.add_argument('--table-whitelist-regex',
                   default=os.environ.get('TABLE_WHITELIST_REGEX'),
                   help="A regex used to whitelist the specific CDC-tracked tables in your DB that you wish to publish "
                        "data for with this tool. Tables names are specified in dot-separated 'schema_name.table_name' "
                        "form.")

    p.add_argument('--topic-name-template',
                   default=os.environ.get('TOPIC_NAME_TEMPLATE', '{schema_name}_{table_name}_cdc'),
                   help="Template by which the Kafka topics will be named. Uses curly braces to specify substituted "
                        "values. Values available for substitution are `schema_name`, `table_name`, and `capture_"
                        "instance_name`.")

    p.add_argument('--snapshot-table-blacklist-regex',
                   default=os.environ.get('SNAPSHOT_TABLE_BLACKLIST_REGEX'),
                   help="A regex used to blacklist any tables for which you don't want to do a full initial-snapshot "
                        "read, in the case that this tool is being applied against them for the first time. Table "
                        "names are specified in dot-separated 'schema_name.table_name' form. Applied after the "
                        "whitelist, if specified.")

    p.add_argument('--snapshot-table-whitelist-regex',
                   default=os.environ.get('SNAPSHOT_TABLE_WHITELIST_REGEX'),
                   help="A regex used to whitelist the specific tables for which you want to do a full initial-"
                        "snapshot read, in the case that this tool is being applied against them for the first time. "
                        "Tables names are specified in dot-separated 'schema_name.table_name' form.")

    p.add_argument('--capture-instance-version-strategy',
                   choices=('regex', 'create_date'),
                   default=os.environ.get('CAPTURE_INSTANCE_VERSION_STRATEGY', 'create_date'),
                   help="If there is more than one capture instance following a given source table, how do you want to "
                        "select which one this tool reads? `create_date` (the default) will follow the one most "
                        "recently created. `regex` allows you to specify a regex against the capture instance name "
                        "(as argument `capture-instance-version-regex`, the first captured group of which will be used "
                        "in a lexicographic ordering of capture instance names to select the highest one. This can be "
                        "useful if your capture instance names have a version number in them.")

    p.add_argument('--capture-instance-version-regex',
                   default=os.environ.get('CAPTURE_INSTANCE_VERSION_REGEX'),
                   help="Regex to use if specifying option `regex` for argument `capture-instance-version-strategy`")

    p.add_argument('--progress-topic-name',
                   default=os.environ.get('PROGRESS_TOPIC_NAME', '_cdc_to_kafka_progress'),
                   help="Name of the topic used to store progress details reading change tables (and also source "
                        "tables, in the case of snapshots). This process will create the topic if it does not yet "
                        "exist. It should have only one partition.")

    p.add_argument('--disable-deletion-tombstones',
                   type=str2bool, nargs='?', const=True,
                   default=str2bool(os.environ.get('DISABLE_DELETION_TOMBSTONES', False)),
                   help="Name of the topic used to store progress details reading change tables (and also source "
                        "tables, in the case of snapshots). This process will create the topic if it does not yet "
                        "exist. It should have only one partition.")

    p.add_argument('--lsn-gap-handling',
                   choices=('raise_exception', 'begin_new_snapshot', 'ignore'),
                   default=os.environ.get('LSN_GAP_HANDLING', 'raise_exception'),
                   help="Controls what happens if the earliest available change LSN in a capture instance is after "
                        "the LSN of the latest change published to Kafka.")

    p.add_argument('--run-validations',
                   type=str2bool, nargs='?', const=True,
                   default=str2bool(os.environ.get('RUN_VALIDATIONS', False)),
                   help="Runs count validations between messages in the Kafka topic and rows in the change and "
                        "source tables, then quits. Respects the table whitelist/blacklist regexes.")

    p.add_argument('--metrics-reporters',
                   default=os.environ.get('METRICS_REPORTERS',
                                          'cdc_kafka.metric_reporting.stdout_reporter.StdoutReporter'),
                   help="Comma-separated list of <module_name>.<class_name>s of metric reporters you want this app "
                        "to emit to.")

    p.add_argument('--metrics-reporting-interval',
                   type=int,
                   default=os.environ.get('METRICS_REPORTING_INTERVAL', 15),
                   help="Interval in seconds between calls to metric reporters.")

    p.add_argument('--metrics-namespace',
                   default=os.environ.get('METRICS_NAMESPACE', socket.getfqdn()),
                   help="Namespace used to key metrics for certain metric reporters, and which is embedded in the "
                        "metrics payload as well. Useful if multiple CDC-to-Kafka instances are emitting metrics to "
                        "the same destination. Defaults to the value returned by `socket.getfqdn()`.")

    p.add_argument('--process-hostname',
                   default=os.environ.get('PROCESS_HOSTNAME', socket.getfqdn()),
                   help="Hostname inserted into progress-tracking and metrics metadata messages. "
                        "Defaults to the value returned by `socket.getfqdn()`.")

    p.add_argument('--partition-count',
                   type=int,
                   default=os.environ.get('PARTITION_COUNT'),
                   help="Number of partitions to specify when creating new topics. If left empty, defaults to 2 or "
                        "the daily average number of rows currently on the change table integer-divided by 500,000, "
                        "whichever is larger.")

    p.add_argument('--replication-factor',
                   type=int,
                   default=os.environ.get('REPLICATION_FACTOR'),
                   help="Replication factor to specify when creating new topics. If left empty, defaults to 3 or the "
                        "number of brokers in the cluster, whichever is smaller.")

    p.add_argument('--extra-topic-config',
                   default=os.environ.get('EXTRA_TOPIC_CONFIG'),
                   help="Additional config parameters to be used when creating new topics. Should be a "
                        "semicolon-separated list of colon-delimited librdkafka config key:value pairs, e.g. "
                        "'min.insync.replicas:2'")

    p.add_argument('--progress-csv-path',
                   default=os.environ.get('PROGRESS_CSV_PATH'),
                   help="File path to which CSV files will be written with data read from the existing progress "
                        "topic at startup time.")

    opts = p.parse_args()

    reporters = []
    if opts.metrics_reporters:
        for class_path in opts.metrics_reporters.split(','):
            package_module, class_name = class_path.rsplit('.', 1)
            module = importlib.import_module(package_module)
            reporter = getattr(module, class_name)()
            reporters.append(reporter)
            reporter.add_arguments(p)

        opts = p.parse_args()

        for reporter in reporters:
            reporter.set_options(opts)

    return opts, reporters
