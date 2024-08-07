import argparse
import importlib
import json
import os
import socket
from typing import Tuple, List, Optional, Callable

from . import constants, kafka_oauth
from .metric_reporting import reporter_base
from .serializers import SerializerAbstract

# String constants for options with discrete choices:
CAPTURE_INSTANCE_VERSION_STRATEGY_REGEX = 'regex'
CAPTURE_INSTANCE_VERSION_STRATEGY_CREATE_DATE = 'create_date'
LSN_GAP_HANDLING_RAISE_EXCEPTION = 'raise_exception'
LSN_GAP_HANDLING_BEGIN_NEW_SNAPSHOT = 'begin_new_snapshot'
LSN_GAP_HANDLING_IGNORE = 'ignore'
NEW_FOLLOW_START_POINT_EARLIEST = 'earliest'
NEW_FOLLOW_START_POINT_LATEST = 'latest'
NEW_CAPTURE_INSTANCE_SNAPSHOT_HANDLING_BEGIN_NEW = 'begin_new_snapshot'
NEW_CAPTURE_INSTANCE_SNAPSHOT_HANDLING_IGNORE = 'ignore'
NEW_CAPTURE_INSTANCE_OVERLAP_HANDLING_REPUBLISH = 'republish_from_new_instance'
NEW_CAPTURE_INSTANCE_OVERLAP_HANDLING_PICKUP = 'start_from_prior_progress'


def str2bool(v: str) -> bool:
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


def get_options_and_metrics_reporters(
        arg_adder: Optional[Callable[[argparse.ArgumentParser, ], None]] = None) -> Tuple[
            argparse.Namespace, List[reporter_base.ReporterBase], SerializerAbstract]:
    p = argparse.ArgumentParser()

    # Required
    p.add_argument('--db-conn-string',
                   default=os.environ.get('DB_CONN_STRING'),
                   help='ODBC connection string for the DB from which you wish to consume CDC logs')

    p.add_argument('--kafka-bootstrap-servers',
                   default=os.environ.get('KAFKA_BOOTSTRAP_SERVERS'),
                   help='Host and port for your Kafka cluster, e.g. "localhost:9092"')

    p.add_argument('--kafka-transactional-id',
                   default=os.environ.get('KAFKA_TRANSACTIONAL_ID'),
                   help='An identifier of your choosing that should stay stable across restarts of a particularly-'
                        'configured deployment of this tool. If for example you have several deployments, each '
                        'pointing at a different source DB or set of tables, each of those should have its own unique '
                        'identifier. This value is passed to the Kafka producer as its `transactional.id` config, '
                        'which is used to guarantee atomic writes across multiple topics, including the topic this '
                        'tool uses to track its own progress against CDC data.')

    # Optional
    p.add_argument('--extra-kafka-consumer-config',
                   default=os.environ.get('EXTRA_KAFKA_CONSUMER_CONFIG', {}), type=json.loads,
                   help='Optional JSON object of additional librdkafka config parameters to be used when instantiating '
                        'the Kafka consumer (used only for checking saved progress upon startup, and when in '
                        'validation mode). For example: '
                        '`{"queued.max.messages.kbytes": "500000", "fetch.wait.max.ms": "250"}`')

    p.add_argument('--extra-kafka-producer-config',
                   default=os.environ.get('EXTRA_KAFKA_PRODUCER_CONFIG', {}), type=json.loads,
                   help='Optional JSON object of additional librdkafka config parameters to be used when instantiating '
                        'the Kafka producer. For example: `{"linger.ms": "200", "retry.backoff.ms": "250"}`')

    p.add_argument('--extra-topic-config',
                   default=os.environ.get('EXTRA_TOPIC_CONFIG', {}), type=json.loads,
                   help='Optional JSON object of additional librdkafka config parameters to be used when creating new '
                        'topics. For example: `{"min.insync.replicas": "2"}`.')

    p.add_argument('--table-exclude-regex',
                   default=os.environ.get('TABLE_EXCLUDE_REGEX'),
                   help="A regex used to exclude any tables that are tracked by CDC in your DB, but for which you "
                        "don't wish to publish data using this tool. Tables names are specified in dot-separated "
                        "'schema_name.table_name' form. Applied after the include regex, if specified.")

    p.add_argument('--table-include-regex',
                   default=os.environ.get('TABLE_INCLUDE_REGEX'),
                   help="A regex used to include the specific CDC-tracked tables in your DB that you wish to publish "
                        "data for with this tool. Tables names are specified in dot-separated 'schema_name.table_name' "
                        "form.")

    p.add_argument('--topic-name-template',
                   default=os.environ.get('TOPIC_NAME_TEMPLATE', '{schema_name}_{table_name}_cdc'),
                   help="Template by which the Kafka topics will be named. Uses curly braces to specify substituted "
                        "values. Values available for substitution are `schema_name`, `table_name`, and `capture_"
                        "instance_name`.")

    p.add_argument('--snapshot-table-exclude-regex',
                   default=os.environ.get('SNAPSHOT_TABLE_EXCLUDE_REGEX'),
                   help="A regex used to exclude any tables for which you don't want to do a full initial-snapshot "
                        "read, in the case that this tool is being applied against them for the first time. Table "
                        "names are specified in dot-separated 'schema_name.table_name' form. Applied after the "
                        "inclusion regex, if specified.")

    p.add_argument('--snapshot-table-include-regex',
                   default=os.environ.get('SNAPSHOT_TABLE_INCLUDE_REGEX'),
                   help="A regex used to include the specific tables for which you want to do a full initial-"
                        "snapshot read, in the case that this tool is being applied against them for the first time. "
                        "Tables names are specified in dot-separated 'schema_name.table_name' form.")

    p.add_argument('--capture-instance-version-strategy',
                   choices=(CAPTURE_INSTANCE_VERSION_STRATEGY_REGEX, CAPTURE_INSTANCE_VERSION_STRATEGY_CREATE_DATE),
                   default=os.environ.get('CAPTURE_INSTANCE_VERSION_STRATEGY',
                                          CAPTURE_INSTANCE_VERSION_STRATEGY_CREATE_DATE),
                   help=f"If there is more than one capture instance following a given source table, how do you want "
                        f"to select which one this tool reads? `{CAPTURE_INSTANCE_VERSION_STRATEGY_CREATE_DATE}` (the "
                        f"default) will follow the one most recently created. "
                        f"`{CAPTURE_INSTANCE_VERSION_STRATEGY_REGEX}` allows you to specify a regex against the "
                        f"capture instance name (as argument `capture-instance-version-regex`, the first captured "
                        f"group of which will be used in a lexicographic ordering of capture instance names to select "
                        f"the highest one. This can be useful if your capture instance names have a version number in "
                        f"them.")

    p.add_argument('--capture-instance-version-regex',
                   default=os.environ.get('CAPTURE_INSTANCE_VERSION_REGEX'),
                   help="Regex to use if specifying option `regex` for argument `capture-instance-version-strategy`")

    p.add_argument('--progress-topic-name',
                   default=os.environ.get('PROGRESS_TOPIC_NAME', '_cdc_to_kafka_progress'),
                   help="Name of the topic used to store progress details reading change tables (and also source "
                        "tables, in the case of snapshots). This process will create the topic if it does not yet "
                        "exist. IMPORTANT: It should have only one partition.")

    p.add_argument('--snapshot-logging-topic-name',
                   default=os.environ.get('SNAPSHOT_LOGGING_TOPIC_NAME'),
                   help="Optional name of a topic which will receive messages logging events related to table "
                        f"snapshots. Logged actions include '{constants.SNAPSHOT_LOG_ACTION_STARTED}', "
                        f"'{constants.SNAPSHOT_LOG_ACTION_RESUMED}', '{constants.SNAPSHOT_LOG_ACTION_COMPLETED}', "
                        f"'{constants.SNAPSHOT_LOG_ACTION_RESET_AUTO}', and "
                        f"'{constants.SNAPSHOT_LOG_ACTION_RESET_MANUAL}'.")

    p.add_argument('--disable-deletion-tombstones',
                   type=str2bool, nargs='?', const=True,
                   default=str2bool(os.environ.get('DISABLE_DELETION_TOMBSTONES', '0')),
                   help="When false (the default), CDC deletion events will lead to emitting two records: one with "
                        "the CDC data and a second with the same key but a null value, to signal Kafka log compaction "
                        "to remove the entry for that key. If set to true, the null-value 'tombstones' are not "
                        "emitted.")

    p.add_argument('--lsn-gap-handling',
                   choices=(LSN_GAP_HANDLING_RAISE_EXCEPTION, LSN_GAP_HANDLING_BEGIN_NEW_SNAPSHOT,
                            LSN_GAP_HANDLING_IGNORE),
                   default=os.environ.get('LSN_GAP_HANDLING', LSN_GAP_HANDLING_RAISE_EXCEPTION),
                   help=f"Controls what happens if the earliest available change LSN in a capture instance is after "
                        f"the LSN of the latest change published to Kafka. Defaults to "
                        f"`{LSN_GAP_HANDLING_RAISE_EXCEPTION}`")

    p.add_argument('--new-follow-start-point',
                   choices=(NEW_FOLLOW_START_POINT_EARLIEST, NEW_FOLLOW_START_POINT_LATEST),
                   default=os.environ.get('NEW_FOLLOW_START_POINT', NEW_FOLLOW_START_POINT_LATEST),
                   help=f"Controls how much change data history to read from SQL Server capture tables, for any tables "
                        f"that are being followed by this process for the first time. Value "
                        f"`{NEW_FOLLOW_START_POINT_EARLIEST}` will pull all existing data from the capture tables; "
                        f"value `{NEW_FOLLOW_START_POINT_EARLIEST}` will only process change data added after this "
                        f"process starts following the table. Note that use of `{NEW_FOLLOW_START_POINT_EARLIEST}` "
                        f"with unified topics may lead to LSN regressions in the sequence of unified topic messages "
                        f"in the case where new tables are added to a previously-tracked set. This setting does not "
                        f"affect the behavior of table snapshots. Defaults to `{NEW_FOLLOW_START_POINT_LATEST}`")

    p.add_argument('--unified-topics',
                   default=os.environ.get('UNIFIED_TOPICS', {}), type=json.loads,
                   help=f'A string that is a JSON object mapping topic names to various configuration parameters as '
                        f'follows: {{"<string: topic name>": {{"included_tables": "<string: a regex, required>", '
                        f'"partition_count": <int: optional, defaults to 1>, "extra_topic_config": {{<JSON object, '
                        f'optional>}}}}, ... }}. For each specified topic name, all change data entries for source '
                        f'tables that match the `included_tables` regex will be produced to the specified topic in '
                        f'globally-consistent LSN order. This means the topic may contain messages with varying '
                        f'schemas, and ideally should be a single-partition topic to simplify in-order consumption of '
                        f'the messages, though if desired this can be overridden via `partition_count` when this '
                        f'process is creating the topic. Similarly `extra_topic_config` can be used to specify '
                        f'additional parameters passed directly to librdkafka at topic creation time (e.g. to specify '
                        f'`retention.ms`). For these topics, snapshot entries will not be included, and since the '
                        f'messages may have varying key schemas, use of topic compaction is not recommended.')

    p.add_argument('--new-capture-instance-snapshot-handling',
                   choices=(NEW_CAPTURE_INSTANCE_SNAPSHOT_HANDLING_BEGIN_NEW,
                            NEW_CAPTURE_INSTANCE_SNAPSHOT_HANDLING_IGNORE),
                   default=os.environ.get('NEW_CAPTURE_INSTANCE_SNAPSHOT_HANDLING',
                                          NEW_CAPTURE_INSTANCE_SNAPSHOT_HANDLING_BEGIN_NEW),
                   help=f"When the process begins consuming from a newer capture instance for a given source table, "
                        f"how is snapshot data handled? `{NEW_CAPTURE_INSTANCE_SNAPSHOT_HANDLING_BEGIN_NEW}`, the "
                        f"default, will begin a new full snapshot of the corresponding source table to pick up data "
                        f"from any new columns added in the newer instance. The behavior of "
                        f"`{NEW_CAPTURE_INSTANCE_SNAPSHOT_HANDLING_IGNORE}` depends on whether there was a snapshot "
                        f"already in progress: if so, the snapshot will continue from where it left off but will begin "
                        f"including any new columns added in the newer capture instance. If the snapshot was already "
                        f"complete, nothing further will happen.")

    p.add_argument('--new-capture-instance-overlap-handling',
                   choices=(NEW_CAPTURE_INSTANCE_OVERLAP_HANDLING_REPUBLISH,
                            NEW_CAPTURE_INSTANCE_OVERLAP_HANDLING_PICKUP),
                   default=os.environ.get('NEW_CAPTURE_INSTANCE_OVERLAP_HANDLING',
                                          NEW_CAPTURE_INSTANCE_OVERLAP_HANDLING_PICKUP),
                   help=f"When the process begins consuming from a newer capture instance for a given source table, "
                        f"how should we handle change data that appears in both instances' change tables? "
                        f"`{NEW_CAPTURE_INSTANCE_OVERLAP_HANDLING_PICKUP}`, the default, will skip over any entries "
                        f"in the newer change table that were previously published based on the older instance, "
                        f"preventing duplication of events. `{NEW_CAPTURE_INSTANCE_OVERLAP_HANDLING_REPUBLISH}` will "
                        f"publish all change entries from the beginning of the new instance's change table, maximizing "
                        f"the amount of change data published for any columns you may have added at the cost of "
                        f"duplicate messages.")

    p.add_argument('--run-validations',
                   type=str2bool, nargs='?', const=True,
                   default=str2bool(os.environ.get('RUN_VALIDATIONS', '0')),
                   help="Runs count validations between messages in the Kafka topic and rows in the change and "
                        "source tables, then quits. Respects the table inclusion/exclusion regexes.")

    p.add_argument('--message-serializer',
                   default=os.environ.get('MESSAGE_SERIALIZER',
                                          'cdc_kafka.serializers.avro.AvroSerializer'),
                   help="The serializer class (from this project's `serializers` module) used to serialize messages"
                        "sent to Kafka.")

    p.add_argument('--metrics-reporters',
                   default=os.environ.get('METRICS_REPORTERS',
                                          'cdc_kafka.metric_reporting.stdout_reporter.StdoutReporter'),
                   help="Comma-separated list of <module_name>.<class_name>s of metric reporters you want this app "
                        "to emit to.")

    p.add_argument('--metrics-namespace',
                   default=os.environ.get('METRICS_NAMESPACE', socket.getfqdn()),
                   help="Namespace used to key metrics for certain metric reporters, and which is embedded in the "
                        "metrics payload as well. Useful if multiple CDC-to-Kafka instances are emitting metrics to "
                        "the same destination. Defaults to the value returned by `socket.getfqdn()`.")

    p.add_argument('--process-hostname',
                   default=os.environ.get('PROCESS_HOSTNAME', socket.getfqdn()),
                   help="Hostname inserted into metrics metadata messages. Defaults to the value returned by "
                        "`socket.getfqdn()`.")

    p.add_argument('--partition-count',
                   type=int,
                   default=os.environ.get('PARTITION_COUNT'),
                   help="Number of partitions to specify when creating new topics. If left empty, defaults to 1 or "
                        "the average number of rows per second in the corresponding change table divided by 10, "
                        "whichever is larger.")

    p.add_argument('--replication-factor',
                   type=int,
                   default=os.environ.get('REPLICATION_FACTOR'),
                   help="Replication factor to specify when creating new topics. If left empty, defaults to 3 or the "
                        "number of brokers in the cluster, whichever is smaller.")

    p.add_argument('--truncate-fields',
                   default=os.environ.get('TRUNCATE_FIELDS', {}), type=json.loads,
                   help='Optional JSON object that maps schema.table.column names to an integer max number of '
                        'UTF-8 encoded bytes that should be serialized into the Kafka message for that field\'s '
                        'values. Only applicable to string types; will raise an exception if used for non-strings. '
                        'Truncation respects UTF-8 character boundaries and will not break in the middle of 2- or '
                        '4-byte characters. The schema, table, and column names are case-insensitive. Example: '
                        '`{"dbo.order.gift_note": 65536}`. When a field is truncated via this mechanism, a Kafka '
                        'message header of the form key: `cdc_to_kafka_truncated_field__<column_name>`, value '
                        '`<original_byte_length>,<truncated_byte_length>` will be added to the message.')

    p.add_argument('--terminate-on-capture-instance-change',
                   type=str2bool, nargs='?', const=True,
                   default=str2bool(os.environ.get('TERMINATE_ON_CAPTURE_INSTANCE_CHANGE', '0')),
                   help="When true, will cause the process to terminate if it detects a change in the set of capture "
                        "instances tracked based on the CAPTURE_INSTANCE_VERSION_* settings, BUT NOT UNTIL the "
                        "existing process has caught up to the minimum LSN available in the new capture instance(s) "
                        "for all such tables. Checked on a period defined in constants.CAPTURE_INSTANCE_EVAL_INTERVAL. "
                        "This is intended to be used with a process supervisor (e.g., the Kubernetes restart loop) "
                        "that will restart the process, to allow transparent migration to updated capture instances. "
                        "Defaults to False")

    p.add_argument('--report-progress-only',
                   type=str2bool, nargs='?', const=True,
                   default=str2bool(os.environ.get('REPORT_PROGRESS_ONLY', '0')),
                   help="Prints the table of instances being captured and their change data / snapshot data progress, "
                        "then exits without changing any state. Can be handy for validating other configuration such "
                        "as the regexes used to control which tables are followed and/or snapshotted.")

    p.add_argument('--db-row-batch-size',
                   type=int,
                   default=os.environ.get('DB_ROW_BATCH_SIZE', 2000),
                   help="Maximum number of rows to retrieve in a single change data or snapshot query. Default 2000.")

    kafka_oauth.add_kafka_oauth_arg(p)
    if arg_adder:
        arg_adder(p)
    opts, _ = p.parse_known_args()

    reporter_classes: List[reporter_base.ReporterBase] = []
    reporters: List[reporter_base.ReporterBase] = []

    if opts.metrics_reporters:
        for class_path in opts.metrics_reporters.split(','):
            package_module, class_name = class_path.rsplit('.', 1)
            module = importlib.import_module(package_module)
            reporter_class = getattr(module, class_name)
            reporter_classes.append(reporter_class)
            reporter_class.add_arguments(p)

        opts, _ = p.parse_known_args()

        for reporter_class in reporter_classes:
            reporters.append(reporter_class.construct_with_options(opts))

    package_module, class_name = opts.message_serializer.rsplit('.', 1)
    module = importlib.import_module(package_module)
    serializer_class: SerializerAbstract = getattr(module, class_name)
    serializer_class.add_arguments(p)
    opts, _ = p.parse_known_args()
    disable_writes: bool = opts.run_validations or opts.report_progress_only
    serializer = serializer_class.construct_with_options(opts, disable_writes)

    return opts, reporters, serializer
