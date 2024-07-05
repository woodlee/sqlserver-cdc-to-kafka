import argparse
import collections
import datetime
import decimal
import functools
import io
import itertools
import json
import logging
import os
import struct
import time
from typing import Tuple, TypeVar, Type, List, Any, Dict, Callable, Literal, Sequence, Optional

import confluent_kafka.avro
from avro.errors import AvroOutOfScaleException
from avro.schema import Schema
from bitarray import bitarray

from . import SerializerAbstract, DeserializedMessage
from .. import constants
from ..metric_reporting.metrics import Metrics
from ..options import str2bool
from ..parsed_row import ParsedRow
from ..progress_tracking import ProgressEntry
from ..tracked_tables import TrackedTable

logger = logging.getLogger(__name__)

AvroSerializerType = TypeVar('AvroSerializerType', bound='AvroSerializer')

COMPARE_CANONICAL_EVERY_NTH = 50_000
AVRO_SCHEMA_NAMESPACE = "cdc_to_kafka"

PROGRESS_TRACKING_SCHEMA_VERSION = '2'
PROGRESS_TRACKING_AVRO_KEY_SCHEMA = confluent_kafka.avro.loads(json.dumps({
    "name": f"{AVRO_SCHEMA_NAMESPACE}__progress_tracking_v{PROGRESS_TRACKING_SCHEMA_VERSION}__key",
    "namespace": AVRO_SCHEMA_NAMESPACE,
    "type": "record",
    "fields": [
        {
            "name": "topic_name",
            "type": "string"
        },
        {
            "name": "progress_kind",
            "type": {
                "type": "enum",
                "name": "progress_kind",
                "symbols": [
                    constants.CHANGE_ROWS_KIND,
                    constants.SNAPSHOT_ROWS_KIND
                ]
            }
        }
    ]
}))
PROGRESS_TRACKING_AVRO_VALUE_SCHEMA = confluent_kafka.avro.loads(json.dumps({
    "name": f"{AVRO_SCHEMA_NAMESPACE}__progress_tracking_v{PROGRESS_TRACKING_SCHEMA_VERSION}__value",
    "namespace": AVRO_SCHEMA_NAMESPACE,
    "type": "record",
    "fields": [
        {
            "name": "source_table_name",
            "type": "string"
        },
        {
            "name": "change_table_name",
            "type": "string"
        },
        # ------------------------------------------------------------------------------------------------
        # These next two are defunct/deprecated as of v4 but remain here to ease the upgrade transition
        # for anyone with existing progress recorded by earlier versions:
        {
            "name": "last_ack_partition",
            "type": ["null", "int"]
        },
        {
            "name": "last_ack_offset",
            "type": ["null", "long"]
        },
        # ------------------------------------------------------------------------------------------------
        {
            "name": "last_ack_position",
            "type": [
                {
                    "type": "record",
                    "name": f"{constants.CHANGE_ROWS_KIND}_progress",
                    "namespace": AVRO_SCHEMA_NAMESPACE,
                    "fields": [
                        {
                            "name": constants.LSN_NAME,
                            "type": "string",
                        },
                        {
                            "name": constants.SEQVAL_NAME,
                            "type": "string",
                        },
                        {
                            "name": constants.OPERATION_NAME,
                            "type": {
                                "type": "enum",
                                "name": constants.OPERATION_NAME,
                                "symbols": list(constants.CDC_OPERATION_NAME_TO_ID.keys())
                            }
                        }
                    ]
                },
                {
                    "type": "record",
                    "name": f"{constants.SNAPSHOT_ROWS_KIND}_progress",
                    "namespace": AVRO_SCHEMA_NAMESPACE,
                    "fields": [
                        {
                            "name": "key_fields",
                            "type": {
                                "type": "map",
                                "values": ["string", "long"]
                            }
                        }
                    ]
                }
            ]
        }
    ]
}))

SNAPSHOT_LOGGING_SCHEMA_VERSION = '1'
SNAPSHOT_LOGGING_AVRO_VALUE_SCHEMA = confluent_kafka.avro.loads(json.dumps({
    "name": f"{AVRO_SCHEMA_NAMESPACE}__snapshot_logging_v{SNAPSHOT_LOGGING_SCHEMA_VERSION}__value",
    "namespace": AVRO_SCHEMA_NAMESPACE,
    "type": "record",
    "fields": [
        {
            "name": "topic_name",
            "type": "string"
        },
        {
            "name": "table_name",
            "type": "string"
        },
        {
            "name": "action",
            "type": "string"
        },
        {
            "name": "process_hostname",
            "type": "string"
        },
        {
            "name": "event_time_utc",
            "type": "string"
        },
        {
            "name": "key_schema_id",
            "type": ["null", "long"]
        },
        {
            "name": "value_schema_id",
            "type": ["null", "long"]
        },
        {
            "name": "partition_watermarks_low",
            "type": ["null", {
                "type": "map",
                "values": "long"
            }]
        },
        {
            "name": "partition_watermarks_high",
            "type": ["null", {
                "type": "map",
                "values": "long"
            }]
        },
        {
            "name": "starting_snapshot_index",
            "type": ["null", {
                "type": "map",
                "values": ["string", "long"]
            }]
        },
        {
            "name": "ending_snapshot_index",
            "type": ["null", {
                "type": "map",
                "values": ["string", "long"]
            }]
        }
    ]
}))

METRICS_SCHEMA_VERSION = '2'

METRICS_AVRO_KEY_SCHEMA = confluent_kafka.avro.loads(json.dumps({
    "name": f"{AVRO_SCHEMA_NAMESPACE}__metrics_v{METRICS_SCHEMA_VERSION}__key",
    "namespace": AVRO_SCHEMA_NAMESPACE,
    "type": "record",
    "fields": [
        {
            "name": "metrics_namespace",
            "type": "string"
        }
    ]
}))

METRICS_AVRO_VALUE_SCHEMA = confluent_kafka.avro.loads(json.dumps({
    "name": f"{AVRO_SCHEMA_NAMESPACE}__metrics_v{METRICS_SCHEMA_VERSION}__value",
    "namespace": AVRO_SCHEMA_NAMESPACE,
    "type": "record",
    "fields": [
        {
            "name": k,
            "type": v
        } for (k, v) in Metrics.FIELDS_AND_TYPES
    ]
}))


class AvroTableDataSerializerMetadata(object):
    __slots__ = ('key_schema_id', 'value_schema_id', 'key_field_ordinals', 'ordered_serializers',
                 'key_field_names', 'value_field_names', 'all_cols_updated_enum_bytes')

    @staticmethod
    def get_all_cols_updated_enum_bytes(col_count: int) -> bytes:
        buf = io.BytesIO()
        int_to_int(buf, col_count)
        for p in range(1, col_count + 1):
            int_to_int(buf, p)
        buf.write(b'\x00')
        return buf.getvalue()

    def __init__(self, key_schema_id: int, value_schema_id: int, key_field_ordinals: Sequence[int],
                 ordered_serializers: List[Callable[[io.BytesIO, Any], None]],
                 key_field_names: Sequence[str], value_field_names: Sequence[str]) -> None:
        self.key_schema_id: int = key_schema_id
        self.value_schema_id: int = value_schema_id
        self.key_field_ordinals: Tuple[int, ...] = tuple(key_field_ordinals)
        self.ordered_serializers: List[Callable[[io.BytesIO, Any], None]] = ordered_serializers
        self.key_field_names: Tuple[str, ...] = tuple(key_field_names)
        self.value_field_names: Tuple[str, ...] = tuple(value_field_names)
        self.all_cols_updated_enum_bytes: bytes = AvroTableDataSerializerMetadata.get_all_cols_updated_enum_bytes(
            len(ordered_serializers))


class AvroSchemaGenerator(object):
    _instance = None

    def __init__(self, always_use_avro_longs: bool,
                 avro_type_spec_overrides: Dict[str, str | Dict[str, str | int]]) -> None:
        if AvroSchemaGenerator._instance is not None:
            raise Exception('AvroSchemaGenerator class should be used as a singleton.')

        self.always_use_avro_longs: bool = always_use_avro_longs
        self.normalized_avro_type_overrides: Dict[Tuple[str, str, str], str | Dict[str, str | int]] = {}
        for k, v in avro_type_spec_overrides.items():
            if k.count('.') != 2:
                raise Exception(f'Avro type spec override "{k}" was incorrectly specified. Please key this config in '
                                'the form <schema name>.<table name>.<column name>')
            sn, tn, cn = k.split('.')
            self.normalized_avro_type_overrides[(sn.lower(), tn.lower(), cn.lower())] = v

        AvroSchemaGenerator._instance = self

    def generate_key_schema(self, table: TrackedTable) -> Schema:
        key_schema_fields = [self.get_record_field_schema(
            table.schema_name, table.table_name, kf.name, kf.sql_type_name, kf.decimal_precision,
            kf.decimal_scale, False
        ) for kf in table.key_fields]
        schema_json = {
            "name": f"{table.schema_name}_{table.table_name}_cdc__key",
            "namespace": AVRO_SCHEMA_NAMESPACE,
            "type": "record",
            "fields": key_schema_fields
        }
        return confluent_kafka.avro.loads(json.dumps(schema_json))

    def generate_value_schema(self, table: TrackedTable) -> Schema:
        # In CDC tables, all columns are nullable so that if the column is dropped from the source table, the capture
        # instance need not be updated. We align with that by making the Avro value schema for all captured fields
        # nullable (which also helps with maintaining future Avro schema compatibility).
        value_schema_fields = [self.get_record_field_schema(
            table.schema_name, table.table_name, vf.name, vf.sql_type_name, vf.decimal_precision,
            vf.decimal_scale, True
        ) for vf in table.value_fields]
        value_field_names = [f.name for f in table.value_fields]
        value_fields_plus_metadata_fields = AvroSchemaGenerator.get_cdc_metadata_fields_avro_schemas(
            table.schema_name, table.table_name, value_field_names) + value_schema_fields
        schema_json = {
            "name": f"{table.schema_name}_{table.table_name}_cdc__value",
            "namespace": AVRO_SCHEMA_NAMESPACE,
            "type": "record",
            "fields": value_fields_plus_metadata_fields
        }
        return confluent_kafka.avro.loads(json.dumps(schema_json))

    def get_record_field_schema(self, db_schema_name: str, db_table_name: str, field_name: str, sql_type_name: str,
                                decimal_precision: int, decimal_scale: int, make_nullable: bool) -> Dict[str, Any]:
        override_type = self.normalized_avro_type_overrides.get(
            (db_schema_name.lower(), db_table_name.lower(), field_name.lower()))
        if override_type:
            avro_type = override_type
        else:
            if sql_type_name in ('decimal', 'numeric', 'money', 'smallmoney'):
                if (not decimal_precision) or decimal_scale is None:
                    raise Exception(f"Field '{field_name}': For SQL decimal, money, or numeric types, the scale and "
                                    f"precision must be provided.")
                avro_type = {
                    "type": "bytes",
                    "logicalType": "decimal",
                    "precision": decimal_precision,
                    "scale": decimal_scale
                }
            elif sql_type_name == 'bigint':
                avro_type = "long"
            elif sql_type_name == 'bit':
                avro_type = "boolean"
            elif sql_type_name == 'float':
                avro_type = "double"
            elif sql_type_name == 'real':
                avro_type = "float"
            elif sql_type_name in ('int', 'smallint', 'tinyint'):
                avro_type = "long" if self.always_use_avro_longs else "int"
            # For date and time we don't respect always_use_avro_longs since the underlying type being `int` for these
            # logical types is spelled out in the Avro spec:
            elif sql_type_name == 'date':
                avro_type = {"type": "int", "logicalType": "date"}
            elif sql_type_name == 'time':
                avro_type = {"type": "int", "logicalType": "time-millis"}
            elif sql_type_name in ('datetime', 'datetime2', 'datetimeoffset', 'smalldatetime',
                                   'xml') + constants.SQL_STRING_TYPES:
                avro_type = "string"
            elif sql_type_name == 'uniqueidentifier':
                avro_type = {"type": "string", "logicalType": "uuid"}
            elif sql_type_name in ('binary', 'image', 'varbinary', 'rowversion'):
                avro_type = "bytes"
            else:
                raise Exception(f"Field '{field_name}': I am unsure how to convert SQL type {sql_type_name} to Avro")

        if make_nullable:
            return {
                "name": field_name,
                "type": [
                    "null",
                    avro_type
                ],
                "default": None
            }
        else:
            return {
                "name": field_name,
                "type": avro_type
            }

    @staticmethod
    # These fields are common to all change/snapshot data messages published to Kafka by this process
    def get_cdc_metadata_fields_avro_schemas(db_schema_name: str, db_table_name: str,
                                             source_field_names: List[str]) -> List[Dict[str, Any]]:
        return [
            {
                "name": constants.OPERATION_NAME,
                "type": {
                    "type": "enum",
                    "name": f'{db_schema_name}_{db_table_name}{constants.OPERATION_NAME}',
                    "symbols": list(constants.CDC_OPERATION_NAME_TO_ID.keys())
                }
            },
            {
                # as ISO 8601 timestamp... either the change's tran_end_time OR the time the snapshot row was read:
                "name": constants.EVENT_TIME_NAME,
                "type": "string"
            },
            {
                "name": constants.LSN_NAME,
                "type": ["null", "string"]
            },
            {
                "name": constants.SEQVAL_NAME,
                "type": ["null", "string"]
            },
            {
                # Messages will list the names of all fields that were updated in the event (for snapshots or CDC insert
                # records this will be all rows):
                "name": constants.UPDATED_FIELDS_NAME,
                "type": {
                    "type": "array",
                    "items": {
                        "type": "enum",
                        "name": f'{db_schema_name}_{db_table_name}{constants.UPDATED_FIELDS_NAME}',
                        "default": constants.UNRECOGNIZED_COLUMN_DEFAULT_NAME,
                        "symbols": [constants.UNRECOGNIZED_COLUMN_DEFAULT_NAME] + source_field_names
                    }
                }
            }
        ]


class AvroSerializer(SerializerAbstract):
    def __init__(self, schema_registry_url: str, always_use_avro_longs: bool, progress_topic_name: str,
                 snapshot_logging_topic_name: str, metrics_topic_name: str,
                 avro_type_spec_overrides: Dict[str, str | Dict[str, str | int]], disable_writes: bool) -> None:
        self.always_use_avro_longs: bool = always_use_avro_longs
        self.avro_type_spec_overrides: Dict[str, str | Dict[str, str | int]] = avro_type_spec_overrides
        self.disable_writes: bool = disable_writes
        self._schema_registry: confluent_kafka.avro.CachedSchemaRegistryClient = \
            confluent_kafka.avro.CachedSchemaRegistryClient(schema_registry_url)  # type: ignore[call-arg]
        self._confluent_serializer: confluent_kafka.avro.MessageSerializer = \
            confluent_kafka.avro.MessageSerializer(self._schema_registry)
        self._tables: Dict[str, AvroTableDataSerializerMetadata] = {}
        self._schema_generator: AvroSchemaGenerator = AvroSchemaGenerator(
            always_use_avro_longs, avro_type_spec_overrides)
        self._canonical_compare_ctr: Dict[str, int] = collections.defaultdict(int)
        if progress_topic_name:
            self._progress_key_schema_id: int = self._get_or_register_schema(
                f'{progress_topic_name}-key', PROGRESS_TRACKING_AVRO_KEY_SCHEMA,
                constants.DEFAULT_KEY_SCHEMA_COMPATIBILITY_LEVEL)
            self._progress_value_schema_id: int = self._get_or_register_schema(
                f'{progress_topic_name}-value', PROGRESS_TRACKING_AVRO_VALUE_SCHEMA,
                constants.DEFAULT_VALUE_SCHEMA_COMPATIBILITY_LEVEL)
        if snapshot_logging_topic_name:
            self._snapshot_logging_schema_id: int = self._get_or_register_schema(
                f'{snapshot_logging_topic_name}-value', SNAPSHOT_LOGGING_AVRO_VALUE_SCHEMA,
                constants.DEFAULT_VALUE_SCHEMA_COMPATIBILITY_LEVEL)
        if metrics_topic_name:
            self._metrics_key_schema_id: int = self._get_or_register_schema(
                f'{metrics_topic_name}-key', METRICS_AVRO_KEY_SCHEMA,
                constants.DEFAULT_KEY_SCHEMA_COMPATIBILITY_LEVEL)
            self._metrics_value_schema_id: int = self._get_or_register_schema(
                f'{metrics_topic_name}-value', METRICS_AVRO_VALUE_SCHEMA,
                constants.DEFAULT_VALUE_SCHEMA_COMPATIBILITY_LEVEL)

    def _get_or_register_schema(self, subject_name: str, schema: Schema,
                                compatibility_level: Literal["NONE", "FULL", "FORWARD", "BACKWARD"]) -> int:
        # TODO: it turns out that if you try to re-register a schema that was previously registered but later superseded
        # (e.g. in the case of adding and then later deleting a column), the schema registry will accept that and return
        # you the previously-registered schema ID without updating the `latest` version associated with the registry
        # subject, or verifying that the change is Avro-compatible. It seems like the way to handle this, per
        # https://github.com/confluentinc/schema-registry/issues/1685, would be to detect the condition and delete the
        # subject-version-number of that schema before re-registering it. Since subject-version deletion is not
        # available in the `CachedSchemaRegistryClient` we use here--and since this is a rare case--I'm explicitly
        # choosing to punt on it for the moment. The Confluent lib does now have a newer `SchemaRegistryClient` class
        # which supports subject-version deletion, but changing this code to use it appears to be a non-trivial task.

        current_schema: Schema
        schema_id_str, current_schema, _ = self._schema_registry.get_latest_schema(subject_name)
        schema_id: int
        if (current_schema is None or current_schema != schema) and not self.disable_writes:
            logger.info('Schema for subject %s does not exist or is outdated; registering now.', subject_name)
            schema_id = self._schema_registry.register(subject_name, schema)
            logger.debug('Schema registered for subject %s: %s', subject_name, schema)
            time.sleep(constants.KAFKA_CONFIG_RELOAD_DELAY_SECS)
            if current_schema is None:
                self._schema_registry.update_compatibility(compatibility_level, subject_name)
        else:
            schema_id = int(schema_id_str)
        return schema_id

    def register_table(self, table: TrackedTable) -> None:
        key_schema = self._schema_generator.generate_key_schema(table)
        value_schema = self._schema_generator.generate_value_schema(table)
        key_schema_id: int = self._get_or_register_schema(f'{table.topic_name}-key', key_schema,
                                                          constants.DEFAULT_KEY_SCHEMA_COMPATIBILITY_LEVEL)
        value_schema_id: int = self._get_or_register_schema(f'{table.topic_name}-value', value_schema,
                                                            constants.DEFAULT_VALUE_SCHEMA_COMPATIBILITY_LEVEL)
        ordered_serializers: List[Callable[[io.BytesIO, Any], None]] = []

        serializer: Callable[[io.BytesIO, Any], None]
        for vf in table.value_fields:
            sql_type_name: str = vf.sql_type_name.lower()
            if sql_type_name in ('decimal', 'numeric', 'money', 'smallmoney'):
                if (not vf.decimal_precision) or vf.decimal_scale is None:
                    raise Exception(f"Field '{vf.name}': For SQL decimal, money, or numeric types, the scale and "
                                    f"precision must be provided.")
                serializer = functools.partial(decimal_to_decimal, scale=vf.decimal_scale)
            elif sql_type_name == 'bit':
                serializer = bool_to_bool
            elif sql_type_name == 'float':
                serializer = float_to_double
            elif sql_type_name == 'real':
                serializer = float_to_float
            elif sql_type_name in ('int', 'smallint', 'tinyint', 'bigint'):
                serializer = int_to_int
            elif sql_type_name == 'date':
                serializer = date_to_int
            elif sql_type_name == 'time':
                serializer = time_to_int
            elif sql_type_name in ('datetime', 'datetime2', 'datetimeoffset', 'smalldatetime'):
                serializer = datetime_to_string
            elif sql_type_name in ('xml', 'uniqueidentifier') + constants.SQL_STRING_TYPES:
                serializer = string_to_string
            elif sql_type_name in ('binary', 'image', 'varbinary', 'rowversion'):
                serializer = bytes_to_bytes
            else:
                raise Exception(f"Field '{vf.name}': I am unsure how to convert SQL type {sql_type_name} to Avro")

            ordered_serializers.append(serializer)

        self._tables[table.topic_name] = AvroTableDataSerializerMetadata(
            key_schema_id, value_schema_id, table.key_field_source_table_ordinals, ordered_serializers,
            table.key_field_names, table.value_field_names)
    
    def serialize_table_data_message(self, row: ParsedRow) -> Tuple[bytes, bytes]:
        metadata = self._tables[row.destination_topic]
        key_writer = io.BytesIO()
        key_writer.write(struct.pack('>bI', 0, metadata.key_schema_id))
        value_writer = io.BytesIO()
        value_writer.write(struct.pack('>bI', 0, metadata.value_schema_id))
        int_to_int(value_writer, row.operation_id)
        as_bytes: bytes = row.event_db_time.isoformat().encode("utf-8")
        int_to_int(value_writer, len(as_bytes))
        value_writer.write(struct.pack(f"{len(as_bytes)}s", as_bytes))
        if row.change_idx is None or row.operation_id == constants.SNAPSHOT_OPERATION_ID:
            value_writer.write(b'\x00\x00')
        else:
            value_writer.write(b'\x02')
            as_bytes = f',0x{row.change_idx.lsn.hex()}'.encode("utf-8")
            value_writer.write(struct.pack("23s", as_bytes))
            value_writer.write(b'\x02')
            as_bytes = f',0x{row.change_idx.seqval.hex()}'.encode("utf-8")
            value_writer.write(struct.pack("23s", as_bytes))
        if row.operation_id in (constants.SNAPSHOT_OPERATION_ID, constants.INSERT_OPERATION_ID,
                                constants.DELETE_OPERATION_ID):
            value_writer.write(metadata.all_cols_updated_enum_bytes)
        else:
            bits = bitarray()
            bits.frombytes(row.cdc_update_mask)
            bits.reverse()
            int_to_int(value_writer, bits.count())
            for i, bit in enumerate(bits):
                if bit:
                    int_to_int(value_writer, i + 1)
            value_writer.write(b'\x00')
        for ix, f in enumerate(metadata.ordered_serializers):
            if row.table_data_cols[ix] is None:
                value_writer.write(b'\x00')
            else:
                value_writer.write(b'\x02')
                f(value_writer, row.table_data_cols[ix])
        for ix in metadata.key_field_ordinals:
            f = metadata.ordered_serializers[ix - 1]
            datum = row.table_data_cols[ix - 1]
            f(key_writer, datum)
        serialized_key: bytes = key_writer.getvalue()
        key_writer.close()
        serialized_value: bytes = value_writer.getvalue()
        value_writer.close()

        if self._canonical_compare_ctr.get(row.destination_topic, 0) % COMPARE_CANONICAL_EVERY_NTH == 0:
            self.compare_canonical(metadata, row, serialized_key, serialized_value)

        self._canonical_compare_ctr[row.destination_topic] += 1

        return serialized_key, serialized_value

    # Not normally used, but here to help debug custom Avro serialization by comparing against that used
    # by the Confluent library:
    def compare_canonical(self, metadata: AvroTableDataSerializerMetadata, row: ParsedRow,
                          serialized_key: bytes, serialized_value: bytes) -> None:

        key_dict = dict(zip(metadata.key_field_names, row.ordered_key_field_values))
        value_dict = dict(zip(metadata.value_field_names, row.table_data_cols))

        if row.operation_id == constants.SNAPSHOT_OPERATION_ID:
            value_dict[constants.OPERATION_NAME] = constants.SNAPSHOT_OPERATION_NAME
            value_dict[constants.LSN_NAME] = None
            value_dict[constants.SEQVAL_NAME] = None
        else:
            change_idx = row.change_idx
            value_dict.update(change_idx.as_dict())

        if row.operation_id in (constants.PRE_UPDATE_OPERATION_ID, constants.POST_UPDATE_OPERATION_ID):
            arr = bitarray()
            arr.frombytes(row.cdc_update_mask)
            arr.reverse()
            value_dict[constants.UPDATED_FIELDS_NAME] = list(itertools.compress(metadata.value_field_names, arr))
        else:
            value_dict[constants.UPDATED_FIELDS_NAME] = list(metadata.value_field_names)

        value_dict[constants.EVENT_TIME_NAME] = row.event_db_time.isoformat()
        dates_transformed = {k: v.isoformat() for k, v in value_dict.items() if type(v) is datetime.datetime}
        value_dict.update(dates_transformed)

        comp_key: bytes = self._confluent_serializer.encode_record_with_schema_id(  # type: ignore[assignment]
            metadata.key_schema_id, key_dict, True)
        if serialized_key != comp_key:
            # import pdb; pdb.set_trace()
            raise Exception(
                f'Avro serialization does not match the canonical library serialization. Key {key_dict} for message '
                f'to topic {row.destination_topic} with schema ID {metadata.key_schema_id} serialized as '
                f'"{serialized_key!r}" but canonical form was "{comp_key!r}".')

        comp_value: bytes = self._confluent_serializer.encode_record_with_schema_id(  # type: ignore[assignment]
            metadata.value_schema_id, value_dict, False)
        if serialized_value != comp_value:
            # import pdb; pdb.set_trace()
            raise Exception(
                f'Avro serialization does not match the canonical library serialization. Value {value_dict} for '
                f'message to topic {row.destination_topic} with schema ID {metadata.value_schema_id} serialized '
                f'as "{serialized_value!r}" but canonical form was "{comp_value!r}".')

    def serialize_progress_tracking_message(self, progress_entry: ProgressEntry) -> Tuple[bytes, Optional[bytes]]:
        k: bytes = self._confluent_serializer.encode_record_with_schema_id(  # type: ignore[assignment]
            self._progress_key_schema_id, progress_entry.key, True)
        if progress_entry.value is None:
            return k, None
        v: bytes = self._confluent_serializer.encode_record_with_schema_id(  # type: ignore[assignment]
            self._progress_value_schema_id, progress_entry.value, False)
        return k, v

    def serialize_metrics_message(self, metrics_namespace: str, metrics: Dict[str, Any]) -> Tuple[bytes, bytes]:
        k: bytes = self._confluent_serializer.encode_record_with_schema_id(  # type: ignore[assignment]
            self._metrics_key_schema_id, {'metrics_namespace': metrics_namespace}, True)
        v: bytes = self._confluent_serializer.encode_record_with_schema_id(  # type: ignore[assignment]
            self._metrics_value_schema_id, metrics, False)
        return k, v

    def serialize_snapshot_logging_message(self, snapshot_log: Dict[str, Any]) -> Tuple[None, bytes]:
        v: bytes = self._confluent_serializer.encode_record_with_schema_id(  # type: ignore[assignment]
            self._snapshot_logging_schema_id, snapshot_log, False)
        return None, v

    def deserialize(self, msg: confluent_kafka.Message) -> DeserializedMessage:
        # noinspection PyArgumentList
        return DeserializedMessage(msg,
                                   self._confluent_serializer.decode_message(msg.key(), is_key=True),
                                   self._confluent_serializer.decode_message(msg.value(), is_key=False))

    @staticmethod
    def add_arguments(parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            '--schema-registry-url', default=os.environ.get('SCHEMA_REGISTRY_URL'),
            help='URL to your Confluent Schema Registry, e.g. "http://localhost:8081"')
        parser.add_argument(
            '--always-use-avro-longs', type=str2bool, nargs='?', const=True,
            default=str2bool(os.environ.get('ALWAYS_USE_AVRO_LONGS', '0')),
            help="Defaults to False. If set to True, Avro schemas produced/registered by this process will "
                 "use the Avro `long` type instead of the `int` type for fields corresponding to SQL Server "
                 "INT, SMALLINT, or TINYINT columns. This can be used to future-proof in cases where the column "
                 "size may need to be upgraded in the future, at the potential cost of increased storage or "
                 "memory space needs in consuming processes. Note that if this change is made for existing "
                 "topics, the schema registration attempt will violate Avro FORWARD compatibility checks (the "
                 "default used by this process), meaning that you may need to manually override the schema "
                 "registry compatibility level for any such topics first.")
        parser.add_argument(
            '--avro-type-spec-overrides',
            default=os.environ.get('AVRO_TYPE_SPEC_OVERRIDES', {}), type=json.loads,
            help='Optional JSON object that maps schema.table.column names to a string or object indicating the '
                 'Avro schema type specification you want to use for the field. This will override the default '
                 'mapping of SQL types to Avro types otherwise used and found in avro.py. Note that setting '
                 'this only changes the generated schema and will NOT affect the way values are passed to the '
                 'Avro serialization library, so any overriding type specified should be compatible with the '
                 'SQL/Python types of the actual data. Example: `{"dbo.order.orderid": "long"}` could be used '
                 'to specify the use of an Avro `long` type for a source DB column that is only a 32-bit INT, '
                 'perhaps in preparation for a future DB column change.')

    @classmethod
    def construct_with_options(cls: Type[AvroSerializerType], opts: argparse.Namespace,
                               disable_writes: bool) -> AvroSerializerType:
        if not opts.schema_registry_url:
            raise Exception('AvroSerializer cannot be used without specifying a value for SCHEMA_REGISTRY_URL')
        metrics_topic_name: str = hasattr(opts, 'kafka_metrics_topic') and opts.kafka_metrics_topic or ''
        return cls(opts.schema_registry_url, opts.always_use_avro_longs, opts.progress_topic_name,
                   opts.snapshot_logging_topic_name, metrics_topic_name, opts.avro_type_spec_overrides,
                   disable_writes)


def decimal_to_decimal(writer: io.BytesIO, datum: decimal.Decimal, scale: int) -> None:
    sign, digits, exp = datum.as_tuple()
    if (-1 * int(exp)) > scale:
        raise AvroOutOfScaleException(scale, datum, exp)  # type: ignore[no-untyped-call]

    unscaled_datum = 0
    for digit in digits:
        unscaled_datum = (unscaled_datum * 10) + digit

    bits_req = unscaled_datum.bit_length() + 1
    if sign:
        unscaled_datum = (1 << bits_req) - unscaled_datum

    bytes_req = bits_req // 8
    padding_bits = ~((1 << bits_req) - 1) if sign else 0
    packed_bits = padding_bits | unscaled_datum

    bytes_req += 1 if (bytes_req << 3) < bits_req else 0
    int_to_int(writer, bytes_req)
    for index in range(bytes_req - 1, -1, -1):
        bits_to_write = packed_bits >> (8 * index)
        writer.write(bytearray([bits_to_write & 0xFF]))


def int_to_int(writer: io.BytesIO, datum: int) -> None:
    datum = (datum << 1) ^ (datum >> 63)
    while datum & ~0x7F:
        writer.write(bytearray([(datum & 0x7F) | 0x80]))
        datum >>= 7
    writer.write(bytearray([datum]))


def bool_to_bool(writer: io.BytesIO, datum: bool) -> None:
    writer.write(bytearray([bool(datum)]))


def float_to_double(writer: io.BytesIO, datum: float) -> None:
    writer.write(struct.Struct("<d").pack(datum))


def float_to_float(writer: io.BytesIO, datum: float) -> None:
    writer.write(struct.Struct("<f").pack(datum))


def date_to_int(writer: io.BytesIO, datum: datetime.date) -> None:
    delta_date = datum - datetime.date(1970, 1, 1)
    int_to_int(writer, delta_date.days)


def time_to_int(writer: io.BytesIO, datum: datetime.time) -> None:
    milliseconds = datum.hour * 3600000 + datum.minute * 60000 + datum.second * 1000 + datum.microsecond // 1000
    int_to_int(writer, milliseconds)


def datetime_to_string(writer: io.BytesIO, datum: datetime.datetime) -> None:
    as_bytes: bytes = datum.isoformat().encode("utf-8")
    int_to_int(writer, len(as_bytes))
    writer.write(struct.pack(f"{len(as_bytes)}s", as_bytes))


def bytes_to_bytes(writer: io.BytesIO, datum: bytes) -> None:
    int_to_int(writer, len(datum))
    writer.write(struct.pack(f"{len(datum)}s", datum))


def string_to_string(writer: io.BytesIO, datum: str) -> None:
    as_bytes: bytes = datum.encode("utf-8")
    int_to_int(writer, len(as_bytes))
    writer.write(struct.pack(f"{len(as_bytes)}s", as_bytes))
