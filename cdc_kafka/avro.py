import json
from typing import Dict, Union, Sequence, List, Any, Tuple, Optional, Callable

from avro.schema import Schema
import confluent_kafka.avro

from . import constants

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .tracked_tables import TrackedField


class AvroSchemaGenerator(object):
    def __init__(self, always_use_avro_longs: bool, avro_type_spec_overrides: Dict[str, Union[str, Dict]]) -> None:
        self.always_use_avro_longs: bool = always_use_avro_longs
        self.normalized_avro_type_overrides: Dict[Tuple[str, str, str], Union[str, Dict]] = {}
        for k, v in avro_type_spec_overrides.items():
            if k.count('.') != 2:
                raise Exception(f'Avro type spec override "{k}" was incorrectly specified. Please key this config in '
                                'the form <schema name>.<table name>.<column name>')
            schema, table, column = k.split('.')
            self.normalized_avro_type_overrides[(schema.lower(), table.lower(), column.lower())] = v

    def generate_key_schema(self, db_schema_name: str, db_table_name: str,
                            key_fields: Sequence['TrackedField']) -> Schema:
        key_schema_fields = [self.get_record_field_schema(
            db_schema_name, db_table_name, kf.name, kf.sql_type_name, kf.decimal_precision, kf.decimal_scale, False
        ) for kf in key_fields]
        schema_json = {
            "name": f"{db_schema_name}_{db_table_name}_cdc__key",
            "namespace": constants.AVRO_SCHEMA_NAMESPACE,
            "type": "record",
            "fields": key_schema_fields
        }
        return confluent_kafka.avro.loads(json.dumps(schema_json))

    def generate_value_schema(self, db_schema_name: str, db_table_name: str,
                              value_fields: Sequence['TrackedField']) -> Schema:
        # In CDC tables, all columns are nullable so that if the column is dropped from the source table, the capture
        # instance need not be updated. We align with that by making the Avro value schema for all captured fields
        # nullable (which also helps with maintaining future Avro schema compatibility).
        value_schema_fields = [self.get_record_field_schema(
            db_schema_name, db_table_name, vf.name, vf.sql_type_name, vf.decimal_precision, vf.decimal_scale, True
        ) for vf in value_fields]
        value_field_names = [f.name for f in value_fields]
        value_fields_plus_metadata_fields = AvroSchemaGenerator.get_cdc_metadata_fields_avro_schemas(
            db_schema_name, db_table_name, value_field_names) + value_schema_fields
        schema_json = {
            "name": f"{db_schema_name}_{db_table_name}_cdc__value",
            "namespace": constants.AVRO_SCHEMA_NAMESPACE,
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


def avro_transform_fn_from_sql_type(sql_type_name: str) -> Optional[Callable[[Any], Any]]:
    if sql_type_name in ('datetime', 'datetime2', 'datetimeoffset', 'smalldatetime'):
        # We have chosen to represent datetime values as ISO8601 strings rather than using the usual Avro convention of
        # an int type + 'timestamp-millis' logical type that captures them as ms since the Unix epoch. This is because
        # the latter presumes the time is in UTC, whereas we do not always know the TZ of datetimes we pull from the
        # DB. It seems more 'faithful' to represent them exactly as they exist in the DB.
        return lambda x: x and x.isoformat()
    return None
