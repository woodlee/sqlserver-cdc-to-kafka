from typing import List, Dict, Callable, Any, Optional

from . import constants

SQL_STRING_TYPES = ('char', 'nchar', 'varchar', 'ntext', 'nvarchar', 'text')


# These fields are common to all change/snapshot data messages published to Kafka by this process
def get_cdc_metadata_fields_avro_schemas(table_fq_name: str, source_field_names: List[str]) -> List[Dict[str, Any]]:
    base_name = table_fq_name.replace('.', '_')
    return [
        {
            "name": constants.OPERATION_NAME,
            "type": {
                "type": "enum",
                "name": f'{base_name}{constants.OPERATION_NAME}',
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
                    "name": f'{base_name}{constants.UPDATED_FIELDS_NAME}',
                    "default": constants.UNRECOGNIZED_COLUMN_DEFAULT_NAME,
                    "symbols": [constants.UNRECOGNIZED_COLUMN_DEFAULT_NAME] + source_field_names
                }
            }
        }
    ]


# In CDC tables, all columns are nullable so that if the column is dropped from the source table, the capture instance
# need not be updated. We align with that by making the Avro value schema for all captured fields nullable (which also
# helps with maintaining future Avro schema compatibility).
def avro_schema_from_sql_type(source_field_name: str, sql_type_name: str, decimal_precision: int,
                              decimal_scale: int, make_nullable: bool, force_avro_long: bool) -> Dict[str, Any]:
    if sql_type_name in ('decimal', 'numeric', 'money', 'smallmoney'):
        if (not decimal_precision) or decimal_scale is None:
            raise Exception(f"Field '{source_field_name}': For SQL decimal, money, or numeric types, the scale and "
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
        avro_type = "long" if force_avro_long else "int"
    # For date and time we don't respect force_avro_long since the underlying type being `int` for these logical
    # types is spelled out in the Avro spec:
    elif sql_type_name == 'date':
        avro_type = {"type": "int", "logicalType": "date"}
    elif sql_type_name == 'time':
        avro_type = {"type": "int", "logicalType": "time-millis"}
    elif sql_type_name in ('datetime', 'datetime2', 'datetimeoffset', 'smalldatetime', 'xml') + SQL_STRING_TYPES:
        avro_type = "string"
    elif sql_type_name == 'uniqueidentifier':
        avro_type = {"type": "string", "logicalType": "uuid"}
    elif sql_type_name in ('binary', 'image', 'varbinary', 'rowversion'):
        avro_type = "bytes"
    else:
        raise Exception(f"Field '{source_field_name}': I am unsure how to convert SQL type {sql_type_name} to Avro")

    if make_nullable:
        return {
            "name": source_field_name,
            "type": [
                "null",
                avro_type
            ],
            "default": None
        }
    else:
        return {
            "name": source_field_name,
            "type": avro_type
        }


def avro_transform_fn_from_sql_type(sql_type_name: str) -> Optional[Callable[[Any], Any]]:
    if sql_type_name in ('datetime', 'datetime2', 'datetimeoffset', 'smalldatetime'):
        # We have chosen to represent datetime values as ISO8601 strings rather than using the usual Avro convention of
        # an int type + 'timestamp-millis' logical type that captures them as ms since the Unix epoch. This is because
        # the latter presumes the time is in UTC, whereas we do not always know the TZ of datetimes we pull from the
        # DB. It seems more 'faithful' to represent them exactly as they exist in the DB.
        return lambda x: x and x.isoformat()
    return None
