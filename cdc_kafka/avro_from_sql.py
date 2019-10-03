from typing import List, Dict, Callable

from . import constants


# These fields are common to all messages published to Kafka by this process
def get_cdc_metadata_fields_avro_schemas(source_field_names: List[str]) -> List[Dict[str, object]]:
    return [
        {
            "name": "_cdc_start_lsn",
            "type": "bytes"
        },
        {
            "name": "_cdc_seqval",
            "type": "bytes"
        },
        {
            "name": "_cdc_operation",
            "type": {"type": "enum", "name": "cdc_operation",
                     "symbols": list(constants.CDC_OPERATION_NAME_TO_ID.keys())}
        },
        {
            "name": "_cdc_tran_end_time",
            "type": "string"
        },
        {
            # Will list the names of all fields that were updated in this CDC event
            "name": "_cdc_updated_fields",
            "type": {"type": "array", "items": {"type": "enum", "name": "updated_fields",
                                                "symbols": source_field_names}}
        }
    ]


# In CDC tables, are fields are nullable so that if the column is dropped from the source table, the capture instance
# does not have to be updated. We align with that by making that Avro schema for all captured fields nullable.
def avro_schema_from_sql_type(source_field_name: str, sql_type_name: str, decimal_precision: int,
                              decimal_scale: int) -> Dict[str, object]:
    if sql_type_name in ('decimal', 'numeric'):
        if not decimal_precision or not decimal_scale:
            raise Exception(
                f"Field '{source_field_name}': For SQL decimal or numeric types, the scale and precision must be"
                f"provided.")
        return {"name": source_field_name, "type": ["null", {
            "type": "bytes",
            "logicalType": "decimal",
            "precision": decimal_precision,
            "scale": decimal_scale
        }]}
    elif sql_type_name == 'bigint':
        return {"name": source_field_name, "type": ["null", "long"]}
    elif sql_type_name == 'bit':
        return {"name": source_field_name, "type": ["null", "boolean"]}
    elif sql_type_name == 'date':
        return {"name": source_field_name, "type": ["null", {"type": "int", "logicalType": "date"}]}
    elif sql_type_name in ('int', 'smallint', 'tinyint'):
        return {"name": source_field_name, "type": ["null", "int"]}
    elif sql_type_name in ('datetime', 'datetime2', 'char', 'nchar', 'varchar', 'ntext', 'nvarchar', 'text'):
        return {"name": source_field_name, "type": ["null", "string"]}
    elif sql_type_name == 'time':
        return {"name": source_field_name, "type": ["null", {"type": "int", "logicalType": "time-millis"}]}
    elif sql_type_name == 'uniqueidentifier':
        return {"name": source_field_name, "type": ["null", {"type": "string", "logicalType": "uuid"}]}
    elif sql_type_name in ('binary', 'image', 'varbinary'):
        return {"name": source_field_name, "type": ["null", "bytes"]}
    else:
        raise Exception(f"Field '{source_field_name}': I am unsure how to convert SQL type {sql_type_name} to Avro")


def avro_transform_fn_from_sql_type(sql_type_name: str) -> Callable[[object], object]:
    if sql_type_name in ('datetime', 'datetime2'):
        # We have chose to represent datetime values as ISO8601 strings rather than using the usual Avro convention of
        # an int type + 'timestamp-millis' logical type that captures them as ms since the Unix epoch. This is because
        # the latter presumes the time is in UTC, whereas we do not always know the TZ of datetimes we pull from the
        # DB. It seems more 'faithful' to represent them exactly as they exist in the DB.
        return lambda x: x and x.isoformat()
    return lambda x: x
