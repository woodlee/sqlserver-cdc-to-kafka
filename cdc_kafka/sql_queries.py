from typing import List, Tuple, Iterable, Collection, Optional

import pyodbc

from . import constants

# Methods in this module should return (<the SQL query text>, <a list of the query parameters' type specifications>),
# where the param specs are tuples of (odbc_type, column_size, decimal_digits)


def get_cdc_capture_instances_metadata() -> Tuple[str, List[Tuple[int, int, Optional[int]]]]:
    return f'''
-- cdc-to-kafka: get_cdc_capture_instances_metadata
SELECT
    OBJECT_SCHEMA_NAME(source_object_id) AS schema_name
    , OBJECT_NAME(source_object_id) AS table_name
    , capture_instance
    , start_lsn
    , create_date
FROM [{constants.CDC_DB_SCHEMA_NAME}].[change_tables]
ORDER BY source_object_id
    ''', []


def get_cdc_tracked_tables_metadata(capture_instance_names: List[str]) -> \
        Tuple[str, List[Tuple[int, int, Optional[int]]]]:
    ci_list = ", ".join([f"'{x}'" for x in capture_instance_names])
    return f'''
-- cdc-to-kafka: get_cdc_tracked_tables_metadata    
SELECT
    OBJECT_SCHEMA_NAME(ct.source_object_id) AS schema_name
    , OBJECT_NAME(ct.source_object_id) AS table_name
    , ct.capture_instance AS capture_instance_name
    , ct.start_lsn AS capture_min_lsn
    , cc.column_ordinal AS change_table_ordinal
    , cc.column_name AS column_name
    , cc.column_type AS sql_type_name
    , ic.index_ordinal AS primary_key_ordinal
    , sc.precision AS decimal_precision
    , sc.scale AS decimal_scale
FROM
    [{constants.CDC_DB_SCHEMA_NAME}].[change_tables] AS ct
    INNER JOIN [{constants.CDC_DB_SCHEMA_NAME}].[captured_columns] AS cc ON (ct.object_id = cc.object_id)
    LEFT JOIN [{constants.CDC_DB_SCHEMA_NAME}].[index_columns] AS ic 
        ON (cc.object_id = ic.object_id AND cc.column_id = ic.column_id)
    LEFT JOIN sys.columns AS sc ON (sc.object_id = ct.source_object_id AND sc.column_id = cc.column_id)
WHERE ct.capture_instance IN ({ci_list})
ORDER BY ct.object_id, cc.column_ordinal
    ''', []


def get_latest_cdc_entry_time() -> Tuple[str, List[Tuple[int, int, Optional[int]]]]:
    return f'''
-- cdc-to-kafka: get_latest_cdc_entry_time    
SELECT TOP 1 tran_end_time 
FROM [{constants.CDC_DB_SCHEMA_NAME}].[lsn_time_mapping] 
ORDER BY tran_end_time DESC
    ''', []


def get_change_rows_per_second(change_table_name: str) -> Tuple[str, List[Tuple[int, int, Optional[int]]]]:
    return f'''
-- cdc-to-kafka: get_change_rows_per_second    
SELECT ISNULL(COUNT(*) / NULLIF(DATEDIFF(second, MIN(ltm.tran_end_time), MAX(ltm.tran_end_time)), 0), 0)
FROM [{constants.CDC_DB_SCHEMA_NAME}].[{change_table_name}] AS ct WITH (NOLOCK)
INNER JOIN [{constants.CDC_DB_SCHEMA_NAME}].[lsn_time_mapping] AS ltm WITH (NOLOCK) ON ct.__$start_lsn = ltm.start_lsn
    ''', []


def get_change_table_index_cols(change_table_name: str) -> Tuple[str, List[Tuple[int, int, Optional[int]]]]:
    return f'''
-- cdc-to-kafka: get_change_table_index_cols
SELECT COL_NAME(ic.object_id, ic.column_id)
FROM sys.indexes AS i
INNER JOIN sys.index_columns AS ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
WHERE i.object_id = OBJECT_ID('{constants.CDC_DB_SCHEMA_NAME}.{change_table_name}') AND type_desc = 'CLUSTERED'
ORDER BY key_ordinal
    ''', []


def get_date() -> Tuple[str, List[Tuple[int, int, Optional[int]]]]:
    return 'SELECT GETDATE()', []


def get_table_count(schema_name: str, table_name: str, pk_cols: Tuple[str],
                    odbc_columns: Tuple[Tuple]) -> Tuple[str, List[Tuple[int, int, Optional[int]]]]:
    declarations, where_spec, params = _get_snapshot_query_bits(pk_cols, odbc_columns, ('>=', '<='))

    return f'''
-- cdc-to-kafka: get_table_count
DECLARE 
    {declarations}
;

SELECT COUNT(*)
FROM [{schema_name}].[{table_name}]
WHERE {where_spec}
    ''', params


def get_max_key_value(schema_name: str, table_name: str, pk_cols: Tuple[str]) -> \
        Tuple[str, List[Tuple[int, int, Optional[int]]]]:
    select_spec = ", ".join([f'[{x}]' for x in pk_cols])
    order_by_spec = ", ".join([f'[{x}] DESC' for x in pk_cols])
    return f'''
-- cdc-to-kafka: get_max_key_value
SELECT TOP 1 {select_spec}
FROM [{schema_name}].[{table_name}] ORDER BY {order_by_spec}
    ''', []


def get_min_key_value(schema_name: str, table_name: str, pk_cols: Tuple[str]) -> \
        Tuple[str, List[Tuple[int, int, Optional[int]]]]:
    select_spec = ", ".join([f'[{x}]' for x in pk_cols])
    order_by_spec = ", ".join([f'[{x}] ASC' for x in pk_cols])
    return f'''
-- cdc-to-kafka: get_min_key_value
SELECT TOP 1 {select_spec}
FROM [{schema_name}].[{table_name}] ORDER BY {order_by_spec}
    ''', []


def get_change_table_count_by_operation(change_table_name: str) -> Tuple[str, List[Tuple[int, int, Optional[int]]]]:
    return f'''
-- cdc-to-kafka: get_change_table_count_by_operation
DECLARE 
    @LSN BINARY(10) = ?
    , @SEQVAL BINARY(10) = ?
    , @OPERATION INT = ?
;

SELECT 
    COUNT(*)
    , __$operation AS op
FROM [{constants.CDC_DB_SCHEMA_NAME}].[{change_table_name}] WITH (NOLOCK)
WHERE __$operation != 3 
    AND (
        __$start_lsn < @LSN
        OR __$start_lsn = @LSN AND __$seqval < @SEQVAL
        OR __$start_lsn = @LSN AND __$seqval = @SEQVAL AND __$operation <= @OPERATION
    )
GROUP BY __$operation
    ''', [(pyodbc.SQL_BINARY, 10, None), (pyodbc.SQL_BINARY, 10, None), (pyodbc.SQL_INTEGER, 4, None)]


def get_max_lsn() -> Tuple[str, List[Tuple[int, int, Optional[int]]]]:
    return 'SELECT sys.fn_cdc_get_max_lsn()', []


def get_change_rows(change_table_name: str, field_names: Iterable[str],
                    ct_index_cols: Iterable[str]) -> Tuple[str, List[Tuple[int, int, Optional[int]]]]:
    # You may feel tempted to change or simplify this query. TREAD CAREFULLY. There was a lot of iterating here to
    # craft something that would not induce SQL Server to resort to a full index scan. If you change it, run some
    # EXPLAINs and ensure that the steps are still only index SEEKs, not scans.

    # See comments in _get_snapshot_query_bits to understand other details of why these queries look as they do,
    # esp. in regard to the presence of DECLARE statements within them.

    select_column_specs = ', '.join([f'ct.[{f}]' for f in field_names])
    order_spec = ', '.join([f'[{f}]' for f in ct_index_cols])
    return f'''
-- cdc-to-kafka: get_change_rows
DECLARE 
    @LSN BINARY(10) = ?
    , @SEQ BINARY(10) = ?
    , @MAX_LSN BINARY(10) = ?
;

WITH ct AS (
    SELECT *
    FROM [{constants.CDC_DB_SCHEMA_NAME}].[{change_table_name}] AS ct WITH (NOLOCK)
    WHERE ct.__$start_lsn = @LSN AND ct.__$seqval > @SEQ AND ct.__$start_lsn <= @MAX_LSN

    UNION ALL

    SELECT *
    FROM [{constants.CDC_DB_SCHEMA_NAME}].[{change_table_name}] AS ct WITH (NOLOCK)
    WHERE ct.__$start_lsn > @LSN AND ct.__$start_lsn <= @MAX_LSN
)
SELECT TOP ({constants.DB_ROW_BATCH_SIZE})
    ct.__$operation AS {constants.OPERATION_NAME}
    , ltm.tran_end_time AS {constants.EVENT_TIME_NAME}
    , ct.__$start_lsn AS {constants.LSN_NAME}
    , ct.__$seqval AS {constants.SEQVAL_NAME}
    , ct.__$update_mask AS {constants.UPDATED_FIELDS_NAME}
    , {select_column_specs}
FROM ct 
INNER JOIN [{constants.CDC_DB_SCHEMA_NAME}].[lsn_time_mapping] AS ltm WITH (NOLOCK) ON (ct.__$start_lsn = ltm.start_lsn)
WHERE ct.__$operation = 1 OR ct.__$operation = 2 OR ct.__$operation = 4
ORDER BY {order_spec}
    ''', [(pyodbc.SQL_BINARY, 10, None)] * 3


def get_snapshot_rows(
        schema_name: str, table_name: str, field_names: Collection[str], pk_cols: Collection[str], first_read: bool,
        odbc_columns: Collection[Tuple]) -> Tuple[str, List[Tuple[int, int, Optional[int]]]]:
    select_column_specs = ', '.join([f'[{f}]' for f in field_names])
    order_spec = ', '.join([f'[{x}] DESC' for x in pk_cols])

    if first_read:
        declarations = '@K0 int = 0'
        where_spec = '1=1'
        params = []
    else:
        declarations, where_spec, params = _get_snapshot_query_bits(pk_cols, odbc_columns, ('<', ))

    return f'''
-- cdc-to-kafka: get_snapshot_rows
DECLARE 
    {declarations}
;

SELECT TOP ({constants.DB_ROW_BATCH_SIZE})
    {constants.SNAPSHOT_OPERATION_ID} AS {constants.OPERATION_NAME}
    , GETDATE() AS {constants.EVENT_TIME_NAME}
    , NULL AS {constants.LSN_NAME}
    , NULL AS {constants.SEQVAL_NAME}
    , NULL AS {constants.UPDATED_FIELDS_NAME}
    , {select_column_specs}
FROM
    [{schema_name}].[{table_name}]
WHERE {where_spec}
ORDER BY {order_spec}
    ''', params


def _get_snapshot_query_bits(pk_cols: Collection[str], odbc_columns: Iterable[Tuple], comparators: Iterable[str]) \
        -> Tuple[str, str, List[Tuple[int, int, Optional[int]]]]:
    # For multi-column primary keys, this builds a WHERE clause of the following form, assuming
    # for example a PK on (field_a, field_b, field_c):
    #   WHERE (field_a < @K0)
    #    OR (field_a = @K0 AND field_b < @K1)
    #    OR (field_a = @K0 AND field_b = @K1 AND field_c < @K2)

    # You may find it odd that this query (as well as the change data query) has DECLARE statements in it.
    # Why not just pass the parameters with the query like usual? We found that in composite-key cases,
    # the need to pass the parameter for the bounding value of the non-last column(s) more than once caused
    # SQL Server to treat those as different values (even though they were actually the same), and this
    # messed up query plans and caused poor performance esp. since we're asking for results ordered
    # backwards against the PK's index
    #
    # Having the second layer of "declare indirection" seemed to be the only way to arrange reuse of the
    # same passed parameter in more than one place via pyodbc, which only supports '?' positional
    # placeholders for parameters.

    odbc_types = {x[3]: (x[3], x[4], x[5], x[6], x[8]) for x in odbc_columns}
    pk_odbc_cols = [odbc_types[col_name] for col_name in pk_cols]

    comparator_where_clauses = []
    param_declarations = []
    params = []

    for comparator_ix, comparator in enumerate(comparators):
        also_equal = '=' in comparator
        comparator = comparator.replace('=', '')
        key_where_clauses = []

        for pk_ix, (col_name, data_type, type_name, column_size, decimal_digits) in enumerate(pk_odbc_cols):
            type_name = type_name.replace('identity', '')
            if 'char' in type_name:
                type_name += f'({column_size})'
            params.append((data_type, column_size, decimal_digits))
            param_ix = len(params) - 1
            param_declarations.append(f'@K{param_ix} {type_name} = ?')

            inner_clauses = []

            for jx, prior_field in enumerate(pk_cols[0:pk_ix]):
                prior_ix = jx + comparator_ix * len(pk_cols)
                inner_clauses.append(f'[{prior_field}] = @K{prior_ix}')
            inner_clauses.append(f'[{col_name}] {comparator} @K{param_ix}')
            if also_equal and pk_ix == len(pk_odbc_cols) - 1:
                inner_clauses[-1] = inner_clauses[-1] .replace(comparator, comparator + '=')

            key_where_clauses.append(f"\n    ({' AND '.join(inner_clauses)})")

        comparator_where_clauses.append(f"({' OR '.join(key_where_clauses)})")

    declarations = ', '.join(param_declarations)
    where_spec = '\n  AND '.join(comparator_where_clauses)

    return declarations, where_spec, params
