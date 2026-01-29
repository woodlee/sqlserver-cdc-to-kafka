"""Table metadata classes for the replayer module."""

import itertools
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import ctds  # type: ignore[import-untyped]

from .logging_config import logger
from .models import OrderedOperation, ReplayConfig
from .utils import parse_sql_default


class TableMetadata:
    """Shared table metadata for both backfill and follow modes.

    Encapsulates column information, primary keys, and provides methods for:
    - Converting Kafka message values to database row values
    - Building SQL statements (DELETE, MERGE)
    - Creating temp tables for batch operations
    """

    def __init__(self, config: ReplayConfig, db_conn: ctds.Connection) -> None:
        self.config = config
        self.fq_target_table_name = f'[{config.target_db_table_schema.strip()}].[{config.target_db_table_name.strip()}]'
        self.cols_to_not_sync: set[str] = set([c.strip().lower() for c in config.cols_to_not_sync.split(',')])
        self.cols_to_not_sync.discard('')

        # Temp table names (set during create_temp_tables)
        temp_table_base = f'#replayer_{config.target_db_table_schema.strip()}_{config.target_db_table_name.strip()}'
        self.delete_temp_table_name: str = temp_table_base + '_delete'
        self.merge_temp_table_name: str = temp_table_base + '_merge'

        # SQL statements (set during create_temp_tables)
        self.delete_stmt: str = ''
        self.merge_stmt: str = ''

        with db_conn.cursor() as cursor:
            # Get primary key fields
            if config.primary_key_fields_override.strip():
                self.primary_key_field_names = [x.strip() for x in config.primary_key_fields_override.split(',')]
            else:
                cursor.execute('''
SELECT [COLUMN_NAME]
FROM [INFORMATION_SCHEMA].[KEY_COLUMN_USAGE]
WHERE OBJECTPROPERTY(OBJECT_ID([CONSTRAINT_SCHEMA] + '.' + QUOTENAME([CONSTRAINT_NAME])), 'IsPrimaryKey') = 1
AND [TABLE_SCHEMA] = :0
AND [TABLE_NAME] = :1
ORDER BY [ORDINAL_POSITION]
                ''', (config.target_db_table_schema, config.target_db_table_name))
                self.primary_key_field_names = [r[0] for r in cursor.fetchall()]

            # Get computed columns
            cursor.execute('''
SELECT name
FROM sys.computed_columns
WHERE OBJECT_SCHEMA_NAME(object_id) = :0
    AND OBJECT_NAME(object_id) = :1
            ''', (config.target_db_table_schema, config.target_db_table_name))
            self.computed_cols: set[str] = {r[0].lower() for r in cursor.fetchall()}

            # Get column metadata
            self.field_names: List[str] = []
            self.datetime_field_names: set[str] = set()
            self.varchar_field_names: set[str] = set()
            self.nvarchar_field_names: set[str] = set()
            self.column_defaults: Dict[str, Any] = {}
            self.pk_col_specs: List[Tuple[str, str, Optional[int], str]] = []  # (name, type, precision, is_nullable)

            cursor.execute('''
SELECT [COLUMN_NAME]
    , [DATA_TYPE]
    , COALESCE([CHARACTER_MAXIMUM_LENGTH], [DATETIME_PRECISION]) AS [PRECISION_SPEC]
    , [IS_NULLABLE]
    , [COLUMN_DEFAULT]
FROM [INFORMATION_SCHEMA].[COLUMNS]
WHERE [TABLE_SCHEMA] = :0
    AND [TABLE_NAME] = :1
ORDER BY [ORDINAL_POSITION]
            ''', (config.target_db_table_schema, config.target_db_table_name))

            for col_name, col_type, col_precision, col_is_nullable, col_default in cursor.fetchall():
                if col_name.lower() in self.cols_to_not_sync or col_name.lower() in self.computed_cols:
                    continue
                self.field_names.append(col_name)
                if col_type.lower().startswith('datetime'):
                    self.datetime_field_names.add(col_name)
                if col_type.lower() in ('char', 'varchar', 'text'):
                    self.varchar_field_names.add(col_name)
                if col_type.lower() in ('nchar', 'nvarchar', 'ntext'):
                    self.nvarchar_field_names.add(col_name)
                self.column_defaults[col_name] = parse_sql_default(col_default)
                if col_name in self.primary_key_field_names:
                    self.pk_col_specs.append((col_name, col_type, col_precision, col_is_nullable))

            # Check for identity column
            cursor.execute('SELECT TOP 1 [name] FROM sys.columns WHERE object_id = OBJECT_ID(:0) AND is_identity = 1',
                          (self.fq_target_table_name,))
            rows = cursor.fetchall()
            self.identity_col_name: Optional[str] = rows and rows[0][0] or None

    def convert_msg_to_row_values(self, msg_val: Dict[str, Any], for_bcp: bool = False) -> List[Any]:
        """Convert a Kafka message value dict to a list of database row values.

        Handles datetime conversion, varchar/nvarchar encoding, missing fields, and None values.
        """
        vals: List[Any] = []
        for f in self.field_names:
            if f not in msg_val:
                vals.append(self.column_defaults[f])
            elif msg_val[f] is None:
                vals.append(None)
            elif f in self.datetime_field_names:
                dt: datetime = datetime.fromisoformat(msg_val[f])
                if dt.year < 1753 and for_bcp:
                    # FML--something in either CTDS or FreeTDS gets weird when trying to BCP anything earlier
                    # so we're just going to standardize the cutoff for anything before this (which is likely
                    # bad data anyway):
                    dt = datetime(1753, 1, 1, 0, 0, 0)
                vals.append(dt)
            elif f in self.varchar_field_names and for_bcp:
                # The below assumes your DB uses SQL_Latin1_General_CP1_CI_AS collation; if not, you may
                # need to change 'cp1252' to something else:
                vals.append(ctds.SqlVarChar(msg_val[f].encode('cp1252')))
            elif f in self.nvarchar_field_names and for_bcp:
                # See https://zillow.github.io/ctds/bulk_insert.html#text-columns
                vals.append(ctds.SqlVarChar(msg_val[f].encode('utf-16le')))
            else:
                vals.append(msg_val[f])
        return vals

    def create_temp_tables(self, db_conn: ctds.Connection) -> None:
        """Create the temp tables needed for batch delete and merge operations.

        Also builds the DELETE and MERGE SQL statements.
        """
        with db_conn.cursor() as cursor:
            # Build delete temp table column specs
            delete_temp_table_col_specs: List[str] = []
            for col_name, col_type, col_precision, col_is_nullable in self.pk_col_specs:
                precision = f'({col_precision})' if col_precision is not None else ''
                nullability = '' if col_is_nullable else 'NOT NULL'
                delete_temp_table_col_specs.append(f'[{col_name}] {col_type}{precision} {nullability}')

            # Create merge temp table
            cursor.execute(f'DROP TABLE IF EXISTS {self.merge_temp_table_name};')
            # Yep, this looks weird--it's a hack to prevent SQL Server from copying over the IDENTITY property
            # of any columns that have it whenever it creates the temp table. https://stackoverflow.com/a/57509258
            cursor.execute(f'SELECT TOP 0 * INTO {self.merge_temp_table_name} FROM {self.fq_target_table_name} '
                          f'UNION ALL SELECT * FROM {self.fq_target_table_name} WHERE 1 <> 1;')
            for c in itertools.chain(self.cols_to_not_sync, self.computed_cols):
                cursor.execute(f'ALTER TABLE {self.merge_temp_table_name} DROP COLUMN [{c}];')

            # Create delete temp table
            cursor.execute(f'DROP TABLE IF EXISTS {self.delete_temp_table_name};')
            cursor.execute(f'''
CREATE TABLE {self.delete_temp_table_name} (
    {",".join(delete_temp_table_col_specs)},
    CONSTRAINT [PK_{self.delete_temp_table_name}]
    PRIMARY KEY ([{"], [".join(self.primary_key_field_names)}])
);
            ''')

            # Build DELETE statement
            delete_join_predicates = ' AND '.join([f'tgt.[{c}] = dtt.[{c}]' for c in self.primary_key_field_names])
            self.delete_stmt = f'''
DELETE tgt
FROM {self.fq_target_table_name} AS tgt
INNER JOIN {self.delete_temp_table_name} AS dtt ON ({delete_join_predicates});

TRUNCATE TABLE {self.delete_temp_table_name};
            '''

            # Build MERGE statement
            set_identity_insert = f'SET IDENTITY_INSERT {self.fq_target_table_name} ON; ' \
                if self.identity_col_name else ''
            merge_match_predicates = ' AND '.join([f'tgt.[{c}] = src.[{c}]' for c in self.primary_key_field_names])

            # This is a real edge case, but if all the table cols are in the PK, then SQL always models an
            # update as an insert+delete in CDC data, so the WHEN MATCHED THEN UPDATE SET would wind up empty
            # which is syntactically invalid:
            if set(self.field_names) == set(self.primary_key_field_names):
                self.merge_stmt = f'''
{set_identity_insert}
MERGE {self.fq_target_table_name} AS tgt
USING {self.merge_temp_table_name} AS src
    ON ({merge_match_predicates})
WHEN NOT MATCHED THEN
    INSERT ([{'], ['.join(self.field_names)}]) VALUES (src.[{'], src.['.join(self.field_names)}]);

TRUNCATE TABLE {self.merge_temp_table_name};
                '''
            else:
                self.merge_stmt = f'''
{set_identity_insert}
MERGE {self.fq_target_table_name} AS tgt
USING {self.merge_temp_table_name} AS src
    ON ({merge_match_predicates})
WHEN MATCHED THEN
    UPDATE SET {", ".join([f'[{x}] = src.[{x}]' for x in self.field_names if x not in self.primary_key_field_names and x != self.identity_col_name])}
WHEN NOT MATCHED THEN
    INSERT ([{'], ['.join(self.field_names)}]) VALUES (src.[{'], src.['.join(self.field_names)}]);

TRUNCATE TABLE {self.merge_temp_table_name};
                '''

            logger.debug(f"Created temp tables for {self.fq_target_table_name}")


class FollowModeTableMetadata(TableMetadata):
    """Extends TableMetadata with follow-mode specific functionality.

    Adds operation counters, parameterized INSERT/UPDATE statements, and a method
    to prepare OrderedOperations from messages.
    """

    def __init__(self, config: ReplayConfig, db_conn: ctds.Connection) -> None:
        super().__init__(config, db_conn)
        self.delete_cnt = 0
        self.upsert_cnt = 0

        # Build parameterized INSERT statement for 'Insert' CDC operations
        set_identity_insert = f'SET IDENTITY_INSERT {self.fq_target_table_name} ON; ' \
            if self.identity_col_name else ''
        self.insert_stmt = f'''
{set_identity_insert}INSERT INTO {self.fq_target_table_name} ([{'], ['.join(self.field_names)}])
VALUES ({', '.join([':' + str(i) for i in range(len(self.field_names))])})
'''

        # Build parameterized UPDATE statement for 'PostUpdate' CDC operations
        # SET clause excludes PK columns and identity column; WHERE clause uses PK columns
        self._non_pk_fields = [f for f in self.field_names
                              if f not in self.primary_key_field_names and f != self.identity_col_name]
        set_clause = ', '.join([f'[{f}] = :{i}' for i, f in enumerate(self._non_pk_fields)])
        where_clause = ' AND '.join([f'[{pk}] = :{len(self._non_pk_fields) + i}'
                                     for i, pk in enumerate(self.primary_key_field_names)])
        self.update_stmt = f'''
UPDATE {self.fq_target_table_name}
SET {set_clause}
WHERE {where_clause}
'''

        logger.info(f"Follow mode: initialized table metadata for {self.fq_target_table_name}")

    def prepare_operation(self, msg_key: Optional[Dict[str, Any]],
                         msg_val: Optional[Dict[str, Any]]) -> Optional[OrderedOperation]:
        """Prepare an OrderedOperation from a message without executing it.

        Returns None if msg_key is None.
        """
        if msg_key is None:
            return None

        key_val = tuple((msg_key[x] for x in self.primary_key_field_names))
        cdc_operation = msg_val['__operation']

        if msg_val is None or msg_val.get('__operation') == 'Delete':
            self.delete_cnt += 1
            return OrderedOperation(
                original_topic=self.config.replay_topic,
                operation_type='delete',
                key_val=key_val,
                row_values=None,
                cdc_operation=cdc_operation
            )
        else:
            vals = self.convert_msg_to_row_values(msg_val)
            self.upsert_cnt += 1
            return OrderedOperation(
                original_topic=self.config.replay_topic,
                operation_type='upsert',
                key_val=key_val,
                row_values=vals,
                cdc_operation=cdc_operation
            )

    def build_update_params(self, row_values: List[Any]) -> Tuple[Any, ...]:
        """Reorder row values for UPDATE: non-PK fields first (excluding identity), then PK fields."""
        field_to_val = dict(zip(self.field_names, row_values))
        non_pk_vals = [field_to_val[f] for f in self._non_pk_fields]
        pk_vals = [field_to_val[f] for f in self.primary_key_field_names]
        return tuple(non_pk_vals + pk_vals)

    def log_stats(self) -> None:
        """Log processing statistics."""
        logger.info(f"Follow mode table {self.config.replay_topic}: "
                   f"processed {self.delete_cnt} deletes, {self.upsert_cnt} upserts")
