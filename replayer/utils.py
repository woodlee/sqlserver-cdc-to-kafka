import argparse
from typing import Any, Optional


def get_pyodbc_conn_string_from_opts(opts: argparse.Namespace) -> str:
    return (f'DRIVER={{ODBC Driver 18 for SQL Server}};'
            f'SERVER={opts.target_db_server};'
            f'DATABASE={opts.target_db_database};'
            f'UID={opts.target_db_user};'
            f'PWD={opts.target_db_password};'
            f'TrustServerCertificate=yes;')


def parse_sql_default(col_default: Optional[str]) -> Any:
    """Parse a SQL Server column default value into a Python value.

    Handles string literals, numeric literals, and NULL values.
    Returns None for complex expressions (functions like getdate(), newid(), etc.).
    """
    if not col_default:
        return None

    val = col_default.strip()
    while val.startswith('(') and val.endswith(')'):
        val = val[1:-1].strip()

    if (not val) or val.upper() == 'NULL':
        return None

    # String literal: 'text' or N'text'
    if val.upper().startswith("N'") and val.endswith("'"):
        return val[2:-1].replace("''", "'")  # Unescape doubled quotes
    if val.startswith("'") and val.endswith("'"):
        return val[1:-1].replace("''", "'")

    # Numeric literal
    try:
        if '.' in val:
            return float(val)
        return int(val)
    except ValueError:
        pass

    # Anything else (functions like getdate(), newid(), complex expressions)
    # Return None and let SQL Server handle it or accept NULL
    return None
