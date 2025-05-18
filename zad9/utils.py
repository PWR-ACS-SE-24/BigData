from typing import LiteralString
from pyhive import hive


def setup_output_table(cursor: hive.Cursor, table_name: LiteralString, fields: LiteralString) -> None:
    print(f"Setting up table {table_name}...")
    print("  Dropping table...")
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    print("  Creating table...")
    cursor.execute(f"CREATE TABLE {table_name} ({fields})")
    print("  Done.")
