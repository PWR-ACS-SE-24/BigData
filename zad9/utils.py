from contextlib import contextmanager
from typing import Generator, LiteralString
from pyhive import hive
import time
import logging
import subprocess


def preview_table(cursor: hive.Cursor, table_name: LiteralString) -> None:
    cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")
    print(cursor.fetchall()[0])


def setup_output_table(
    cursor: hive.Cursor, table_name: LiteralString, fields: LiteralString
) -> None:
    print(f"Setting up table {table_name}...")
    print("  Dropping table...")
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    print("  Creating table...")
    cursor.execute(f"CREATE TABLE {table_name} ({fields})")
    print("  Done.")


@contextmanager
def connect_to_hive(level=logging.INFO, *, reducers: int = 1, split_mb: int = 256) -> Generator[hive.Cursor, None, None]:
    logging.basicConfig(level=level)
    print("Connecting to Hive...")
    with (
        hive.connect(
            host="localhost",
            port=10000,
            configuration={
                "hive.execution.engine": "spark",
                "hive.stats.autogather": "false",
                # "mapreduce.job.reduces": str(reducers),
                # "mapreduce.input.fileinputformat.split.maxsize": str(split_mb * 1024 * 1024),
            },
        ) as connection,
        connection.cursor() as cursor,
    ):
        print("Connection established.")
        yield cursor


@contextmanager
def bench() -> Generator[None, None, None]:
    print("Executing query...")
    start_time = time.time_ns()
    yield
    end_time = time.time_ns()
    print(f"  Execution time: {(end_time - start_time) / 1e9:.3f} seconds")
    print("  " + subprocess.run("yarn application -list -appStates ALL 2>/dev/null | tail -n +3 | head -n 1", shell=True, capture_output=True, text=True).stdout.strip())
    print("  Done.")
