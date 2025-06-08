from contextlib import contextmanager
import os
import time
from typing import Generator, LiteralString
from pyspark.sql import DataFrame, SparkSession
from IPython.display import display

def connect() -> SparkSession:
    print("Connecting to Spark...")
    spark = SparkSession.builder\
        .appName("Zad11")\
        .master("yarn")\
        .config("spark.cores.max", "4")\
        .config("spark.sql.shuffle.partitions", "5")\
        .getOrCreate()
    display(spark)
    return spark


def load_table(spark: SparkSession, path: LiteralString, *, header: list[str] | None = None) -> DataFrame:
    name = os.path.basename(path).split('.')[0]
    print(f"Loading '{path}' into table '{name}'...")
    df = spark.read\
        .option("header", "true" if header is None else "false")\
        .csv(path)
    if header is not None:
        df = df.toDF(*header)
    df.createOrReplaceTempView(name)
    return df


@contextmanager
def bench() -> Generator[None, None, None]:
    start_time = time.time_ns()
    yield
    end_time = time.time_ns()
    print(f"Execution time: {(end_time - start_time) / 1e9:.3f} seconds")


def process(spark: SparkSession, name: LiteralString, sql: LiteralString) -> None:
    print(f"Processing query and saving to '/{name}/*'...")
    with bench():
        df = spark.sql(sql)
        df.write\
          .mode("overwrite")\
          .option("header", "true")\
          .csv(f"/{name}")
        df.show()
