from contextlib import contextmanager
import os
import time
from typing import Generator, LiteralString
from pyspark.sql import DataFrame, SparkSession
from IPython.display import display

def connect(*, cores_max: int = 4, shuffle_partitions: int = 5, executor_memory: int = 1024, broadcast_threshold: int = 10) -> SparkSession:
    print("Connecting to Spark...")
    spark = SparkSession.builder\
        .appName("Zad11")\
        .master("yarn")\
        .config("spark.cores.max", str(cores_max))\
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))\
        .config("spark.executor.memory", f"{executor_memory}m")\
        .config("spark.sql.autoBroadcastJoinThreshold", str(broadcast_threshold * 1024 * 1024) if broadcast_threshold > 0 else -1)\
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
        df.count()
    df.show()
    df.createOrReplaceTempView(name)
    df.write\
          .mode("overwrite")\
          .option("header", "true")\
          .csv(f"/{name}")


def example(spark: SparkSession, name: LiteralString) -> None:
    spark.sql(f"SELECT * FROM {name}").show(truncate=False, n=1000)