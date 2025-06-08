from typing import LiteralString
from pyspark.sql import DataFrame, SparkSession
from IPython.display import display

def connect() -> SparkSession:
    spark = SparkSession.builder\
        .appName("Zad11")\
        .master("yarn")\
        .config("spark.cores.max", "4")\
        .config("spark.sql.shuffle.partitions", "5")\
        .getOrCreate()
    display(spark)
    return spark


def load_table(spark: SparkSession, name: str, *, header: list[str] | None = None) -> DataFrame:
    df = spark.read\
        .option("header", "true" if header is None else "false")\
        .csv(f"../../input/{name}.csv")
    if header is not None:
        df = df.toDF(*header)
    df.createOrReplaceTempView(name)
    return df


def process(spark: SparkSession, name: str, sql: LiteralString) -> None:
    df = spark.sql(sql)
    df.write\
      .mode("overwrite")\
      .option("header", "true")\
      .csv(f"../../{name}.csv")
    df.show()
