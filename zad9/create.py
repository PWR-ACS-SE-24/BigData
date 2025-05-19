import logging
from typing import LiteralString
import subprocess
from uuid import uuid4
from utils import connect_to_hive

with connect_to_hive(level=logging.ERROR) as cursor:

    def run_cmd(cmd: str) -> None:
        subprocess.call([cmd], shell=True)

    def setup_table(
        table_name: LiteralString, fields: LiteralString, *, remove_header: bool = False
    ) -> None:
        print(f"Setting up table {table_name}...")
        print("  Dropping table...")
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        print("  Creating table...")
        cursor.execute(
            f"CREATE TABLE {table_name} ({fields}) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE"
        )
        print("  Copying input data...")
        tmp_name = uuid4().hex
        run_cmd(f"hdfs dfs -cp /input/{table_name}.csv /{tmp_name}.csv")
        if remove_header:
            print("  Removing header...")
            old_tmp_name = tmp_name
            tmp_name = uuid4().hex
            run_cmd(
                f"hdfs dfs -cat /{old_tmp_name}.csv | tail -n +2 | hdfs dfs -put - /{tmp_name}.csv"
            )
        print("  Loading data into table...")
        cursor.execute(f"LOAD DATA INPATH '/{tmp_name}.csv' INTO TABLE {table_name}")
        print("  Done.")

    setup_table("charts_daily_sum_small", "region STRING, date_ DATE, streams BIGINT")
    setup_table("charts_daily_sum", "region STRING, date_ DATE, streams BIGINT")
    setup_table(
        "daily_weather_small",
        "station_id STRING, date_ DATE, avg_temp_c DOUBLE, precipitation_mm DOUBLE",
        remove_header=True,
    )
    setup_table(
        "daily_weather_2017",
        "station_id STRING, date_ DATE, avg_temp_c DOUBLE, precipitation_mm DOUBLE",
        remove_header=True,
    )
    setup_table(
        "cities",
        "station_id STRING, city_name STRING, country STRING, state STRING, iso2 STRING, iso3 STRING, lat DOUBLE, lon DOUBLE",
        remove_header=True,
    )
    setup_table(
        "cities_small",
        "station_id STRING, city_name STRING, country STRING, state STRING, iso2 STRING, iso3 STRING, lat DOUBLE, lon DOUBLE",
        remove_header=True,
    )
    setup_table(
        "WDIData_2017",
        "country_name STRING, indicator_code STRING, y2016 DOUBLE, y2017 DOUBLE, y2018 DOUBLE, y2019 DOUBLE, y2020 DOUBLE, y2021 DOUBLE, y2022 DOUBLE",
        remove_header=True,
    )
