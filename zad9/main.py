from typing import LiteralString
from pyhive import hive
import subprocess
from uuid import uuid4

# INITIALIZATION

print("Connecting to Hive...")
connection = hive.connect(host='localhost', port=10000, configuration={'hive.stats.autogather': 'false'})
cursor = connection.cursor()
print("Connection established.")

# FUNCTIONS

def run_cmd(cmd: str) -> None:
    subprocess.call([cmd], shell=True)

def setup_table(table_name: LiteralString, fields: LiteralString, *, remove_header: bool = False) -> None:
    print(f"Setting up table {table_name}...")
    print("  Dropping table...")
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    print("  Creating table...")
    cursor.execute(f"CREATE TABLE {table_name} ({fields}) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    print("  Copying input data...")
    tmp_name = uuid4().hex
    run_cmd(f"hdfs dfs -cp /input/{table_name}.csv /{tmp_name}.csv")
    if remove_header:
        print("  Removing header...")
        old_tmp_name = tmp_name
        tmp_name = uuid4().hex
        run_cmd(f"hdfs dfs -cat /{old_tmp_name}.csv | tail -n +2 | hdfs dfs -put - /{tmp_name}.csv")
    print("  Loading data into table...")
    cursor.execute(f"LOAD DATA INPATH '/{tmp_name}.csv' INTO TABLE {table_name}")
    print("  Done.")

# MAIN LOGIC

setup_table("charts_daily_sum_small", "region STRING, date_ DATE, streams BIGINT")
setup_table("charts_daily_sum", "region STRING, date_ DATE, streams BIGINT")
setup_table("daily_weather_small", "station_id STRING, date_ DATE, avg_temp_c DOUBLE, precipitation_mm DOUBLE", remove_header=True)
setup_table("daily_weather_2017", "station_id STRING, date_ DATE, avg_temp_c DOUBLE, precipitation_mm DOUBLE", remove_header=True)
setup_table("cities", "station_id STRING, city_name STRING, country STRING, state STRING, iso2 STRING, iso3 STRING, lat DOUBLE, lon DOUBLE", remove_header=True)
setup_table("cities_small", "station_id STRING, city_name STRING, country STRING, state STRING, iso2 STRING, iso3 STRING, lat DOUBLE, lon DOUBLE", remove_header=True)
setup_table("WDIData", "country_name STRING, country_code STRING, indicator_name STRING, indicator_code STRING, y1960 DOUBLE, y1961 DOUBLE, y1962 DOUBLE, y1963 DOUBLE, y1964 DOUBLE, y1965 DOUBLE, y1966 DOUBLE, y1967 DOUBLE, y1968 DOUBLE, y1969 DOUBLE, y1970 DOUBLE, y1971 DOUBLE, y1972 DOUBLE, y1973 DOUBLE, y1974 DOUBLE, y1975 DOUBLE, y1976 DOUBLE, y1977 DOUBLE, y1978 DOUBLE, y1979 DOUBLE, y1980 DOUBLE, y1981 DOUBLE, y1982 DOUBLE, y1983 DOUBLE, y1984 DOUBLE, y1985 DOUBLE, y1986 DOUBLE, y1987 DOUBLE, y1988 DOUBLE, y1989 DOUBLE, y1990 DOUBLE, y1991 DOUBLE, y1992 DOUBLE, y1993 DOUBLE, y1994 DOUBLE, y1995 DOUBLE, y1996 DOUBLE, y1997 DOUBLE, y1998 DOUBLE, y1999 DOUBLE, y2000 DOUBLE, y2001 DOUBLE, y2002 DOUBLE, y2003 DOUBLE, y2004 DOUBLE, y2005 DOUBLE, y2006 DOUBLE, y2007 DOUBLE, y2008 DOUBLE, y2009 DOUBLE, y2010 DOUBLE, y2011 DOUBLE, y2012 DOUBLE, y2013 DOUBLE, y2014 DOUBLE, y2015 DOUBLE, y2016 DOUBLE, y2017 DOUBLE, y2018 DOUBLE, y2019 DOUBLE, y2020 DOUBLE, y2021 DOUBLE, y2022 DOUBLE", remove_header=True)

# CLEANUP

cursor.close()
connection.close()
