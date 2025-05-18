from pyhive import hive
from utils import setup_output_table
import logging
logging.basicConfig(level=logging.INFO)

print("Connecting to Hive...")
with hive.connect(host='localhost', port=10000, configuration={'hive.stats.autogather': 'false'}) as connection, connection.cursor() as cursor:
    print("Connection established.")

    setup_output_table(cursor, "charts_yearly_stats", "region STRING, year_ INT, stream_avg DOUBLE, stream_dev DOUBLE")

    print("Executing query...")
    cursor.execute("""
    INSERT INTO charts_yearly_stats
        SELECT
            region,
            YEAR(date_) as year_,
            AVG(streams) as stream_avg,
            STDDEV(streams) as stream_dev
        FROM charts_daily_sum
        GROUP BY region, YEAR(date_)
        ORDER BY region, YEAR(date_)
    """)
    print("  Done.")
