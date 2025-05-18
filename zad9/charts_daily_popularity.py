from pyhive import hive
from utils import setup_output_table
import logging
logging.basicConfig(level=logging.INFO)

print("Connecting to Hive...")
with hive.connect(host='localhost', port=10000, configuration={'hive.stats.autogather': 'false'}) as connection, connection.cursor() as cursor:
    print("Connection established.")

    setup_output_table(cursor, "charts_daily_popularity", "region STRING, date_ DATE, popularity STRING")

    print("Executing query...")
    cursor.execute("""
    INSERT INTO charts_daily_popularity
        SELECT
            region,
            date_,
            CASE
                WHEN stream_std < -1.5 THEN 'VERY LOW'
                WHEN stream_std < -0.5 THEN 'LOW'
                WHEN stream_std > 1.5 THEN 'VERY HIGH'
                WHEN stream_std > 0.5 THEN 'HIGH'
                ELSE 'AVERAGE'
            END AS popularity
        FROM (
            SELECT
                d.region,
                d.date_,
                (d.streams - y.stream_avg) / y.stream_dev as stream_std
            FROM charts_daily_sum AS d
            JOIN charts_yearly_stats AS y
                ON YEAR(d.date_) = y.year_ AND d.region = y.region
        ) AS sub
    """)
    print("  Done.")
