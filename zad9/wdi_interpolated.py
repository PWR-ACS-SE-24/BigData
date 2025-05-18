from pyhive import hive
from utils import setup_output_table
import logging
logging.basicConfig(level=logging.INFO)

print("Connecting to Hive...")
with hive.connect(host='localhost', port=10000, configuration={'hive.stats.autogather': 'false'}) as connection, connection.cursor() as cursor:
    print("Connection established.")

    setup_output_table(cursor, "wdi_interpolated", "country STRING, date_ DATE, rural_population_percent DOUBLE, fertility_rate DOUBLE, gdp_per_capita_usd DOUBLE, mobile_subscriptions_per_100 DOUBLE, refugee_population_promille DOUBLE")

    print("Executing query...")
    cursor.execute("""
    INSERT INTO wdi_interpolated
        SELECT
            prv.country,
            d.date_,
            prv.rural_population_percent * (1 - (DATE_FORMAT(d.date_, 'D') / 365)) + nxt.rural_population_percent * (DATE_FORMAT(d.date_, 'D') / 365) AS rural_population_percent,
            prv.fertility_rate * (1 - (DATE_FORMAT(d.date_, 'D') / 365)) + nxt.fertility_rate * (DATE_FORMAT(d.date_, 'D') / 365) AS fertility_rate,
            prv.gdp_per_capita_usd * (1 - (DATE_FORMAT(d.date_, 'D') / 365)) + nxt.gdp_per_capita_usd * (DATE_FORMAT(d.date_, 'D') / 365) AS gdp_per_capita_usd,
            prv.mobile_subscriptions_per_100 * (1 - (DATE_FORMAT(d.date_, 'D') / 365)) + nxt.mobile_subscriptions_per_100 * (DATE_FORMAT(d.date_, 'D') / 365) AS mobile_subscriptions_per_100,
            prv.refugee_population_promille * (1 - (DATE_FORMAT(d.date_, 'D') / 365)) + nxt.refugee_population_promille * (DATE_FORMAT(d.date_, 'D') / 365) AS refugee_population_promille
        FROM (
            SELECT DATE_ADD('2017-01-01', counter.i) AS date_
            FROM (select 1) sub_
            LATERAL VIEW POSEXPLODE(SPLIT(SPACE(DATEDIFF('2021-12-31', '2017-01-01')), ' ')) counter AS i, space_
        ) AS d
        JOIN wdi_normalized AS nxt
            ON YEAR(d.date_) = nxt.year_
        JOIN wdi_normalized AS prv
            ON YEAR(d.date_) - 1 = prv.year_ AND prv.country = nxt.country
        ORDER BY prv.country, d.date_
    """)
    print("  Done.")
