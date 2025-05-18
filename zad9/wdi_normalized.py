from pyhive import hive
from utils import setup_output_table
import logging
logging.basicConfig(level=logging.INFO)

print("Connecting to Hive...")
with hive.connect(host='localhost', port=10000, configuration={'hive.stats.autogather': 'false'}) as connection, connection.cursor() as cursor:
    print("Connection established.")

    setup_output_table(cursor, "wdi_normalized", "country STRING, year_ INT, rural_population_percent DOUBLE, fertility_rate DOUBLE, gdp_per_capita_usd DOUBLE, mobile_subscriptions_per_100 DOUBLE, refugee_population_promille DOUBLE")

    print("Executing query...")
    cursor.execute("""
        WITH wdi AS (
            SELECT
                country_name as country,
                indicator_code as code,
                y2016, y2017, y2018, y2019, y2020, y2021
            FROM WDIData_2017
            WHERE indicator_code IN ('SP.RUR.TOTL.ZS', 'SP.DYN.TFRT.IN', 'NY.GDP.PCAP.CD', 'IT.CEL.SETS.P2', 'SM.POP.REFG', 'SP.POP.TOTL')
        )
    INSERT INTO wdi_normalized
        SELECT
            country,
            year_,
            rural_population_percent,
            fertility_rate,
            gdp_per_capita_usd,
            mobile_subscriptions_per_100,
            refugee_population / total_population * 1000 AS refugee_population_promille
        FROM (
            SELECT
                country, year_,
                MAX(CASE WHEN code = 'SP.RUR.TOTL.ZS' THEN value END) AS rural_population_percent,
                MAX(CASE WHEN code = 'SP.DYN.TFRT.IN' THEN value END) AS fertility_rate,
                MAX(CASE WHEN code = 'NY.GDP.PCAP.CD' THEN value END) AS gdp_per_capita_usd,
                MAX(CASE WHEN code = 'IT.CEL.SETS.P2' THEN value END) AS mobile_subscriptions_per_100,
                MAX(CASE WHEN code = 'SM.POP.REFG' THEN value END) AS refugee_population,
                MAX(CASE WHEN code = 'SP.POP.TOTL' THEN value END) AS total_population
            FROM (
                SELECT country, code, 2016 AS year_, y2016 AS value FROM wdi
                UNION ALL
                SELECT country, code, 2017 AS year_, y2017 AS value FROM wdi
                UNION ALL
                SELECT country, code, 2018 AS year_, y2018 AS value FROM wdi
                UNION ALL
                SELECT country, code, 2019 AS year_, y2019 AS value FROM wdi
                UNION ALL
                SELECT country, code, 2020 AS year_, y2020 AS value FROM wdi
                UNION ALL
                SELECT country, code, 2021 AS year_, y2021 AS value FROM wdi
            ) AS sub1
            GROUP BY country, year_
        ) AS sub2
        WHERE rural_population_percent IS NOT NULL
            AND fertility_rate IS NOT NULL
            AND gdp_per_capita_usd IS NOT NULL
            AND mobile_subscriptions_per_100 IS NOT NULL
            AND refugee_population IS NOT NULL
            AND total_population IS NOT NULL
        ORDER BY country, year_
    """)
    print("  Done.")
