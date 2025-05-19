from utils import bench, connect_to_hive, preview_table, setup_output_table


with connect_to_hive() as cursor:
    setup_output_table(
        cursor,
        "daily_country_weather",
        "country STRING, date_ DATE, temperature_c DOUBLE, precipitation_mm DOUBLE",
    )

    with bench():
        cursor.execute("""
        INSERT INTO daily_country_weather
            SELECT
                c.country,
                w.date_,
                AVG(w.temperature_c) AS temperature_c,
                COALESCE(AVG(w.precipitation_mm), 0) AS precipitation_mm
            FROM (
                SELECT
                    station_id,
                    date_,
                    avg_temp_c AS temperature_c,
                    precipitation_mm
                FROM daily_weather_2017
                WHERE date_ BETWEEN '2017-01-01' AND '2021-12-31'
            ) AS w
            JOIN cities AS c
                ON w.station_id = c.station_id
            GROUP BY c.country, w.date_
            HAVING temperature_c IS NOT NULL
            ORDER BY c.country, w.date_
        """)

    preview_table(cursor, "daily_country_weather")
