from utils import bench, connect_to_hive, preview_table, setup_output_table


with connect_to_hive() as cursor:
    setup_output_table(
        cursor,
        "charts_yearly_stats",
        "region STRING, year_ INT, stream_avg DOUBLE, stream_dev DOUBLE",
    )

    with bench():
        cursor.execute("""
        INSERT INTO charts_yearly_stats
            SELECT
                region,
                YEAR(date_) AS year_,
                AVG(streams) AS stream_avg,
                STDDEV(streams) AS stream_dev
            FROM charts_daily_sum
            GROUP BY region, YEAR(date_)
            ORDER BY region, YEAR(date_)
        """)

    preview_table(cursor, "charts_yearly_stats")
