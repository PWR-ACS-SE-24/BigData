daily_weather = LOAD '/input/daily_weather_2017.csv' USING PigStorage(',') 
    AS (station_id:chararray, date:chararray, avg_temp_c:double, precipitation_mm:double);
cities = LOAD '/input/cities.csv' USING PigStorage(',')
    AS (station_id:chararray, city_name:chararray, country:chararray, state:chararray, iso2::chararray, iso3::chararray, latitude::double, longitude::double);
filtered_weather = FILTER daily_weather BY (date >= '2017-01-01') AND (date <= '2021-12-31');
joined = JOIN filtered_weather BY station_id, cities BY station_id;
grouped = GROUP joined BY (cities::country, filtered_weather::date);
aggregated = FOREACH grouped GENERATE
    group.country AS country,
    group.date AS date,
    AVG(joined.filtered_weather::avg_temp_c) AS avg_temp_c,
    AVG(joined.filtered_weather::precipitation_mm) AS avg_precipitation_mm;
coalesced = FOREACH aggregated GENERATE
    country,
    date,
    avg_temp_c,
    ( (avg_precipitation_mm IS NULL) ? 0.0 : avg_precipitation_mm ) AS precipitation_mm;
final = FILTER coalesced BY avg_temp_c IS NOT NULL;
ordered = ORDER final BY country, date;
DUMP ordered;
