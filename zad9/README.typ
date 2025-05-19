#set par(justify: true)

#align(center)[
  #text(size: 20pt, weight: "bold")[Przetwarzanie dużych zbiorów danych (PDZD)]

  Zespół B1 (geopolityka i muzyka): \
  *Tomasz Chojnacki (260365), Kamila Iwańska (253027), Jakub Zehner (260285)*
]

= Zadanie 9 -- HIVE

== Wybrane procesy

Poniżej, na obrazku przedstawiono *5 procesów*, które wybraliśmy do analizy.

*Legenda:*
- #text(fill: rgb("#B85450"))[*czerwony*] -- etapy zaimplementowane *tylko w obecnym etapie* (w HIVE, 4);
- #text(fill: rgb("#D79B00"))[*pomarańczowy*] -- etapy zaimplementowane *w obu etapach* (w Map-Reduce oraz w HIVE, 1);
- #text(fill: rgb("#82B366"))[*zielony*] -- etapy zaimplementowane poprzednio (w Map-Reduce, 2).

#image("./img/pdzd.drawio.png")

== Proces 1 -- `charts_yearly_stats`

Proces 1 jest odpowiedzialny za agregację danych dotyczących liczby odtworzeń utworów muzycznych danego dnia w statystki roczne. Przyjmuje na wejściu sumy liczb odtworzeń muzyki w~serwisie Spotify w danym kraju i dniu, a zwraca ich średnią i odchylenie standardowe w skali roku w danym kraju.

#grid(columns: (1fr, 1fr), [
*Wejścia:*
- `charts_daily_sum`
], [
*Wyjścia:*
- `charts_yearly_stats`
])

#pagebreak()

#align(center)[
```SQL
INSERT INTO charts_yearly_stats
    SELECT
        region,
        YEAR(date_) AS year_,
        AVG(streams) AS stream_avg,
        STDDEV(streams) AS stream_dev
    FROM charts_daily_sum
    GROUP BY region, YEAR(date_)
    ORDER BY region, YEAR(date_)
```
]

== Proces 2 -- `charts_daily_popularity`

Proces 2 ma za zadanie obliczenie dziennego współczynnika słuchalności muzyki w kraju. Przyjmuje dzienne i roczne statystyki słuchalności i sprawdza na ile dla danego dnia i kraju liczba odtworzeń jest większa lub mniejsza od średniej rocznej. Wartość zwracana jest tekstem `VERY LOW`, `LOW`, `AVERAGE`, `HIGH` lub `VERY HIGH`.

#grid(columns: (1fr, 1fr), [
*Wejścia:*
- `charts_daily_sum`
- `charts_yearly_stats`
], [
*Wyjścia:*
- `charts_daily_popularity`
])

#align(center)[
```SQL
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
```
]

== Proces 3 -- `daily_country_weather`

Proces 3 jest odpowiedzialny za złączenie danych pogodowych dla poszczególnych stacji meteorologicznych z danymi dotyczącymi tych stacji (w szczególności rzutowanie stacji do krajów). Celem procesu jest zebranie cząstkowych danych z poszczególnych stacji i obliczenie średnich współczynników dla pogody (temperatura, opady) w danym kraju danego dnia. Wykluczane są stacje, które nie odnotowały żadnych pomiarów temperatury.

#grid(columns: (1fr, 1fr), [
*Wejścia:*
- `daily_weather_2017`
- `cities`
], [
*Wyjścia:*
- `daily_country_weather`
])

#pagebreak()

#align(center)[
```SQL
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
```
]

== Proces 4 -- `wdi_normalized`

Proces 4 ma za zadanie dokonania jednocześnie pivotu oraz unpivotu (częściowa transpozycja) danych z World Data Indicators. Gromadzone są wiersze zawierające lata i wskaźniki, które nas interesują w~późniejszych etapach. Następnie państwa oraz lata umieszczane są w kolejnych wierszach, a~wskaźniki w kolumnach.

#grid(columns: (1fr, 1fr), [
*Wejścia:*
- `WDIData_2017`
], [
*Wyjścia:*
- `wdi_normalized`
])

#align(center)[
#set text(size: 9.5pt)
```SQL
    WITH wdi AS (
        SELECT
            country_name as country,
            indicator_code as code,
            y2016, y2017, y2018, y2019, y2020, y2021
        FROM WDIData_2017
        WHERE indicator_code IN ('SP.RUR.TOTL.ZS', 'SP.DYN.TFRT.IN', 'NY.GDP.PCAP.CD',
          'IT.CEL.SETS.P2', 'SM.POP.REFG', 'SP.POP.TOTL')
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
```
]

== Proces 5 -- `wdi_interpolated`

Finalnie, proces 5 dokonuje interpolacji liniowej danych znormalizowanych z procesu 4. Wartości wskaźników World Data Indicators są raportowane z częstotliwością roczną, a my potrzebujemy ich wartości dziennych. W związku z tym, interpolujemy liniowo dane pomiędzy rocznymi wartościami na podstawie tego jak duży odsetek dni roku już minął:

Liniowa funkcja interpolująca $L(x)$ w przedziale $[x_0, x_1]$ ma postać #footnote(link("https://pl.wikipedia.org/wiki/Interpolacja_liniowa")):
$$$
L(x) = y_0 + (y_1 - y_0)/(x_1-x_0)(x-x_0).
$$$

#grid(columns: (1fr, 1fr), [
*Wejścia:*
- `wdi_normalized`
], [
*Wyjścia:*
- `wdi_interpolated`
])

#align(center)[
```SQL
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
```
]

== Infrastruktura uruchomieniowa i pomiarowa

Do uruchamiania procesów oraz dokonania pomiarów na cele eksperymentów zaimplementowano platformę uruchomieniową dla zadań SQL HIVE w języku Python, korzystając z biblioteki `PyHive`#footnote(link("https://pypi.org/project/PyHive")) oraz `hdfs`#footnote(link("https://pypi.org/project/hdfs")). Przykładowy kod uruchamiający proces wygląda następująco:

```Python
with connect_to_hive() as cursor:
    setup_output_table(
        cursor,
        "charts_daily_popularity",
        "region STRING, date_ DATE, popularity STRING",
    )

    with bench():
        cursor.execute("""
          -- KOD PROCESU
        """)

    preview_table(cursor, "charts_daily_popularity")
```

Funkcja `connect_to_hive` łączy się z klastrem HIVE oraz tworzy kursor, dbając jednocześnie o zamknięcie połączeń po zakończeniu działania. Funkcja `setup_output_table` tworzy tabelę wyjściową, o ile jest to konieczne, na podstawie podanego schematu. Funkcja `bench` mierzy czas wykonania zadania oraz wypisuje jego status z klastra HIVE. Funkcja `preview_table` wypisuje pierwszy wiersz tabeli wyjściowej, aby dodatkowo zweryfikować poprawność działania procesu.

Przykładowy dziennik wykonania procesu wygląda następująco:
#[
#set text(size: 7.5pt)
```
Connecting to Hive...
INFO:pyhive.hive:USE `default`
Connection established.
Setting up table charts_yearly_stats...
  Dropping table...
INFO:pyhive.hive:DROP TABLE IF EXISTS charts_yearly_stats
  Creating table...
INFO:pyhive.hive:CREATE TABLE charts_yearly_stats (region STRING, year_ INT, stream_avg DOUBLE, stream_dev DOUBLE)
  Done.
Executing query...
INFO:pyhive.hive:
        INSERT INTO charts_yearly_stats
            SELECT
                region,
                YEAR(date_) as year_,
                AVG(streams) as stream_avg,
                STDDEV(streams) as stream_dev
            FROM charts_daily_sum
            GROUP BY region, YEAR(date_)
            ORDER BY region, YEAR(date_)

  Execution time: 55.435 seconds
  application_1747668091494_0008        INSERT INTO charts_yearly_stat...YEAR(date_) (Stage-2)             MAPREDUCE          root         default                FINISHED           SUCCEEDED                 100% http://slave1:19888/jobhistory/job/job_1747668091494_0008
  Done.
INFO:pyhive.hive:SELECT * FROM charts_yearly_stats LIMIT 1
('Argentina', 2017, 7424229.113259668, 1032612.4806788638)
```
]

Dane ładowane są do systemu HIVE z systemu plików HDFS osobnym skryptem, który tworzy wymagane tabele oraz ładuje dane do systemu z odpowiednich plików CSV. Przykładowe wywołanie funkcji odpowiedzialnej za to wygląda następująco:
```Python
setup_table("charts_daily_sum", "region STRING, date_ DATE, streams BIGINT")
```

Przykładowy dziennik ładowania danych wygląda następująco:
#[
#set text(size: 7pt)
```
Connecting to Hive...
Connection established.
Setting up table charts_daily_sum_small...
  Creating table...
  Copying input data...
  Loading data into table...
  Done.
Setting up table charts_daily_sum...
  Creating table...
  Copying input data...
  Loading data into table...
  Done.
Setting up table daily_weather_small...
  Creating table...
  Copying input data...
  Removing header...
  Loading data into table...
  Done.
Setting up table daily_weather_2017...
  Creating table...
  Copying input data...
  Removing header...
  Loading data into table...
  Done.
Setting up table cities...
  Creating table...
  Copying input data...
  Removing header...
  Loading data into table...
  Done.
Setting up table cities_small...
  Creating table...
  Copying input data...
  Removing header...
  Loading data into table...
  Done.
Setting up table WDIData_2017...
  Creating table...
  Copying input data...
  Removing header...
  Loading data into table...
  Done.
```
]

== Benchmark



#pagebreak()
#set page(flipped: true)

== Przykłady

=== Proces 1 -- `charts_yearly_stats`

==== Wejście -- `charts_daily_sum_small`

==== Wyjście -- `charts_yearly_stats_small`

=== Proces 2 -- `charts_daily_popularity`

==== Wejście -- `charts_daily_sum`

==== Wejście -- `charts_yearly_stats`

==== Wyjście -- `charts_daily_popularity`

=== Proces 3 -- `daily_country_weather`

==== Wejście -- `daily_weather_small`

#[
#set text(size: 8pt)
```
station_id,date,avg_temp_c,precipitation_mm
87582,2017-01-01 00:00:00,30.3,0.0
87582,2017-01-02 00:00:00,25.6,
87582,2017-01-03 00:00:00,26.2,
87582,2017-01-04 00:00:00,26.2,
87166,2017-01-01 00:00:00,29.6,
87166,2017-01-02 00:00:00,31.1,
87166,2017-01-03 00:00:00,31.3,
87166,2017-01-04 00:00:00,29.2,
87344,2017-01-01 00:00:00,24.2,
87344,2017-01-02 00:00:00,23.3,0.3
87344,2017-01-03 00:00:00,28.0,0.0
87344,2017-01-04 00:00:00,21.9,18.0
12566,2017-01-01 00:00:00,-3.7,0.0
12566,2017-01-02 00:00:00,-2.4,2.0
12566,2017-01-03 00:00:00,-1.3,1.0
12566,2017-01-04 00:00:00,0.9,2.0
12375,2017-01-01 00:00:00,1.7,0.0
12375,2017-01-02 00:00:00,0.9,0.0
12375,2017-01-03 00:00:00,-0.7,0.8
12375,2017-01-04 00:00:00,1.3,7.1
12424,2017-01-01 00:00:00,-1.1,0.0
12424,2017-01-02 00:00:00,-0.7,0.5
12424,2017-01-03 00:00:00,1.0,0.5
12424,2017-01-04 00:00:00,2.1,1.5
```
]

==== Wejście -- `cities_small`

#[
#set text(size: 8pt)
```
station_id,city_name,country,state,iso2,iso3,latitude,longitude
87582,Buenos Aires,Argentina,Ciudad de Buenos Aires,AR,ARG,-34.6025016085,-58.3975313737
87166,Corrientes,Argentina,Corrientes,AR,ARG,-27.4899641736,-58.8099868181
87344,Córdoba,Argentina,Córdoba,AR,ARG,-31.3999580701,-64.1822945573
12566,Kraków,Poland,Lesser Poland,PL,POL,50.0599792675,19.9600113512
12375,Warsaw,Poland,Masovian,PL,POL,52.2500006298,20.9999995511
12424,Wrocław,Poland,Lower Silesian,PL,POL,51.1104319449,17.0300093167
```
]

==== Wyjście -- `daily_country_weather_small`

#[
#set text(size: 8pt)
```
Argentina,2017-01-01,28.03,0.00
Argentina,2017-01-02,26.67,0.30
Argentina,2017-01-03,28.50,0.00
Argentina,2017-01-04,25.77,18.00
Poland,2017-01-01,-1.03,0.00
Poland,2017-01-02,-0.73,0.83
Poland,2017-01-03,-0.33,0.77
Poland,2017-01-04,1.43,3.53
```
]


=== Proces 4 -- `wdi_normalized`

==== Wejście -- `WDIData_2017`

==== Wyjście -- `wdi_normalized`

=== Proces 5 -- `wdi_interpolated`

==== Wejście -- `wdi_normalized`

==== Wyjście -- `wdi_interpolated`
