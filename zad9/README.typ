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

Proces 4 ma za zadanie dokonania jednocześnie pivotu oraz unpivotu (częściowa transpozycja) danych z World Data Indicators. Gromadzone są wiersze zawierające lata i wskaźniki, które nas interesują w~późniejszych etapach. Następnie państwa oraz lata umieszczane są w kolejnych wierszach, a~wskaźniki w kolumnach. Ponadto łączone są dane dotyczące liczby uchodźców z danymi o populacji, aby uzyskać wskaźnik liczby uchodźców na 1000 mieszkańców danego kraju.

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

Wszystkie pomiary wykonano trzykrotnie i uśredniono. Wszystkie czasy są podane w sekundach.

=== Czasy wykonania poszczególnych procesów

Wykorzystujemy podział pliku o~rozmiarze 256 MB oraz 1 reducer.

#align(center, table(
    columns: 2,
    align: horizon + right,
    table.header([*Proces*], [*Czas wykonania [s]*]),
    [`charts_yearly_stats`], [35.961],
    [`charts_daily_popularity`], [16.424],
    [`daily_country_weather`], [41.711],
    [`wdi_normalized`], [35.547],
    [`wdi_interpolated`], [25.698]
))

#pagebreak()

=== Eksperyment -- porównanie HIVE z Map-Reduce na podstawie `daily_country_weather`

Porównano czas wykonania dla podejścia Map-Reduce oraz HIVE dla procesu `daily_country_weather`, który był zaimplementowany w obu etapach.

Wykorzystujemy podział pliku o rozmiarze 256 MB oraz 1 reducer dla obu podejść.

#align(center, table(
    columns: 2,
    align: horizon + right,
    table.header([*Narzędzie*], [*Czas wykonania [s]*]),
    [*Map-Reduce*], [39.923],
    [*HIVE*], [41.711]
))

=== Eksperyment -- wpływ liczby reducerów na czas wykonania

Zbadano wpływ liczby reducerów na czas wykonywania wszystkich procesów.

Wykorzystujemy podział pliku o rozmiarze 256 MB.

#align(center, table(
    columns: 4,
    align: horizon + right,
    table.header(table.cell(rowspan: 2)[*Proces*], table.cell(colspan: 3)[*Reducery*], [*1*], [*2*], [*3*]),
    [`charts_yearly_stats`], [35.961], [35.565], [36.802],
    [`charts_daily_popularity`], [16.424], [18.076], [16.428],
    [`daily_country_weather`], [41.711], [41.937], [43.301],
    [`wdi_normalized`], [35.547], [36.705], [37.480],
    [`wdi_interpolated`], [25.698], [27.167], [25.624],
    [*Suma*], [155.341], [159.450], [159.635]
))

Uruchomiliśmy również proces z 10 reducerami, co zakończyło się awarią całego systemu Hadoop.

=== Eksperyment -- wpływ rozmiaru podziału pliku na czas wykonania

Zbadano wpływ rozmiaru podziału pliku na czas wykonywania wszystkich procesów.

Wykorzystujemy 1 reducer.

#align(center, table(
    columns: 6,
    align: horizon + right,
    table.header(table.cell(rowspan: 2)[*Proces*], table.cell(colspan: 5)[*Rozmiar podziału pliku*], [*16 MB*], [*32 MB*], [*64 MB*], [*128 MB*], [*256 MB*]),
    [`charts_yearly_stats`], [34.745], [36.085], [33.570], [35.630], [35.961],
    [`charts_daily_popularity`], [15.443], [16.799], [17.641], [17.257], [16.424],
    [`daily_country_weather`], [42.971], [40.276], [40.218], [40.556], [41.711],
    [`wdi_normalized`], [34.134], [36.457], [34.903], [35.077], [35.547],
    [`wdi_interpolated`], [27.099], [25.714], [26.152], [26.676], [25.698],
    [*Suma*], [154.392], [155.331], [152.484], [155.196], [155.341]
))

#pagebreak()
#set page(flipped: true)

== Przykłady

=== Proces 1 -- `charts_yearly_stats`

==== Wejście -- `charts_daily_sum_small`

#[
#set text(size: 7pt)
```
+------------+-------------+----------+
|   region   |    date_    | streams  |
+------------+-------------+----------+
| Argentina  | 2017-01-01  | 1696685  |
| Poland     | 2017-01-01  | 215444   |
+------------+-------------+----------+
```
]

==== Wyjście -- `charts_yearly_stats_small`

#[
#set text(size: 7pt)
```
+------------+--------+-------------+-------------+
|   region   | year_  | stream_avg  | stream_dev  |
+------------+--------+-------------+-------------+
| Argentina  | 2017   | 1696685.0   | 0.0         |
| Poland     | 2017   | 215444.0    | 0.0         |
+------------+--------+-------------+-------------+
```
]

=== Proces 2 -- `charts_daily_popularity`

==== Wejście -- `charts_daily_sum`

#[
#set text(size: 7pt)
```
+------------+-------------+----------+
|   region   |    date_    | streams  |
+------------+-------------+----------+
| Argentina  | 2017-01-01  | 1696685  |
| Poland     | 2017-01-01  | 215444   |
+------------+-------------+----------+
```
]

==== Wejście -- `charts_yearly_stats`

#[
#set text(size: 7pt)
```
+------------+--------+-------------+-------------+
|   region   | year_  | stream_avg  | stream_dev  |
+------------+--------+-------------+-------------+
| Argentina  | 2017   | 1696685.0   | 0.0         |
| Poland     | 2017   | 215444.0    | 0.0         |
+------------+--------+-------------+-------------+
```
]

==== Wyjście -- `charts_daily_popularity`

#[
#set text(size: 7pt)
```
+------------+-------------+-------------+
|   region   |    date_    | popularity  |
+------------+-------------+-------------+
| Argentina  | 2017-01-01  | AVERAGE     |
| Poland     | 2017-01-01  | AVERAGE     |
+------------+-------------+-------------+
```
]

#pagebreak()

=== Proces 3 -- `daily_country_weather`

==== Wejście -- `daily_weather_small`

#[
#set text(size: 6.25pt)
```
+-------------+-------------+-------------+-------------------+
| station_id  |    date_    | avg_temp_c  | precipitation_mm  |
+-------------+-------------+-------------+-------------------+
| 87582       | 2017-01-01  | 30.3        | 0.0               |
| 87582       | 2017-01-02  | 25.6        | NULL              |
| 87582       | 2017-01-03  | 26.2        | NULL              |
| 87582       | 2017-01-04  | 26.2        | NULL              |
| 87166       | 2017-01-01  | 29.6        | NULL              |
| 87166       | 2017-01-02  | 31.1        | NULL              |
| 87166       | 2017-01-03  | 31.3        | NULL              |
| 87166       | 2017-01-04  | 29.2        | NULL              |
| 87344       | 2017-01-01  | 24.2        | NULL              |
| 87344       | 2017-01-02  | 23.3        | 0.3               |
| 87344       | 2017-01-03  | 28.0        | 0.0               |
| 87344       | 2017-01-04  | 21.9        | 18.0              |
| 12566       | 2017-01-01  | -3.7        | 0.0               |
| 12566       | 2017-01-02  | -2.4        | 2.0               |
| 12566       | 2017-01-03  | -1.3        | 1.0               |
| 12566       | 2017-01-04  | 0.9         | 2.0               |
| 12375       | 2017-01-01  | 1.7         | 0.0               |
| 12375       | 2017-01-02  | 0.9         | 0.0               |
| 12375       | 2017-01-03  | -0.7        | 0.8               |
| 12375       | 2017-01-04  | 1.3         | 7.1               |
| 12424       | 2017-01-01  | -1.1        | 0.0               |
| 12424       | 2017-01-02  | -0.7        | 0.5               |
| 12424       | 2017-01-03  | 1.0         | 0.5               |
| 12424       | 2017-01-04  | 2.1         | 1.5               |
+-------------+-------------+-------------+-------------------+
```
]

==== Wejście -- `cities_small`

#[
#set text(size: 6.25pt)
```
+-------------+---------------+------------+-------------------------+-------+-------+-----------------+-----------------+
| station_id  |   city_name   |  country   |          state          | iso2  | iso3  |       lat       |       lon       |
+-------------+---------------+------------+-------------------------+-------+-------+-----------------+-----------------+
| 87582       | Buenos Aires  | Argentina  | Ciudad de Buenos Aires  | AR    | ARG   | -34.6025016085  | -58.3975313737  |
| 87166       | Corrientes    | Argentina  | Corrientes              | AR    | ARG   | -27.4899641736  | -58.8099868181  |
| 87344       | Cordoba       | Argentina  | Cordoba                 | AR    | ARG   | -31.3999580701  | -64.1822945573  |
| 12566       | Kraków        | Poland     | Lesser Poland           | PL    | POL   | 50.0599792675   | 19.9600113512   |
| 12375       | Warsaw        | Poland     | Masovian                | PL    | POL   | 52.2500006298   | 20.9999995511   |
| 12424       | Wrocław       | Poland     | Lower Silesian          | PL    | POL   | 51.1104319449   | 17.0300093167   |
+-------------+---------------+------------+-------------------------+-------+-------+-----------------+-----------------+
```
]

==== Wyjście -- `daily_country_weather_small`

#[
#set text(size: 6.25pt)
```
+------------+-------------+----------------------+---------------------+
|  country   |    date_    |    temperature_c     |  precipitation_mm   |
+------------+-------------+----------------------+---------------------+
| Argentina  | 2017-01-01  | 28.033333333333335   | 0.0                 |
| Argentina  | 2017-01-02  | 26.666666666666668   | 0.3                 |
| Argentina  | 2017-01-03  | 28.5                 | 0.0                 |
| Argentina  | 2017-01-04  | 25.766666666666666   | 18.0                |
| Poland     | 2017-01-01  | -1.0333333333333334  | 0.0                 |
| Poland     | 2017-01-02  | -0.7333333333333334  | 0.8333333333333334  |
| Poland     | 2017-01-03  | -0.3333333333333333  | 0.7666666666666666  |
| Poland     | 2017-01-04  | 1.4333333333333336   | 3.533333333333333   |
+------------+-------------+----------------------+---------------------+
```
]

#pagebreak()

=== Proces 4 -- `wdi_normalized`

==== Wejście -- `WDIData_small`

#[
#set text(size: 8pt)
```
+---------------+-----------------+-------------------+-------------------+-------------------+-------------------+-------------------+-------------------+--------+
| country_name  | indicator_code  |       y2016       |       y2017       |       y2018       |       y2019       |       y2020       |       y2021       | y2022  |
+---------------+-----------------+-------------------+-------------------+-------------------+-------------------+-------------------+-------------------+--------+
| Argentina     | SP.DYN.TFRT.IN  | 2.241             | 2.168             | 2.039             | 1.994             | 1.911             | 1.885             | NULL   |
| Argentina     | NY.GDP.PCAP.CD  | 12790.2424732447  | 14613.041824658   | 11795.1593866287  | 9963.6725062053   | 8496.42414176374  | 10636.1201956183  | NULL   |
| Argentina     | IT.CEL.SETS.P2  | 145.9179567       | 140.5014607       | 131.9371685       | 125.9409814       | 121.6001889       | 130.4550081       | NULL   |
| Argentina     | SP.POP.TOTL     | 4.3590368E7       | 4.4044811E7       | 4.4494502E7       | 4.4938712E7       | 4.5376763E7       | 4.5808747E7       | NULL   |
| Argentina     | SM.POP.REFG     | 3267.0            | 3332.0            | 3442.0            | 3857.0            | 3965.0            | 4050.0            | NULL   |
| Argentina     | SP.RUR.TOTL.ZS  | 8.373             | 8.251             | 8.13              | 8.009             | 7.889             | 7.771             | NULL   |
| Poland        | SP.DYN.TFRT.IN  | 1.39              | 1.48              | 1.46              | 1.44              | 1.39              | 1.33              | NULL   |
| Poland        | NY.GDP.PCAP.CD  | 12378.75943742    | 13815.6217986247  | 15504.5804848188  | 15699.9113500703  | 15816.989398397   | 17999.9099495446  | NULL   |
| Poland        | IT.CEL.SETS.P2  | 137.5522178       | 130.9503547       | 125.3471416       | 125.7168536       | 128.4226449       | 132.058828        | NULL   |
| Poland        | SP.POP.TOTL     | 3.7970087E7       | 3.7974826E7       | 3.797475E7        | 3.7965475E7       | 3.789907E7        | 3.7747124E7       | NULL   |
| Poland        | SM.POP.REFG     | 11738.0           | 12225.0           | 12495.0           | 12658.0           | 2771.0            | 4875.0            | NULL   |
| Poland        | SP.RUR.TOTL.ZS  | 39.822            | 39.895            | 39.942            | 39.963            | 39.957            | 39.925            | NULL   |
+---------------+-----------------+-------------------+-------------------+-------------------+-------------------+-------------------+-------------------+--------+
```
]

==== Wyjście -- `wdi_normalized_small`

#[
#set text(size: 8pt)
```
+------------+--------+---------------------------+-----------------+---------------------+-------------------------------+------------------------------+
|  country   | year_  | rural_population_percent  | fertility_rate  | gdp_per_capita_usd  | mobile_subscriptions_per_100  | refugee_population_promille  |
+------------+--------+---------------------------+-----------------+---------------------+-------------------------------+------------------------------+
| Argentina  | 2016   | 8.373                     | 2.241           | 12790.2424732447    | 145.9179567                   | 0.07494774992493755          |
| Argentina  | 2017   | 8.251                     | 2.168           | 14613.041824658     | 140.5014607                   | 0.07565022812789457          |
| Argentina  | 2018   | 8.13                      | 2.039           | 11795.1593866287    | 131.9371685                   | 0.07735787221531325          |
| Argentina  | 2019   | 8.009                     | 1.994           | 9963.6725062053     | 125.9409814                   | 0.08582800503939676          |
| Argentina  | 2020   | 7.889                     | 1.911           | 8496.42414176374    | 121.6001889                   | 0.08737952506660733          |
| Argentina  | 2021   | 7.771                     | 1.885           | 10636.1201956183    | 130.4550081                   | 0.08841106262958906          |
| Poland     | 2016   | 39.822                    | 1.39            | 12378.75943742      | 137.5522178                   | 0.3091380854618532           |
| Poland     | 2017   | 39.895                    | 1.48            | 13815.6217986247    | 130.9503547                   | 0.32192379235654695          |
| Poland     | 2018   | 39.942                    | 1.46            | 15504.5804848188    | 125.3471416                   | 0.32903442418975765          |
| Poland     | 2019   | 39.963                    | 1.44            | 15699.9113500703    | 125.7168536                   | 0.3334081820390763           |
| Poland     | 2020   | 39.957                    | 1.39            | 15816.989398397     | 128.4226449                   | 0.0731152505853046           |
| Poland     | 2021   | 39.925                    | 1.33            | 17999.9099495446    | 132.058828                    | 0.12914891211314536          |
+------------+--------+---------------------------+-----------------+---------------------+-------------------------------+------------------------------+
```
]

#pagebreak()

=== Proces 5 -- `wdi_interpolated`

==== Wejście -- `wdi_normalized_small`

#[
#set text(size: 8pt)
```
+------------+--------+---------------------------+-----------------+---------------------+-------------------------------+------------------------------+
|  country   | year_  | rural_population_percent  | fertility_rate  | gdp_per_capita_usd  | mobile_subscriptions_per_100  | refugee_population_promille  |
+------------+--------+---------------------------+-----------------+---------------------+-------------------------------+------------------------------+
| Argentina  | 2016   | 8.373                     | 2.241           | 12790.2424732447    | 145.9179567                   | 0.07494774992493755          |
| Argentina  | 2017   | 8.251                     | 2.168           | 14613.041824658     | 140.5014607                   | 0.07565022812789457          |
| Argentina  | 2018   | 8.13                      | 2.039           | 11795.1593866287    | 131.9371685                   | 0.07735787221531325          |
| Argentina  | 2019   | 8.009                     | 1.994           | 9963.6725062053     | 125.9409814                   | 0.08582800503939676          |
| Argentina  | 2020   | 7.889                     | 1.911           | 8496.42414176374    | 121.6001889                   | 0.08737952506660733          |
| Argentina  | 2021   | 7.771                     | 1.885           | 10636.1201956183    | 130.4550081                   | 0.08841106262958906          |
| Poland     | 2016   | 39.822                    | 1.39            | 12378.75943742      | 137.5522178                   | 0.3091380854618532           |
| Poland     | 2017   | 39.895                    | 1.48            | 13815.6217986247    | 130.9503547                   | 0.32192379235654695          |
| Poland     | 2018   | 39.942                    | 1.46            | 15504.5804848188    | 125.3471416                   | 0.32903442418975765          |
| Poland     | 2019   | 39.963                    | 1.44            | 15699.9113500703    | 125.7168536                   | 0.3334081820390763           |
| Poland     | 2020   | 39.957                    | 1.39            | 15816.989398397     | 128.4226449                   | 0.0731152505853046           |
| Poland     | 2021   | 39.925                    | 1.33            | 17999.9099495446    | 132.058828                    | 0.12914891211314536          |
+------------+--------+---------------------------+-----------------+---------------------+-------------------------------+------------------------------+
```
]

==== Wyjście -- `wdi_interpolated_small`

*UWAGA:* Z charakterystyki procesu 5 wynika, że nawet przy bardzo małej ilości danych wejściowych, proces generuje dużo danych wyjściowych. W związku z tym, do weryfikacji poprawności działania procesu, wykorzystujemy tylko podzbiór wyniku uzyskany za pomocą:
```SQL
SELECT * FROM wdi_interpolated_small ORDER BY date_ LIMIT 10;
```

#[
#set text(size: 8pt)
```
+------------+-------------+---------------------------+---------------------+---------------------+-------------------------------+------------------------------+
|  country   |    date_    | rural_population_percent  |   fertility_rate    | gdp_per_capita_usd  | mobile_subscriptions_per_100  | refugee_population_promille  |
+------------+-------------+---------------------------+---------------------+---------------------+-------------------------------+------------------------------+
| Poland     | 2017-01-01  | 39.8222                   | 1.3902465753424658  | 12382.69604662878   | 137.5341305038356             | 0.3091731147958112           |
| Argentina  | 2017-01-01  | 8.372665753424657         | 2.2408              | 12795.23644407049   | 145.9031169849315             | 0.07494967452275388          |
| Poland     | 2017-01-02  | 39.82240000000001         | 1.3904931506849314  | 12386.63265583756   | 137.51604320767123            | 0.3092081441297693           |
| Argentina  | 2017-01-02  | 8.372331506849314         | 2.2406              | 12800.23041489628   | 145.888277269863              | 0.07495159912057019          |
| Argentina  | 2017-01-03  | 8.371997260273972         | 2.2404              | 12805.22438572207   | 145.87343755479452            | 0.07495352371838652          |
| Poland     | 2017-01-03  | 39.82260000000001         | 1.3907397260273973  | 12390.56926504634   | 137.49795591150684            | 0.30924317346372737          |
| Poland     | 2017-01-04  | 39.8228                   | 1.3909863013698631  | 12394.50587425512   | 137.47986861534247            | 0.3092782027976854           |
| Argentina  | 2017-01-04  | 8.37166301369863          | 2.2402              | 12810.21835654786   | 145.85859783972603            | 0.07495544831620284          |
| Argentina  | 2017-01-05  | 8.371328767123286         | 2.24                | 12815.212327373649  | 145.84375812465754            | 0.07495737291401915          |
| Poland     | 2017-01-05  | 39.823                    | 1.3912328767123285  | 12398.4424834639    | 137.46178131917807            | 0.3093132321316435           |
+------------+-------------+---------------------------+---------------------+---------------------+-------------------------------+------------------------------+
```
]
