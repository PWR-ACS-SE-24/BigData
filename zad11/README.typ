#set par(justify: true)

#align(center)[
  #text(size: 20pt, weight: "bold")[Przetwarzanie dużych zbiorów danych (PDZD)]

  Zespół B1 (geopolityka i muzyka): \
  *Tomasz Chojnacki (260365), Kamila Iwańska (253027), Jakub Zehner (260285)*
]

= Zadanie 11 -- Spark

== Wybrane procesy

Poniżej, na obrazku przedstawiono *6 procesów*, które wybraliśmy do analizy.

*Legenda:*
- #text(fill: rgb("#B85450"))[*czerwony*] -- etapy wykonany *tylko w obecnym zadaniu* (w Spark, 5);
- #text(fill: rgb("#D79B00"))[*pomarańczowy*] -- etapy wykonane *we wszystkich zadaniach* (w Map-Reduce/HIVE/Spark, 1);
- #text(fill: rgb("#82B366"))[*zielony*] -- etapy wykonane poprzednio (w Map-Reduce/HIVE, 6).

#image("./img/pdzd.drawio.png")

#pagebreak()

== Kod pomocniczy

Do implementacji etapów na Spark wykorzystaliśmy język Python i przygotowaliśmy następujące pomocnicze funkcje:

#[
#show raw: set text(fill: blue)
- `connect(**kwargs) -> SparkSession` -- połączenie się z klastrem Spark z wykorzystaniem odpowiednich parametrów `kwargs` (potrzebnych do eksperymentów);
- `load_table(spark: SparkSession, path: LiteralString, *, header: list[str] | None = None) -> DataFrame` -- wczytanie tabeli do DataFrame lub tymczasowego widoku (do SparkSQL) z~pliku CSV znajdującego się pod `path` w HDFS, wspierane jest wczytywanie plików z nagłówkiem oraz własnoręczne podanie nazw kolumn w `header` w przypadku braku nagłówka;
- `@contextmanager bench() -> Generator[None, None, None]` -- mierzenie czasu wykonania kodu znajdującego się w kontekście, zwraca czas w sekundach z dokładnością do 3 miejsc po przecinku;
- `process(spark: SparkSession, name: LiteralString, sql: LiteralString) -> None` -- przetworzenie danych w SparkSQL, za pomocą kwerendy `sql`, zapisanie ich do pliku oraz widoku tymczasowego o nazwie `name`, wyświetlenie podglądu oraz pomiar czasu wykonania przez `bench`;
- `example(spark: SparkSession, name: LiteralString) -> None` -- wyświetlenie pełnej postaci skróconej tabeli `name` do wykorzystania w sekcji "Przykłady" na końcu raportu.
]

== Proces 1 -- `charts_artists`

Pierwszy proces jest bardzo prosty i łączy dane utworów z list przebojów wraz z danymi o artystach, którzy są jego wykonawcami. Obliczane jest również unikalne ID artysty, które jest skrótem MD5 jego nazwy. Nazwy wykonawców nie powtarzają się w naszym zbiorze danych. O ile MD5 teoretycznie może spowodować kolizję, w naszym przypadku to ryzyko jest kompletnie pomijalne. Nie widzimy potrzeby wykorzystania bardziej skomplikowanych algorytmów.

*Wejścia:*
- `charts_fmt` -- wstępnie przetworzone dane z list przebojów;
- `api_track_to_artist` -- cache danych o artystach dynamicznie pobieranych z API Spotify.

*Wyjścia:*
- `charts_artists` -- tabela zawierająca dane o artystach, którzy wykonują utwory z list przebojów.

#align(center)[```sql
SELECT
    c.region,
    c.date,
    MD5(a.artist) AS artist_id,
    c.streams
FROM charts_fmt AS c
JOIN api_track_to_artist AS a
    ON c.track_id = a.track_id
```]

== Proces 2 -- `charts_genres`

Proces numer dwa jest podobny do pierwszego, tym razem złączane są natomiast dane o gatunkach utworów, a nie ich artystach.

*Wejścia:*
- `charts_fmt` -- wstępnie przetworzone dane z list przebojów;
- `api_track_to_genre` -- cache danych o gatunkach dynamicznie pobieranych z API Spotify.

*Wyjścia:*
- `charts_genres` -- tabela zawierająca dane o gatunkach utworów z list przebojów.

#pagebreak()

#align(center)[```sql
SELECT
    c.region,
    c.date,
    t.genre,
    c.streams
FROM charts_fmt AS c
JOIN api_track_to_genre AS t
    ON c.track_id = t.track_id
```]

== Proces 3 -- `charts_daily_genres`

Trzeci proces jest bardziej zaawansowany, wykonuje pivot gatunków w wierszy do kolumn, produkując tabelę zawierającą dzień, region oraz po jednej kolumnie, zawierającej łączną liczbę odtworzeń, na analizowany gatunek.

*Wejścia:*
- `charts_genres` -- tabela zawierająca dane o gatunkach utworów z list przebojów.

*Wyjścia:*
- `charts_daily_genres` -- tabela zawierająca dane o liczbie odtworzeń utworów z list przebojów, pogrupowane po regionach i dniach, z gatunkami w kolumnach.

#align(center)[
#set text(size: 9.3pt)
```sql
SELECT
    region,
    date,
    COALESCE(MAX(CASE WHEN genre = 'pop' THEN total_streams END), 0) AS pop,
    COALESCE(MAX(CASE WHEN genre = 'rap' THEN total_streams END), 0) AS rap,
    COALESCE(MAX(CASE WHEN genre = 'rock' THEN total_streams END), 0) AS rock,
    COALESCE(MAX(CASE WHEN genre = 'edm' THEN total_streams END), 0) AS edm,
    COALESCE(MAX(CASE WHEN genre = 'hip hop' THEN total_streams END), 0) AS hip_hop,
    COALESCE(MAX(CASE WHEN genre = 'trap latino' THEN total_streams END), 0) AS trap_latino,
    COALESCE(MAX(CASE WHEN genre = 'reggaeton' THEN total_streams END), 0) AS reggaeton,
    COALESCE(MAX(CASE WHEN genre = 'electropop' THEN total_streams END), 0) AS electropop,
    COALESCE(MAX(CASE WHEN genre = 'dance pop' THEN total_streams END), 0) AS dance_pop,
    COALESCE(MAX(CASE WHEN genre = 'pop rap' THEN total_streams END), 0) AS pop_rap,
    COALESCE(MAX(CASE WHEN genre = 'musica mexicana' THEN total_streams END), 0) AS musica_mexicana,
    COALESCE(MAX(CASE WHEN genre = 'trap' THEN total_streams END), 0) AS trap,
    COALESCE(MAX(CASE WHEN genre = 'modern rock' THEN total_streams END), 0) AS modern_rock,
    COALESCE(MAX(CASE WHEN genre = 'classic rock' THEN total_streams END), 0) AS classic_rock,
    COALESCE(MAX(CASE WHEN genre = 'uk pop' THEN total_streams END), 0) AS uk_pop,
    COALESCE(MAX(CASE WHEN genre = 'k-pop' THEN total_streams END), 0) AS k_pop,
    COALESCE(MAX(CASE WHEN genre = 'tropical house' THEN total_streams END), 0) AS tropical_house,
    COALESCE(MAX(CASE WHEN genre = 'melodic rap' THEN total_streams END), 0) AS melodic_rap,
    COALESCE(MAX(CASE WHEN genre = 'canadian pop' THEN total_streams END), 0) AS canadian_pop,
    COALESCE(MAX(CASE WHEN genre = 'modern bollywood' THEN total_streams END), 0) AS modern_bollywood
FROM (
    SELECT
        region,
        date,
        genre,
        SUM(streams) AS total_streams
    FROM charts_genres
    WHERE genre IN ('pop', 'rap', 'rock', 'edm', 'hip hop', 'trap latino', 'reggaeton', 'electropop', 'dance pop', 'pop rap', 'musica mexicana', 'trap', 'modern rock', 'classic rock', 'uk pop', 'k-pop', 'tropical house', 'melodic rap', 'canadian pop', 'modern bollywood')
    GROUP BY region, date, genre
)
GROUP BY region, date
ORDER BY region, date
```]

#pagebreak()

== Proces 4 -- `charts_genre_popularity`

Proces czwarty normalizuje liczby wyświetleń utworów na popularności (z zakresu 0 do 1), dzieląc liczby odtworzeń dla poszczególnych gatunków przez łączną liczbę odtworzeń z tego dnia i tego regionu. Wartość popularności 0.25 oznacza więc, że dany gatunek stanowi 25% wszystkich odtworzeń utworów tego dnia w tym regionie. Wskaźniki popularności nie sumują się do 1.

*Wejścia:*
- `charts_daily_genres` -- tabela zawierająca dane o liczbie odtworzeń utworów z list przebojów, pogrupowane po regionach i dniach, z gatunkami w kolumnach;
- `charts_daily_sum` -- tabela zawierająca sumy odtworzeń wszystkich utworów z list przebojów, pogrupowane po regionach i dniach.

*Wyjścia:*
- `charts_genre_popularity` -- tabela zawierająca dane o popularności gatunków utworów z list przebojów, pogrupowane po regionach i dniach.

#align(center)[```sql
SELECT
    g.region,
    g.date,
    g.pop / s.streams AS pop,
    g.rap / s.streams AS rap,
    g.rock / s.streams AS rock,
    g.edm / s.streams AS edm,
    g.hip_hop / s.streams AS hip_hop,
    g.trap_latino / s.streams AS trap_latino,
    g.reggaeton / s.streams AS reggaeton,
    g.electropop / s.streams AS electropop,
    g.dance_pop / s.streams AS dance_pop,
    g.pop_rap / s.streams AS pop_rap,
    g.musica_mexicana / s.streams AS musica_mexicana,
    g.trap / s.streams AS trap,
    g.modern_rock / s.streams AS modern_rock,
    g.classic_rock / s.streams AS classic_rock,
    g.uk_pop / s.streams AS uk_pop,
    g.k_pop / s.streams AS k_pop,
    g.tropical_house / s.streams AS tropical_house,
    g.melodic_rap / s.streams AS melodic_rap,
    g.canadian_pop / s.streams AS canadian_pop,
    g.modern_bollywood / s.streams AS modern_bollywood
FROM charts_daily_genres AS g
JOIN charts_daily_sum AS s
    ON g.region = s.region AND g.date = s.date
```]

== Proces 5 -- `output`

Proces piąty jest ostatnim procesem naszego przetwarzania, który łączy częściowe wyniki z wielu procesów wejściowych (5 tabel), wykorzystując wspólne klucze oraz ujednolica nazwy kolumn i~sortuje wyniki. Otrzymane dane mogą być wykorzystane do dalszej analizy, np. wizualizacji lub wytrenowania modelu uczenia maszynowego, przewidującego popularność gatunku na podstawie pogody, rozwoju gospodarczego i innych czynników.

#pagebreak()

*Wejścia:*
- `charts_genre_popularity` -- tabela zawierająca dane o popularności gatunków utworów z list przebojów, pogrupowane po regionach i dniach;
- `charts_daily_popularity` -- tabela zawierająca dane o popularności utworów z list przebojów (w~formie tekstowej, np. `AVERAGE`, `HIGH`, itd.), pogrupowane po regionach i dniach;
- `charts_daily_sum` -- tabela zawierająca sumy odtworzeń wszystkich utworów z list przebojów, pogrupowane po regionach i dniach;
- `daily_country_weather` -- tabela zawierająca uśrednione dane o pogodzie (temperatura, opady, itd.) w poszczególnych krajach i dniach;
- `wdi_interpolated` -- tabela zawierająca interpolowane dane z World Development Indicators (WDI).

*Wyjścia:*
- `output` -- tabela zawierająca złączone dane wynikowe.

#align(center)[```sql
SELECT
    cgp.region AS country,
    cgp.date,
    cds.streams AS daily_streams,
    cdp.popularity AS daily_popularity,
    dcw.temperature_c AS temperature_c,
    dcw.precipitation_mm AS precipitation_mm,
    wdi.rural_population_percent AS `wdi:rural_population_percent`,
    wdi.fertility_rate AS `wdi:fertility_rate`,
    wdi.gdp_per_capita_usd AS `wdi:gdp_per_capita_usd`,
    wdi.mobile_subscriptions_per_100 AS `wdi:mobile_subscriptions_per_100`,
    wdi.refugee_population_promille AS `wdi:refugee_population_promille`,
    cgp.pop AS `genre:pop`,
    cgp.rap AS `genre:rap`,
    cgp.rock AS `genre:rock`,
    cgp.edm AS `genre:edm`,
    cgp.hip_hop AS `genre:hip_hop`,
    cgp.trap_latino AS `genre:trap_latino`,
    cgp.reggaeton AS `genre:reggaeton`,
    cgp.electropop AS `genre:electropop`,
    cgp.dance_pop AS `genre:dance_pop`,
    cgp.pop_rap AS `genre:pop_rap`,
    cgp.musica_mexicana AS `genre:musica_mexicana`,
    cgp.trap AS `genre:trap`,
    cgp.modern_rock AS `genre:modern_rock`,
    cgp.classic_rock AS `genre:classic_rock`,
    cgp.uk_pop AS `genre:uk_pop`,
    cgp.k_pop AS `genre:k_pop`,
    cgp.tropical_house AS `genre:tropical_house`,
    cgp.melodic_rap AS `genre:melodic_rap`,
    cgp.canadian_pop AS `genre:canadian_pop`,
    cgp.modern_bollywood AS `genre:modern_bollywood`
FROM charts_genre_popularity AS cgp
JOIN charts_daily_popularity AS cdp
    ON cgp.region = cdp.region AND cgp.date = cdp.date
JOIN charts_daily_sum AS cds
    ON cgp.region = cds.region AND cgp.date = cds.date
JOIN daily_country_weather AS dcw
    ON cgp.region = dcw.country AND cgp.date = dcw.date
JOIN wdi_interpolated AS wdi
    ON cgp.region = wdi.country AND cgp.date = wdi.date
ORDER BY cgp.region, cgp.date
```]

== Proces dodatkowy -- `daily_country_weather`

Dodatkowo, w celu wykonania eksperymentalnych porównań z poprzednio wykorzystywanymi zadaniami, przygotowano proces `daily_country_weather`, który wcześniej był zaimplementowany zarówno w Map-Reduce oraz HIVE. Aby pozwolić na dodatkowe porównania, zaimplementowano go za pomocą dwóch interfejsów programistycznych dostępnych w Sparku w Pythonie: DataFrames oraz SQL. Są one opisane poniżej:

*Wejścia:*
- `daily_weather` -- tabela zawierająca odczyty pogodowe (np. temperaturę, wiatr, opady) z~poszczególnych stacji pogodowych opisanych identyfikatorem;
- `cities` -- opisy przynależności stacji pogodowych do miast (a co za tym do krajów).

*Wyjścia:*
- `daily_country_weather` -- tabela zawierająca uśrednione dane o pogodzie (temperatura, opady, itd.) w poszczególnych krajach i dniach.

=== DataFrames

#align(center)[
#set text(size: 9.5pt)
```python
from pyspark.sql.functions import to_date, col, avg, coalesce, lit
result_df = (
    (
        (
            daily_weather_2017.filter(
                (col("date") >= "2017-01-01") & (col("date") <= "2021-12-31")
            ).select(
                col("station_id"),
                to_date(col("date")).alias("date"),
                col("avg_temp_c").alias("temperature_c"),
                col("precipitation_mm"),
            )
        )
        .join(cities, on="station_id")
        .groupBy("country", "date")
        .agg(
            avg("temperature_c").alias("temperature_c"),
            coalesce(avg("precipitation_mm"), lit(0)).alias("precipitation_mm"),
        )
    )
    .filter(col("temperature_c").isNotNull())
    .orderBy("country", "date")
)
```]

=== SQL

#align(center)[
#set text(size: 9.5pt)  
```sql
SELECT
    c.country,
    w.date,
    AVG(w.temperature_c) as temperature_c,
    COALESCE(AVG(w.precipitation_mm), 0) as precipitation_mm
FROM (
    SELECT
        station_id,
        to_date(date) as date,
        avg_temp_c as temperature_c,
        precipitation_mm
    FROM daily_weather_2017
    WHERE date BETWEEN '2017-01-01' AND '2021-12-31'
) AS w
JOIN cities AS c
    ON w.station_id = c.station_id
GROUP BY c.country, w.date
HAVING temperature_c IS NOT NULL
ORDER BY c.country, w.date
```]

== Eksperymenty w Spark

=== Eksperyment 1 -- porównanie SQL i DataFrames

Oba interfejsy programistyczne zostały wykorzystane jedynie w jednym etapie, więc to na nim bazujemy ich porównanie. Czasy wykonania zostały uśrednione z pięciu uruchomień, bez cache. Porównano wyniki wykonywania w obu środowiskach i były one w pełni zgodne.

#align(center, table(
  align: right + horizon,
  columns: 3,
  table.header(table.cell(rowspan: 2)[*Etap*], table.cell(colspan: 2)[*Czas wykonania [s]*], [*SQL*], [*DataFrames*]),
  [`daily_country_weather`], [], []
))

=== Eksperyment 2 -- wpływ liczby dostępnych rdzeni (`cores.max`)

Porównano wpływ całkowitej liczby dostępnych rdzeni (`spark.cores.max`) na czas wykonywania poszczególnych etapów przetwarzania danych. Wyniki podane w sekundach i uśrednione przy pięciu uruchomieniach bez cache zostały zebrane w tabeli poniżej.

#align(center, table(
  align: right + horizon,
  columns: 5,
  table.header(table.cell(rowspan: 2)[*Etap*], table.cell(colspan: 4)[*Czas wykonania [s]*], [*1 rdzeń*], [*2 rdzenie*], [*4 rdzenie*], [*8 rdzeni*]),
  [`charts_artists`], [], [], [], [],
  [`charts_genres`], [], [], [], [],
  [`charts_daily_genres`], [], [], [], [],
  [`charts_genre_popularity`], [], [], [], [],
  [`output`], [], [], [], [],
  [`daily_country_weather`], [], [], [], [],
))

=== Eksperyment 3 -- wpływ partycjonowania (`sql.shuffle.partitions`)

Porównano wpływ liczby partycji do operacji agregacji, złączeń, itp. (`spark.sql.shuffle.partitions`) na czas wykonywania poszczególnych etapów przetwarzania danych. Wyniki podane w sekundach i uśrednione przy pięciu uruchomieniach bez cache zostały zebrane w tabeli poniżej.

#align(center, table(
  align: right + horizon,
  columns: 5,
  table.header(table.cell(rowspan: 2)[*Etap*], table.cell(colspan: 4)[*Czas wykonania [s]*], [*1 partycja*], [*3 partycje*], [*5 partycji*], [*7 partycji*]),
  [`charts_artists`], [], [], [], [],
  [`charts_genres`], [], [], [], [],
  [`charts_daily_genres`], [], [], [], [],
  [`charts_genre_popularity`], [], [], [], [],
  [`output`], [], [], [], [],
  [`daily_country_weather`], [], [], [], [],
))

#pagebreak()

=== Eksperyment 4 -- wpływ pamięci dostępnej egzekutorowi (`executor.memory`)

Porównano wpływ pamięci dostępnej pojedynczemu egzekutorowi (`spark.executor.memory`) na czas wykonywania poszczególnych etapów przetwarzania danych. Wyniki podane w sekundach i uśrednione przy pięciu uruchomieniach bez cache zostały zebrane w tabeli poniżej.

#align(center, table(
  align: right + horizon,
  columns: 4,
  table.header(table.cell(rowspan: 2)[*Etap*], table.cell(colspan: 3)[*Czas wykonania [s]*], [*½ GB*], [*1 GB*], [*2 GB*]),
  [`charts_artists`], [], [], [],
  [`charts_genres`], [], [], [],
  [`charts_daily_genres`], [], [], [],
  [`charts_genre_popularity`], [], [], [],
  [`output`], [], [], [],
  [`daily_country_weather`], [], [], [],
))

=== Eksperyment 5 -- wpływ limitu rozgłoszenia (`sql.autoBroadcastJoinThreshold`)

Porównano wpływ limitu rozgłoszenia (`spark.sql.autoBroadcastJoinThreshold`) na czas wykonywania poszczególnych etapów przetwarzania danych. Wyniki podane w sekundach i uśrednione przy pięciu uruchomieniach bez cache zostały zebrane w tabeli poniżej.

#align(center, table(
  align: right + horizon,
  columns: 5,
  table.header(table.cell(rowspan: 2)[*Etap*], table.cell(colspan: 4)[*Czas wykonania [s]*], [*5 MB*], [*10 MB*], [*15 MB*], [*20 MB*]),
  [`charts_artists`], [], [], [], [],
  [`charts_genres`], [], [], [], [],
  [`charts_daily_genres`], [], [], [], [],
  [`charts_genre_popularity`], [], [], [], [],
  [`output`], [], [], [], [],
  [`daily_country_weather`], [], [], [], [],
))

== Eksperymenty porównawcze

=== Eksperyment 6 -- Spark vs. Map-Reduce

=== Eksperyment 7 -- Spark vs. HIVE

=== Eksperyment 8 -- Spark vs. PIG

#pagebreak()
#set page(flipped: true, margin: 0.5cm)

== Przykłady

== Proces 1 -- `charts_artists`

#[
#grid(columns: (1fr, 1fr), [
=== Wejście -- `charts_fmt_small.csv`

```
+---------+----------+----------------------+-------+
|region   |date      |track_id              |streams|
+---------+----------+----------------------+-------+
|Argentina|2017-01-01|6mICuAdrwEjh6Y6lroV2Kg|253019 |
|Argentina|2017-01-01|7DM4BPaS7uofFul3ywMe46|223988 |
|Argentina|2017-01-01|3AEZUABDXNtecAOSC1qTfo|210943 |
|Argentina|2017-01-01|6rQSrBHf7HlZjtcMZ4S4bO|173865 |
|Argentina|2017-01-01|58IL315gMSTD37DOZPJ2hf|153956 |
|Poland   |2017-01-01|4pdPtRcBmOSQDlJ3Fk945m|26290  |
|Poland   |2017-01-01|5aAx2yezTd8zXrkmtKl66Z|25198  |
|Poland   |2017-01-01|7BKLCZ1jbUBVqRi2FVlTVw|24642  |
|Poland   |2017-01-01|7abpmGpF7PGep2rDU68GBR|24630  |
|Poland   |2017-01-01|5knuzwU65gJK7IF5yJsuaW|23163  |
+---------+----------+----------------------+-------+
```

=== Wejście -- `api_track_to_artist_small.csv`

```
+----------------------+-----------------+
|track_id              |artist           |
+----------------------+-----------------+
|5aAx2yezTd8zXrkmtKl66Z|The Weeknd       |
|5aAx2yezTd8zXrkmtKl66Z|Daft Punk        |
|7DM4BPaS7uofFul3ywMe46|Ricky Martin     |
|7DM4BPaS7uofFul3ywMe46|Maluma           |
|5knuzwU65gJK7IF5yJsuaW|Clean Bandit     |
|5knuzwU65gJK7IF5yJsuaW|Sean Paul        |
|5knuzwU65gJK7IF5yJsuaW|Anne-Marie       |
|7BKLCZ1jbUBVqRi2FVlTVw|The Chainsmokers |
|7BKLCZ1jbUBVqRi2FVlTVw|Halsey           |
|3AEZUABDXNtecAOSC1qTfo|CNCO             |
|6rQSrBHf7HlZjtcMZ4S4bO|J Balvin         |
|6rQSrBHf7HlZjtcMZ4S4bO|Pharrell Williams|
|6rQSrBHf7HlZjtcMZ4S4bO|BIA              |
|6rQSrBHf7HlZjtcMZ4S4bO|Sky Rompiendo    |
|7abpmGpF7PGep2rDU68GBR|Burak Yeter      |
|7abpmGpF7PGep2rDU68GBR|Danelle Sandoval |
|58IL315gMSTD37DOZPJ2hf|Daddy Yankee     |
|4pdPtRcBmOSQDlJ3Fk945m|DJ Snake         |
|4pdPtRcBmOSQDlJ3Fk945m|Justin Bieber    |
+----------------------+-----------------+
```
], [
=== Wyjście -- `charts_artists_small.csv`

```
+---------+----------+--------------------------------+-------+
|region   |date      |artist_id                       |streams|
+---------+----------+--------------------------------+-------+
|Argentina|2017-01-01|818ef62dce0d768b99a492012eacf18b|223988 |
|Argentina|2017-01-01|8c5b8ea2f957eef56e1c23ac25bd48e3|223988 |
|Argentina|2017-01-01|6c6e7227e205e6faa130531b2bb7351e|210943 |
|Argentina|2017-01-01|02b6698efb5bcf3dc2009c9a838b8b96|173865 |
|Argentina|2017-01-01|23a2193a36b72b1eba98b0bee269edde|173865 |
|Argentina|2017-01-01|dca432e1624d923de268b0ab655a1e48|173865 |
|Argentina|2017-01-01|82594863c7df01a9561a05de5ebe9d42|173865 |
|Argentina|2017-01-01|ed73022e38d78447588e214e0d9b6a3f|153956 |
|Poland   |2017-01-01|6175b9f6699984f1ede4bea3a5c30fe1|25198  |
|Poland   |2017-01-01|8ee2c0adee9548498ef22cba1e90a49c|25198  |
|Poland   |2017-01-01|309b74a8a61cadf954a140000b3c71f5|23163  |
|Poland   |2017-01-01|b9fbeb7e58125ff49f7ff735d839cab5|23163  |
|Poland   |2017-01-01|7cc13a578cbab14ed83e7b6b87f52aa3|23163  |
|Poland   |2017-01-01|3eb0c531dd779dd405f1228a58ee9993|24642  |
|Poland   |2017-01-01|16fd20c388393f6f0975940e1dd586b8|24642  |
|Poland   |2017-01-01|ba4c94bc384c116dd322198cdb119621|24630  |
|Poland   |2017-01-01|02b09bfbfa2f11d4ce1c992497835136|24630  |
|Poland   |2017-01-01|cc56ee73e45f0546f16d6a8de1bcff67|26290  |
|Poland   |2017-01-01|24b9298a187e48162094bf976c57f9f7|26290  |
+---------+----------+--------------------------------+-------+
```
])
]

#pagebreak()
#set page(flipped: false)
#[
#show raw.where(block: true): set text(size: 6.5pt)
== Proces 2 -- `charts_genres`

#text(fill: red)[Z uwagi na naturę procesu 2, aby nie stracić żadnych danych przy złączeniu musimy zastosować dość długi przykład, zredukuje się on w~następnych etapach przetwarzania w wyniku agregacji po dniach i państwach.]

#grid(columns: (1fr, 1fr), [
=== Wejście -- `charts_fmt_small.csv`

```
+---------+----------+----------------------+-------+
|region   |date      |track_id              |streams|
+---------+----------+----------------------+-------+
|Argentina|2017-01-01|6mICuAdrwEjh6Y6lroV2Kg|253019 |
|Argentina|2017-01-01|7DM4BPaS7uofFul3ywMe46|223988 |
|Argentina|2017-01-01|3AEZUABDXNtecAOSC1qTfo|210943 |
|Argentina|2017-01-01|6rQSrBHf7HlZjtcMZ4S4bO|173865 |
|Argentina|2017-01-01|58IL315gMSTD37DOZPJ2hf|153956 |
|Poland   |2017-01-01|4pdPtRcBmOSQDlJ3Fk945m|26290  |
|Poland   |2017-01-01|5aAx2yezTd8zXrkmtKl66Z|25198  |
|Poland   |2017-01-01|7BKLCZ1jbUBVqRi2FVlTVw|24642  |
|Poland   |2017-01-01|7abpmGpF7PGep2rDU68GBR|24630  |
|Poland   |2017-01-01|5knuzwU65gJK7IF5yJsuaW|23163  |
+---------+----------+----------------------+-------+
```

=== Wejście -- `api_track_to_genre_small.csv`

```
+----------------------+-------------------------+
|track_id              |genre                    |
+----------------------+-------------------------+
|4pdPtRcBmOSQDlJ3Fk945m|pop rap                  |
|4pdPtRcBmOSQDlJ3Fk945m|pop                      |
|4pdPtRcBmOSQDlJ3Fk945m|canadian pop             |
|4pdPtRcBmOSQDlJ3Fk945m|pop dance                |
|4pdPtRcBmOSQDlJ3Fk945m|electronic trap          |
|4pdPtRcBmOSQDlJ3Fk945m|dance pop                |
|4pdPtRcBmOSQDlJ3Fk945m|edm                      |
|7BKLCZ1jbUBVqRi2FVlTVw|pop                      |
|7BKLCZ1jbUBVqRi2FVlTVw|indie poptimism          |
|7BKLCZ1jbUBVqRi2FVlTVw|etherpop                 |
|7BKLCZ1jbUBVqRi2FVlTVw|pop dance                |
|7BKLCZ1jbUBVqRi2FVlTVw|electropop               |
|7BKLCZ1jbUBVqRi2FVlTVw|dance pop                |
|7BKLCZ1jbUBVqRi2FVlTVw|tropical house           |
|7BKLCZ1jbUBVqRi2FVlTVw|edm                      |
|5aAx2yezTd8zXrkmtKl66Z|pop                      |
|5aAx2yezTd8zXrkmtKl66Z|filter house             |
|5aAx2yezTd8zXrkmtKl66Z|canadian pop             |
|5aAx2yezTd8zXrkmtKl66Z|canadian contemporary r&b|
|5aAx2yezTd8zXrkmtKl66Z|electro                  |
|6rQSrBHf7HlZjtcMZ4S4bO|pop rap                  |
|6rQSrBHf7HlZjtcMZ4S4bO|reggaeton                |
|6rQSrBHf7HlZjtcMZ4S4bO|viral rap                |
|6rQSrBHf7HlZjtcMZ4S4bO|rap                      |
|6rQSrBHf7HlZjtcMZ4S4bO|reggaeton flow           |
|6rQSrBHf7HlZjtcMZ4S4bO|hip hop                  |
|6rQSrBHf7HlZjtcMZ4S4bO|rap latina               |
|6rQSrBHf7HlZjtcMZ4S4bO|trap queen               |
|6rQSrBHf7HlZjtcMZ4S4bO|reggaeton colombiano     |
|6rQSrBHf7HlZjtcMZ4S4bO|trap latino              |
|6rQSrBHf7HlZjtcMZ4S4bO|r&b                      |
|3AEZUABDXNtecAOSC1qTfo|boy band                 |
|3AEZUABDXNtecAOSC1qTfo|reggaeton                |
|3AEZUABDXNtecAOSC1qTfo|latin pop                |
|7abpmGpF7PGep2rDU68GBR|nyc pop                  |
|7abpmGpF7PGep2rDU68GBR|electro house            |
|5knuzwU65gJK7IF5yJsuaW|pop rap                  |
|5knuzwU65gJK7IF5yJsuaW|pop                      |
|5knuzwU65gJK7IF5yJsuaW|uk funky                 |
|5knuzwU65gJK7IF5yJsuaW|dancehall                |
|5knuzwU65gJK7IF5yJsuaW|uk pop                   |
|5knuzwU65gJK7IF5yJsuaW|pop dance                |
|5knuzwU65gJK7IF5yJsuaW|uk dance                 |
|5knuzwU65gJK7IF5yJsuaW|dance pop                |
|5knuzwU65gJK7IF5yJsuaW|edm                      |
|5knuzwU65gJK7IF5yJsuaW|tropical house           |
|5knuzwU65gJK7IF5yJsuaW|post-teen pop            |
|7DM4BPaS7uofFul3ywMe46|dance pop                |
|7DM4BPaS7uofFul3ywMe46|reggaeton                |
|7DM4BPaS7uofFul3ywMe46|puerto rican pop         |
|7DM4BPaS7uofFul3ywMe46|latin pop                |
|7DM4BPaS7uofFul3ywMe46|mexican pop              |
|7DM4BPaS7uofFul3ywMe46|reggaeton colombiano     |
|7DM4BPaS7uofFul3ywMe46|trap latino              |
|58IL315gMSTD37DOZPJ2hf|trap latino              |
|58IL315gMSTD37DOZPJ2hf|latin hip hop            |
|58IL315gMSTD37DOZPJ2hf|reggaeton                |
+----------------------+-------------------------+
```
], [
=== Wyjście -- `charts_genres_small.csv`

```
+---------+----------+-------------------------+-------+
|region   |date      |genre                    |streams|
+---------+----------+-------------------------+-------+
|Argentina|2017-01-01|pop rap                  |173865 |
|Argentina|2017-01-01|reggaeton                |173865 |
|Argentina|2017-01-01|viral rap                |173865 |
|Argentina|2017-01-01|rap                      |173865 |
|Argentina|2017-01-01|reggaeton flow           |173865 |
|Argentina|2017-01-01|hip hop                  |173865 |
|Argentina|2017-01-01|rap latina               |173865 |
|Argentina|2017-01-01|trap queen               |173865 |
|Argentina|2017-01-01|reggaeton colombiano     |173865 |
|Argentina|2017-01-01|trap latino              |173865 |
|Argentina|2017-01-01|r&b                      |173865 |
|Argentina|2017-01-01|boy band                 |210943 |
|Argentina|2017-01-01|reggaeton                |210943 |
|Argentina|2017-01-01|latin pop                |210943 |
|Argentina|2017-01-01|dance pop                |223988 |
|Argentina|2017-01-01|reggaeton                |223988 |
|Argentina|2017-01-01|puerto rican pop         |223988 |
|Argentina|2017-01-01|latin pop                |223988 |
|Argentina|2017-01-01|mexican pop              |223988 |
|Argentina|2017-01-01|reggaeton colombiano     |223988 |
|Argentina|2017-01-01|trap latino              |223988 |
|Argentina|2017-01-01|trap latino              |153956 |
|Argentina|2017-01-01|latin hip hop            |153956 |
|Argentina|2017-01-01|reggaeton                |153956 |
|Poland   |2017-01-01|pop rap                  |26290  |
|Poland   |2017-01-01|pop                      |26290  |
|Poland   |2017-01-01|canadian pop             |26290  |
|Poland   |2017-01-01|pop dance                |26290  |
|Poland   |2017-01-01|electronic trap          |26290  |
|Poland   |2017-01-01|dance pop                |26290  |
|Poland   |2017-01-01|edm                      |26290  |
|Poland   |2017-01-01|pop                      |24642  |
|Poland   |2017-01-01|indie poptimism          |24642  |
|Poland   |2017-01-01|etherpop                 |24642  |
|Poland   |2017-01-01|pop dance                |24642  |
|Poland   |2017-01-01|electropop               |24642  |
|Poland   |2017-01-01|dance pop                |24642  |
|Poland   |2017-01-01|tropical house           |24642  |
|Poland   |2017-01-01|edm                      |24642  |
|Poland   |2017-01-01|pop                      |25198  |
|Poland   |2017-01-01|filter house             |25198  |
|Poland   |2017-01-01|canadian pop             |25198  |
|Poland   |2017-01-01|canadian contemporary r&b|25198  |
|Poland   |2017-01-01|electro                  |25198  |
|Poland   |2017-01-01|nyc pop                  |24630  |
|Poland   |2017-01-01|electro house            |24630  |
|Poland   |2017-01-01|pop rap                  |23163  |
|Poland   |2017-01-01|pop                      |23163  |
|Poland   |2017-01-01|uk funky                 |23163  |
|Poland   |2017-01-01|dancehall                |23163  |
|Poland   |2017-01-01|uk pop                   |23163  |
|Poland   |2017-01-01|pop dance                |23163  |
|Poland   |2017-01-01|uk dance                 |23163  |
|Poland   |2017-01-01|dance pop                |23163  |
|Poland   |2017-01-01|edm                      |23163  |
|Poland   |2017-01-01|tropical house           |23163  |
|Poland   |2017-01-01|post-teen pop            |23163  |
+---------+----------+-------------------------+-------+
```
])
]

#pagebreak()
#set page(flipped: true)

== Proces 3 -- `charts_daily_genres`

#grid(columns: (1fr, 3fr), [
=== Wejście -- `charts_genres_small.csv`

#[
#show raw.where(block: true): set text(size: 5.5pt)
```
+---------+----------+-------------------------+-------+
|region   |date      |genre                    |streams|
+---------+----------+-------------------------+-------+
|Argentina|2017-01-01|pop rap                  |173865 |
|Argentina|2017-01-01|reggaeton                |173865 |
|Argentina|2017-01-01|viral rap                |173865 |
|Argentina|2017-01-01|rap                      |173865 |
|Argentina|2017-01-01|reggaeton flow           |173865 |
|Argentina|2017-01-01|hip hop                  |173865 |
|Argentina|2017-01-01|rap latina               |173865 |
|Argentina|2017-01-01|trap queen               |173865 |
|Argentina|2017-01-01|reggaeton colombiano     |173865 |
|Argentina|2017-01-01|trap latino              |173865 |
|Argentina|2017-01-01|r&b                      |173865 |
|Argentina|2017-01-01|boy band                 |210943 |
|Argentina|2017-01-01|reggaeton                |210943 |
|Argentina|2017-01-01|latin pop                |210943 |
|Argentina|2017-01-01|dance pop                |223988 |
|Argentina|2017-01-01|reggaeton                |223988 |
|Argentina|2017-01-01|puerto rican pop         |223988 |
|Argentina|2017-01-01|latin pop                |223988 |
|Argentina|2017-01-01|mexican pop              |223988 |
|Argentina|2017-01-01|reggaeton colombiano     |223988 |
|Argentina|2017-01-01|trap latino              |223988 |
|Argentina|2017-01-01|trap latino              |153956 |
|Argentina|2017-01-01|latin hip hop            |153956 |
|Argentina|2017-01-01|reggaeton                |153956 |
|Poland   |2017-01-01|pop rap                  |26290  |
|Poland   |2017-01-01|pop                      |26290  |
|Poland   |2017-01-01|canadian pop             |26290  |
|Poland   |2017-01-01|pop dance                |26290  |
|Poland   |2017-01-01|electronic trap          |26290  |
|Poland   |2017-01-01|dance pop                |26290  |
|Poland   |2017-01-01|edm                      |26290  |
|Poland   |2017-01-01|pop                      |24642  |
|Poland   |2017-01-01|indie poptimism          |24642  |
|Poland   |2017-01-01|etherpop                 |24642  |
|Poland   |2017-01-01|pop dance                |24642  |
|Poland   |2017-01-01|electropop               |24642  |
|Poland   |2017-01-01|dance pop                |24642  |
|Poland   |2017-01-01|tropical house           |24642  |
|Poland   |2017-01-01|edm                      |24642  |
|Poland   |2017-01-01|pop                      |25198  |
|Poland   |2017-01-01|filter house             |25198  |
|Poland   |2017-01-01|canadian pop             |25198  |
|Poland   |2017-01-01|canadian contemporary r&b|25198  |
|Poland   |2017-01-01|electro                  |25198  |
|Poland   |2017-01-01|nyc pop                  |24630  |
|Poland   |2017-01-01|electro house            |24630  |
|Poland   |2017-01-01|pop rap                  |23163  |
|Poland   |2017-01-01|pop                      |23163  |
|Poland   |2017-01-01|uk funky                 |23163  |
|Poland   |2017-01-01|dancehall                |23163  |
|Poland   |2017-01-01|uk pop                   |23163  |
|Poland   |2017-01-01|pop dance                |23163  |
|Poland   |2017-01-01|uk dance                 |23163  |
|Poland   |2017-01-01|dance pop                |23163  |
|Poland   |2017-01-01|edm                      |23163  |
|Poland   |2017-01-01|tropical house           |23163  |
|Poland   |2017-01-01|post-teen pop            |23163  |
+---------+----------+-------------------------+-------+
```
]
], [
=== Wyjście -- `charts_daily_genres_small.csv`

#rotate(35deg, reflow: true)[
#show raw.where(block: true): set text(size: 5pt)
```
+---------+----------+-------+--------+----+-------+--------+-----------+---------+----------+---------+--------+---------------+----+-----------+------------+-------+-----+--------------+-----------+------------+----------------+
|region   |date      |pop    |rap     |rock|edm    |hip_hop |trap_latino|reggaeton|electropop|dance_pop|pop_rap |musica_mexicana|trap|modern_rock|classic_rock|uk_pop |k_pop|tropical_house|melodic_rap|canadian_pop|modern_bollywood|
+---------+----------+-------+--------+----+-------+--------+-----------+---------+----------+---------+--------+---------------+----+-----------+------------+-------+-----+--------------+-----------+------------+----------------+
|Argentina|2017-01-01|0.0    |173865.0|0.0 |0.0    |173865.0|551809.0   |762752.0 |0.0       |223988.0 |173865.0|0.0            |0.0 |0.0        |0.0         |0.0    |0.0  |0.0           |0.0        |0.0         |0.0             |
|Poland   |2017-01-01|99293.0|0.0     |0.0 |74095.0|0.0     |0.0        |0.0      |24642.0   |74095.0  |49453.0 |0.0            |0.0 |0.0        |0.0         |23163.0|0.0  |47805.0       |0.0        |51488.0     |0.0             |
+---------+----------+-------+--------+----+-------+--------+-----------+---------+----------+---------+--------+---------------+----+-----------+------------+-------+-----+--------------+-----------+------------+----------------+
```
]
])

#pagebreak()

#[
#show raw.where(block: true): set text(size: 5.5pt)

== Proces 4 -- `charts_genre_popularity`

=== Wejście -- `charts_daily_sum_small.csv`

```
+---------+----------+-------+
|region   |date      |streams|
+---------+----------+-------+
|Argentina|2017-01-01|1015771|
|Poland   |2017-01-01|123923 |
+---------+----------+-------+
```

=== Wejście -- `charts_daily_genres_small.csv`

#text(fill: red)[Tabela została podzielona na dwie części w połowie, aby zmieścić się na stronie.]

#grid(columns: 2, align: horizon, [
```
+---------+----------+-------+--------+----+-------+--------+-----------+---------+----------+---------+--------+--
|region   |date      |pop    |rap     |rock|edm    |hip_hop |trap_latino|reggaeton|electropop|dance_pop|pop_rap |mu
+---------+----------+-------+--------+----+-------+--------+-----------+---------+----------+---------+--------+--
|Argentina|2017-01-01|0.0    |173865.0|0.0 |0.0    |173865.0|551809.0   |762752.0 |0.0       |223988.0 |173865.0|0.
|Poland   |2017-01-01|99293.0|0.0     |0.0 |74095.0|0.0     |0.0        |0.0      |24642.0   |74095.0  |49453.0 |0.
+---------+----------+-------+--------+----+-------+--------+-----------+---------+----------+---------+--------+--
```
], [
#h(0.5em) #text("⋯     ⏎", size: 20pt, fill: red, weight: "bold") 
])
#grid(columns: 2, align: horizon, [
#text("⋯", size: 20pt, fill: red, weight: "bold") #h(1.5em)
], [
```
-------------+----+-----------+------------+-------+-----+--------------+-----------+------------+----------------+
sica_mexicana|trap|modern_rock|classic_rock|uk_pop |k_pop|tropical_house|melodic_rap|canadian_pop|modern_bollywood|
-------------+----+-----------+------------+-------+-----+--------------+-----------+------------+----------------+
0            |0.0 |0.0        |0.0         |0.0    |0.0  |0.0           |0.0        |0.0         |0.0             |
0            |0.0 |0.0        |0.0         |23163.0|0.0  |47805.0       |0.0        |51488.0     |0.0             |
-------------+----+-----------+------------+-------+-----+--------------+-----------+------------+----------------+
```
])

=== Wyjście -- `charts_genre_popularity_small.csv`

#text(fill: red)[Tabela została podzielona na dwie części w połowie, aby zmieścić się na stronie.]

#grid(columns: 2, align: horizon, [
```
+---------+----------+------------------+-------------------+----+------------------+-------------------+------------------+------------------+-------------------+--------
|region   |date      |pop               |rap                |rock|edm               |hip_hop            |trap_latino       |reggaeton         |electropop         |dance_po
+---------+----------+------------------+-------------------+----+------------------+-------------------+------------------+------------------+-------------------+--------
|Argentina|2017-01-01|0.0               |0.17116554814027965|0.0 |0.0               |0.17116554814027965|0.5432415377087946|0.7509094077306795|0.0                |0.220510
|Poland   |2017-01-01|0.8012475488811601|0.0                |0.0 |0.5979116064007488|0.0                |0.0               |0.0               |0.19884928544338015|0.597911
+---------+----------+------------------+-------------------+----+------------------+-------------------+------------------+------------------+-------------------+--------
```
], [
#h(0.5em) #text("⋯     ⏎", size: 20pt, fill: red, weight: "bold") 
])
#grid(columns: 2, align: horizon, [
#text("⋯", size: 20pt, fill: red, weight: "bold") #h(1.5em)
], [
```
-----------+-------------------+---------------+----+-----------+------------+-------------------+-----+------------------+-----------+-------------------+----------------+
p          |pop_rap            |musica_mexicana|trap|modern_rock|classic_rock|uk_pop             |k_pop|tropical_house    |melodic_rap|canadian_pop       |modern_bollywood|
-----------+-------------------+---------------+----+-----------+------------+-------------------+-----+------------------+-----------+-------------------+----------------+
33156095223|0.17116554814027965|0.0            |0.0 |0.0        |0.0         |0.0                |0.0  |0.0               |0.0        |0.0                |0.0             |
6064007488 |0.3990623209573687 |0.0            |0.0 |0.0        |0.0         |0.18691445494379574|0.0  |0.3857637403871759|0.0        |0.41548380849398414|0.0             |
-----------+-------------------+---------------+----+-----------+------------+-------------------+-----+------------------+-----------+-------------------+----------------+
```
])

]

#pagebreak()

== Proces 5 -- `output`

#[
#show raw.where(block: true): set text(size: 3.9pt)

=== Wejście -- `charts_genre_popularity_small.csv`

```
+---------+----------+------------------+-------------------+----+------------------+-------------------+------------------+------------------+-------------------+-------------------+-------------------+---------------+----+-----------+------------+-------------------+-----+------------------+-----------+-------------------+----------------+
|region   |date      |pop               |rap                |rock|edm               |hip_hop            |trap_latino       |reggaeton         |electropop         |dance_pop          |pop_rap            |musica_mexicana|trap|modern_rock|classic_rock|uk_pop             |k_pop|tropical_house    |melodic_rap|canadian_pop       |modern_bollywood|
+---------+----------+------------------+-------------------+----+------------------+-------------------+------------------+------------------+-------------------+-------------------+-------------------+---------------+----+-----------+------------+-------------------+-----+------------------+-----------+-------------------+----------------+
|Argentina|2017-01-01|0.0               |0.17116554814027965|0.0 |0.0               |0.17116554814027965|0.5432415377087946|0.7509094077306795|0.0                |0.22051033156095223|0.17116554814027965|0.0            |0.0 |0.0        |0.0         |0.0                |0.0  |0.0               |0.0        |0.0                |0.0             |
|Poland   |2017-01-01|0.8012475488811601|0.0                |0.0 |0.5979116064007488|0.0                |0.0               |0.0               |0.19884928544338015|0.5979116064007488 |0.3990623209573687 |0.0            |0.0 |0.0        |0.0         |0.18691445494379574|0.0  |0.3857637403871759|0.0        |0.41548380849398414|0.0             |
+---------+----------+------------------+-------------------+----+------------------+-------------------+------------------+------------------+-------------------+-------------------+-------------------+---------------+----+-----------+------------+-------------------+-----+------------------+-----------+-------------------+----------------+
```

=== Wejście -- `charts_daily_popularity_small.csv`

```
+---------+----------+----------+
|region   |date      |popularity|
+---------+----------+----------+
|Argentina|2017-01-01|AVERAGE   |
|Poland   |2017-01-01|AVERAGE   |
+---------+----------+----------+
```

=== Wejście -- `charts_daily_sum_small.csv`

```
+---------+----------+-------+
|region   |date      |streams|
+---------+----------+-------+
|Argentina|2017-01-01|1015771|
|Poland   |2017-01-01|123923 |
+---------+----------+-------+
```

=== Wejście -- `daily_country_weather_small.csv`

```
+---------+----------+------------------+----------------+
|country  |date      |temperature_c     |precipitation_mm|
+---------+----------+------------------+----------------+
|Argentina|2017-01-01|26.084999999999997|11.0            |
|Poland   |2017-01-01|0.5777777777777778|0.0375          |
+---------+----------+------------------+----------------+
```

=== Wejście -- `wdi_interpolated_small.csv`

```
+---------+----------+------------------------+------------------+------------------+----------------------------+---------------------------+
|country  |date      |rural_population_percent|fertility_rate    |gdp_per_capita_usd|mobile_subscriptions_per_100|refugee_population_promille|
+---------+----------+------------------------+------------------+------------------+----------------------------+---------------------------+
|Argentina|2017-01-01|8.372665753424657       |2.2408            |12795.23644407049 |145.9031169849315           |0.07494967452275388        |
|Poland   |2017-01-01|39.8222                 |1.3902465753424658|12382.69604662878 |137.5341305038356           |0.3091731147958112         |
+---------+----------+------------------------+------------------+------------------+----------------------------+---------------------------+
```

=== Wyjście -- `output_small.csv`

#text(fill: red)[Tabela została podzielona na dwie części w połowie, aby zmieścić się na stronie.]

#grid(columns: 2, align: horizon, [
```
+---------+----------+-------------+----------------+------------------+----------------+----------------------------+------------------+----------------------+--------------------------------+-------------------------------+------------------+-------------------+----------+------------------+----
|country  |date      |daily_streams|daily_popularity|temperature_c     |precipitation_mm|wdi:rural_population_percent|wdi:fertility_rate|wdi:gdp_per_capita_usd|wdi:mobile_subscriptions_per_100|wdi:refugee_population_promille|genre:pop         |genre:rap          |genre:rock|genre:edm         |genr
+---------+----------+-------------+----------------+------------------+----------------+----------------------------+------------------+----------------------+--------------------------------+-------------------------------+------------------+-------------------+----------+------------------+----
|Argentina|2017-01-01|1015771      |AVERAGE         |26.084999999999997|11.0            |8.372665753424657           |2.2408            |12795.23644407049     |145.9031169849315               |0.07494967452275388            |0.0               |0.17116554814027965|0.0       |0.0               |0.17
|Poland   |2017-01-01|123923       |AVERAGE         |0.5777777777777778|0.0375          |39.8222                     |1.3902465753424658|12382.69604662878     |137.5341305038356               |0.3091731147958112             |0.8012475488811601|0.0                |0.0       |0.5979116064007488|0.0 
+---------+----------+-------------+----------------+------------------+----------------+----------------------------+------------------+----------------------+--------------------------------+-------------------------------+------------------+-------------------+----------+------------------+----
```
], [
#h(0.5em) #text("⋯     ⏎", size: 20pt, fill: red, weight: "bold")
])
#grid(columns: 2, align: horizon, [
#text("⋯", size: 20pt, fill: red, weight: "bold") #h(1.5em)
], [
```
---------------+------------------+------------------+-------------------+-------------------+-------------------+---------------------+----------+-----------------+------------------+-------------------+-----------+--------------------+-----------------+-------------------+----------------------+
e:hip_hop      |genre:trap_latino |genre:reggaeton   |genre:electropop   |genre:dance_pop    |genre:pop_rap      |genre:musica_mexicana|genre:trap|genre:modern_rock|genre:classic_rock|genre:uk_pop       |genre:k_pop|genre:tropical_house|genre:melodic_rap|genre:canadian_pop |genre:modern_bollywood|
---------------+------------------+------------------+-------------------+-------------------+-------------------+---------------------+----------+-----------------+------------------+-------------------+-----------+--------------------+-----------------+-------------------+----------------------+
116554814027965|0.5432415377087946|0.7509094077306795|0.0                |0.22051033156095223|0.17116554814027965|0.0                  |0.0       |0.0              |0.0               |0.0                |0.0        |0.0                 |0.0              |0.0                |0.0                   |
               |0.0               |0.0               |0.19884928544338015|0.5979116064007488 |0.3990623209573687 |0.0                  |0.0       |0.0              |0.0               |0.18691445494379574|0.0        |0.3857637403871759  |0.0              |0.41548380849398414|0.0                   |
---------------+------------------+------------------+-------------------+-------------------+-------------------+---------------------+----------+-----------------+------------------+-------------------+-----------+--------------------+-----------------+-------------------+----------------------+
```
])
]
