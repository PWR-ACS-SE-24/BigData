#set par(justify: true)

#align(center)[
  #text(size: 20pt, weight: "bold", )[Przetwarzanie dużych zbiorów danych (PDZD)]

  Zespół B1 (geopolityka i muzyka): \
  *Tomasz Chojnacki (260365), Kamila Iwańska (253027), Jakub Zehner (260285)*
]

= Zadanie 4 -- Procesy przetwarzania danych

TU OPIS ŻE POD DIAGRAMEM SĄ OBLICZENIA I PLIKI TYMCZASOWE

TU DIAGRAM

== `charts_fmt`

=== Obliczenia

- filtrowanie `chart == "top200"`
- obliczenie `track_id = url[len("https://open.spotify.com/track/"):]`

=== Plik tymczasowy

Rozmiar: \~400 MB

```
┌────────────┬────────────────────────┬───────────┬─────────┐
│    date    │        track_id        │  region   │ streams │
│    date    │        varchar         │  varchar  │  int64  │
├────────────┼────────────────────────┼───────────┼─────────┤
│ 2017-01-01 │ 6mICuAdrwEjh6Y6lroV2Kg │ Argentina │  253019 │
│ 2017-01-01 │ 7DM4BPaS7uofFul3ywMe46 │ Argentina │  223988 │
│ 2017-01-01 │ 3AEZUABDXNtecAOSC1qTfo │ Argentina │  210943 │
│ 2017-01-01 │ 6rQSrBHf7HlZjtcMZ4S4bO │ Argentina │  173865 │
│ 2017-01-01 │ 58IL315gMSTD37DOZPJ2hf │ Argentina │  153956 │
│ 2017-01-01 │ 5J1c3M4EldCfNxXwrwt8mT │ Argentina │  151140 │
│ 2017-01-01 │ 1MpKZi1zTXpERKwxmOu1PH │ Argentina │  148369 │
│ 2017-01-01 │ 3QwBODjSEzelZyVjxPOHdq │ Argentina │  143004 │
│ 2017-01-01 │ 0sXvAOmXgjR2QUqLK1MltU │ Argentina │  126389 │
│ 2017-01-01 │ 20ZAJdsKB5IGbGj4ilRt2o │ Argentina │  112012 │
...
└────────────┴────────────────────────┴───────────┴─────────┘
```

== `charts_daily_sum`

=== Obliczenia

- grupowanie `region`, `date`
- agregacja `streams` (suma)

=== Plik tymczasowy

Rozmiar: \~1 MB

```
┌───────────┬────────────┬──────────┐
│  region   │    date    │ streams  │
│  varchar  │    date    │  int128  │
├───────────┼────────────┼──────────┤
│ Argentina │ 2017-01-01 │  7888872 │
│ Argentina │ 2017-01-02 │  6010041 │
│ Argentina │ 2017-01-03 │  5921907 │
│ Argentina │ 2017-01-04 │  6019573 │
│ Argentina │ 2017-01-05 │  6223646 │
│ Argentina │ 2017-01-06 │  6485179 │
│ Argentina │ 2017-01-07 │  6840811 │
│ Argentina │ 2017-01-08 │  5691182 │
│ Argentina │ 2017-01-09 │  5880184 │
│ Argentina │ 2017-01-10 │  5896533 │
...
└───────────────────────────────────┘
```

== `charts_yearly_stats`

=== Obliczenia

- obliczenie `year = year(date)`
- grupowanie `region`, `year`
- agregacja `streams` (średnia, odchylenie standardowe)

=== Plik tymczasowy

Rozmiar: \<1 MB

```
┌───────────┬────────────┬────────────┬───────────────────┬────────────────────┐
│  region   │    date    │  streams   │    stream_avg     │     stream_dev     │
│  varchar  │    date    │   double   │      double       │       double       │
├───────────┼────────────┼────────────┼───────────────────┼────────────────────┤
│ Argentina │ 2017-01-01 │  7888872.0 │ 7424229.113259668 │ 1034041.7027895374 │
│ Argentina │ 2017-01-02 │  6010041.0 │ 7424229.113259668 │ 1034041.7027895374 │
│ Argentina │ 2017-01-03 │  5921907.0 │ 7424229.113259668 │ 1034041.7027895374 │
│ Argentina │ 2017-01-04 │  6019573.0 │ 7424229.113259668 │ 1034041.7027895374 │
│ Argentina │ 2017-01-05 │  6223646.0 │ 7424229.113259668 │ 1034041.7027895374 │
│ Argentina │ 2017-01-06 │  6485179.0 │ 7424229.113259668 │ 1034041.7027895374 │
│ Argentina │ 2017-01-07 │  6840811.0 │ 7424229.113259668 │ 1034041.7027895374 │
│ Argentina │ 2017-01-08 │  5691182.0 │ 7424229.113259668 │ 1034041.7027895374 │
│ Argentina │ 2017-01-09 │  5880184.0 │ 7424229.113259668 │ 1034041.7027895374 │
│ Argentina │ 2017-01-10 │  5896533.0 │ 7424229.113259668 │ 1034041.7027895374 │
...
└──────────────────────────────────────────────────────────────────────────────┘
```

== `charts_daily_popularity`

=== Obliczenia

- złączenie `charts_daily_sum d` i `charts_yearly_stats y` \ na `year(d.date) == y.year && d.region == y.region`
- obliczenie `stream_std = (d.streams - y.stream_avg) / y.stream_dev`
- obliczenie `popularity =`
  - `"VERY LOW"` jeśli `stream_std < -1.5`
  - `"LOW"` jeśli `-1.5 <= stream_std < -0.5`
  - `"AVERAGE"` jeśli `-0.5 <= stream_std <= 0.5`
  - `"HIGH"` jeśli `0.5 < stream_std <= 1.5`
  - `"VERY HIGH"` jeśli `stream_std > 1.5`

=== Plik tymczasowy

Rozmiar: \<1 MB

```
┌───────────┬────────────┬────────────┐
│  region   │    date    │ popularity │
│  varchar  │    date    │  varchar   │
├───────────┼────────────┼────────────┤
│ Argentina │ 2017-01-01 │ AVERAGE    │
│ Argentina │ 2017-01-02 │ LOW        │
│ Argentina │ 2017-01-03 │ LOW        │
│ Argentina │ 2017-01-04 │ LOW        │
│ Argentina │ 2017-01-05 │ LOW        │
│ Argentina │ 2017-01-06 │ LOW        │
│ Argentina │ 2017-01-07 │ LOW        │
│ Argentina │ 2017-01-08 │ VERY LOW   │
│ Argentina │ 2017-01-09 │ LOW        │
│ Argentina │ 2017-01-10 │ LOW        │
...
└─────────────────────────────────────┘
```

== `daily_country_weather`

=== Obliczenia

- filtrowanie `date >= "2017-01-01" && date <= "2021-12-31"`
- złączenie `daily_weather w` i `cities c` \ na `w.station_id == c.station_id`
- grupowanie `country`, `date`
- agregacja `temperature_c` (średnia), `precipitation_mm` (średnia)
- obliczenie `precipitation_mm = 0.0` jeśli `precipitation_mm == null`
- filtrowanie `temperature_c != null`

=== Plik tymczasowy

Rozmiar: \~2 MB

```
┌─────────────┬────────────┬──────────────────────┬──────────────────┐
│   country   │    date    │    temperature_c     │ precipitation_mm │
│   varchar   │    date    │        double        │      double      │
├─────────────┼────────────┼──────────────────────┼──────────────────┤
│ Afghanistan │ 2017-01-01 │   5.3166666666666655 │              0.0 │
│ Afghanistan │ 2017-01-02 │    5.016666666666667 │              2.0 │
│ Afghanistan │ 2017-01-03 │   3.0666666666666664 │           10.725 │
│ Afghanistan │ 2017-01-04 │                 2.65 │            109.0 │
│ Afghanistan │ 2017-01-05 │   1.9333333333333333 │            29.95 │
│ Afghanistan │ 2017-01-06 │   0.9833333333333331 │             7.25 │
│ Afghanistan │ 2017-01-07 │   0.2833333333333334 │             33.0 │
│ Afghanistan │ 2017-01-08 │ -0.07999999999999999 │             18.0 │
│ Afghanistan │ 2017-01-09 │ -0.21666666666666665 │              0.0 │
│ Afghanistan │ 2017-01-10 │    0.866666666666667 │              0.0 │
...
└────────────────────────────────────────────────────────────────────┘
```

== `wdi_normalized`

=== Obliczenia

- filtrowanie `code in ('SP.RUR.TOTL.ZS', 'SP.DYN.TFRT.IN', 'NY.GDP.PCAP.CD', 'IT.CEL.SETS.P2', 'SM.POP.REFG', 'SP.POP.TOTL')`
- projekcja `country`, `code`, `2016` `2017`, `2018`, `2019`, `2020`, `2021`
- unpivotowanie `2016`, `2017`, `2018`, `2019`, `2020`, `2021` na `year`, `value`
- pivotowanie `country`, `year`, `code`, `value` na `country`, `year`, `country`, `year`, `rural_population_percent`, `fertility_rate`, `gdp_per_capita_usd`, `mobile_subscriptions_per_100`, `refugee_population`, `total_population`
- obliczenie `refugee_population_promille = refugee_population / total_population * 1000`

=== Plik tymczasowy

Rozmiar: \<1 MB

#[
#set text(size: 5.635pt)
```
┌─────────────────────────────┬───────┬──────────────────────────┬──────────────────┬────────────────────┬──────────────────────────────┬─────────────────────────────┐
│           country           │ year  │ rural_population_percent │  fertility_rate  │ gdp_per_capita_usd │ mobile_subscriptions_per_100 │ refugee_population_promille │
│           varchar           │ int32 │          double          │      double      │       double       │            double            │           double            │
├─────────────────────────────┼───────┼──────────────────────────┼──────────────────┼────────────────────┼──────────────────────────────┼─────────────────────────────┤
│ Afghanistan                 │  2016 │                    74.75 │            5.129 │   530.149830802984 │                  67.13641492 │           2.130182913434396 │
│ Afghanistan                 │  2017 │                    74.75 │            5.129 │   530.149830802984 │                  67.13641492 │           2.130182913434396 │
│ Afghanistan                 │  2018 │                   74.505 │            5.002 │   502.056770622973 │                  59.90264778 │          1.9687743684483217 │
│ Afghanistan                 │  2019 │                   74.246 │             4.87 │   500.522664145294 │                  59.78387904 │          1.9123102480125564 │
│ Afghanistan                 │  2020 │                   73.974 │             4.75 │   516.866552182696 │                   58.1902139 │          1.8546026234577802 │
│ Afghanistan                 │  2021 │                   73.686 │            4.643 │   368.754614175459 │                  56.55443457 │           1.669573521958973 │
...
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```
]

== `wdi_interpolated`

// Tabelka tymczasowa (1):
// - data
// - kraj
// - artysta
// - liczba_odtworzeń


// Tabelka tymczasowa (2):
// - data
// - kraj
// - gatunek
// - liczba_odtworzeń

// Tabelka tymczasowa (8): (to samo co w dwójce tylko bez duplikatów)
// - data
// - kraj
// - gatunek
// - liczba_odtworzeń 
