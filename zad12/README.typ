#set par(justify: true)

#align(center)[
  #text(size: 20pt, weight: "bold")[Przetwarzanie dużych zbiorów danych (PDZD)]

  Zespół B1 (geopolityka i muzyka): \
  *Tomasz Chojnacki (260365), Kamila Iwańska (253027), Jakub Zehner (260285)*
]

= Zadanie 12 -- Podsumowanie

== Wstęp

Poniższy dokument podsumowuje wszystkie etapy kursu Przetwarzanie Dużych Zbiorów Danych, które zrealizowaliśmy w ramach zespołu B1. W dokumencie omawiamy technologie, które wykorzystaliśmy, porównujemy je pod kątem wydajności i doświadczeń deweloperskich, a także przedstawiamy przeprowadzone eksperymenty.

Poniższy obrazek przedstawia diagram etapów, które zrealizowaliśmy w ramach kursu, a pod nim znajduje się tabela z informacjami o tym, jakie etapy zostały zrealizowane w jakiej technologii.

#image("./img/pdzd.drawio.png")

#pagebreak()

Tabela poniżej przedstawia, jakie etapy zostały zrealizowane w jakiej technologii. W tabeli używamy zielonych znaczników (✔) do oznaczenia etapów zrealizowanych w danej technologii, a czerwonych znaczników (✕) do oznaczenia etapów, które nie zostały zrealizowane w danej technologii.

#let g(body) = text(fill: rgb("#58cf39"))[#body]
#let r(body) = text(fill: rgb("#FF4136"))[#body]
#align(
  center,
  table(
    align: center + horizon,
    columns: (auto, 80pt, 80pt, 80pt, 80pt),
    table.header(
      table.cell(rowspan: 2)[*Etap*],
      table.cell(colspan: 4)[*Technologia*],
      [*MapReduce* \ #image("./img/mapreduce.png", height: 25pt)],
      [*Hive* \ #image("./img/hive.png", height: 25pt)],
      [*Spark* \ #image("./img/spark.png", height: 25pt)],
      [*Pig* \ #image("./img/pig.png", height: 25pt)],
    ),

    [`charts_fmt`], g[✔], r[✕], r[✕], r[✕],
    [`charts_artists`], r[✕], r[✕], g[✔], r[✕],
    [`charts_genres`], r[✕], r[✕], g[✔], r[✕],
    [`charts_daily_genres`], r[✕], r[✕], g[✔], r[✕],
    [`charts_daily_sum`], g[✔], r[✕], r[✕], r[✕],
    [`charts_yearly_stats`], r[✕], g[✔], r[✕], r[✕],
    [`charts_genre_popularity`], r[✕], r[✕], g[✔], r[✕],
    [`charts_daily_popularity`], r[✕], g[✔], r[✕], r[✕],
    [`output`], r[✕], r[✕], g[✔], r[✕],
    [`daily_country_weather`], g[✔], g[✔], g[✔], g[✔],
    [`wdi_normalized`], r[✕], g[✔], r[✕], r[✕],
    [`wdi_interpolated`], r[✕], g[✔], r[✕], r[✕],
  ),
)


// TODO: ta taka mapka diagram etapów wszystkich (bez kolorków), tabelka z checkmarkami/iksami jakie etapy w czym zrobione
// @Kuba

== Porównanie wydajności

// TODO: tabelka łącząca wszystkie wyniki (jedna kolumna na technologię, jeden wiersz na etap, większość komórek pusta)
// @Tomek

== Porównanie doświadczeń deweloperskich

// TODO: trudność instalacji/konfiguracji, wygoda "pisania" kodu (jaki język, itd.), napotkane błędy (nie szanujemy parquet)
// @Tomek

== Przeprowadzone eksperymenty

// TODO: przekopiować z poprzednich etapów wszystkie tabelki z eksperymentami + jeden paragraf
// @Kamila

== Podsumowanie

SQL spoko, mapreduce nie spoko

// TODO: opisać jakie technologie w sumie są fajne a jakie nie, co się udało a co nie, jakie są wnioski, zwyzywać hjben
// @Kuba
