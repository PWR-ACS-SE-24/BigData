#set par(justify: true)

#align(center)[
  #text(size: 20pt, weight: "bold")[Przetwarzanie dużych zbiorów danych (PDZD)]

  Zespół B1 (geopolityka i muzyka): \
  *Tomasz Chojnacki (260365), Kamila Iwańska (253027), Jakub Zehner (260285)*
]

#let g(body) = text(fill: rgb("#58cf39"))[#body]
#let r(body) = text(fill: rgb("#FF4136"))[#body]
#let ye = g[*✔*]
#let no = r[*✕*]
#let na = text(fill: rgb("#00000066"))[---]
#let mi = box(text(fill: rgb("#ff851b"))[#scale(150%)[*\~*]])
#let tech_header(head: "Etap") = table.header(
  table.cell(rowspan: 2)[*#head*],
  table.cell(colspan: 4)[*Technologia*],
  [*MapReduce* \ #image("./img/mapreduce.png", height: 22pt)],
  [*Hive* \ #image("./img/hive.png", height: 22pt)],
  [*Spark* \ #image("./img/spark.png", height: 22pt)],
  [*Pig* \ #image("./img/pig.png", height: 22pt)],
)

= Zadanie 12 -- Podsumowanie
#set heading(numbering: (..n) => n.pos().slice(1).map(str).join(".") + ")")

== Wstęp

Poniższy dokument podsumowuje wszystkie etapy kursu Przetwarzanie Dużych Zbiorów Danych, które zrealizowaliśmy w ramach zespołu B1. W dokumencie omawiamy technologie, które wykorzystaliśmy, porównujemy je pod kątem wydajności i doświadczeń deweloperskich, a także przedstawiamy przeprowadzone eksperymenty.

Poniższy obrazek przedstawia diagram etapów, które zrealizowaliśmy w ramach kursu, a pod nim znajduje się tabela z informacjami o tym, jakie etapy zostały zrealizowane w jakiej technologii.

#image("./img/pdzd.drawio.png")

#pagebreak()

Tabela poniżej przedstawia, jakie etapy zostały zrealizowane w jakiej technologii. W tabeli używamy zielonych znaczników (#ye) do oznaczenia etapów zrealizowanych w danej technologii, a czerwonych znaczników (#no) do oznaczenia etapów, które nie zostały zrealizowane w danej technologii.


#align(
  center,
  table(
    align: center + horizon,
    columns: (auto, 80pt, 80pt, 80pt, 80pt),
    tech_header(),
    [`charts_fmt`], ye, no, no, no,
    [`charts_artists`], no, no, ye, no,
    [`charts_genres`], no, no, ye, no,
    [`charts_daily_genres`], no, no, ye, no,
    [`charts_daily_sum`], ye, no, no, no,
    [`charts_yearly_stats`], no, ye, no, no,
    [`charts_genre_popularity`], no, no, ye, no,
    [`charts_daily_popularity`], no, ye, no, no,
    [`output`], no, no, ye, no,
    [`daily_country_weather`], ye, ye, ye, ye,
    [`wdi_normalized`], no, ye, no, no,
    [`wdi_interpolated`], no, ye, no, no,
  ),
)

== Porównanie wydajności

Poniżej przedstawiono wydajność poszczególnych technologii w kontekście różnych procesów przetwarzania. Wszystkie dane zapisano w sekundach oraz w przypadku wielu pomiarów w danej technologii (np. Spark DataFrames vs SQL) wybrano zawsze lepszy wynik.

#align(
  center,
  table(
    align: center + horizon,
    columns: (auto, 80pt, 80pt, 80pt, 80pt),
    tech_header(),
    [`charts_fmt`], [42.010], na, na, na,
    [`charts_artists`], na, na, [6.589], na,
    [`charts_genres`], na, na, [7.288], na,
    [`charts_daily_genres`], na, na, [18.915], na,
    [`charts_daily_sum`], [27.605], na, na, na,
    [`charts_yearly_stats`], na, [35.961], na, na,
    [`charts_genre_popularity`], na, na, [3.381], na,
    [`charts_daily_popularity`], na, [16.424], na, na,
    [`output`], na, na, [6.235], na,
    [`daily_country_weather`], [39.923], [41.711], [5.935], [85.355],
    [`wdi_normalized`], na, [35.547], na, na,
    [`wdi_interpolated`], na, [25.698], na, na,
  ),
)

#pagebreak()

Niestety, tylko jeden z naszych etapów został zaimplementowany we wszystkich technologiach, co utrudnia porównanie wydajności. Na bazie tego procesu moglibyśmy stwierdzić, że najszybszy jest Spark, niezależnie od tego, czy używamy DataFrames czy SQL. Następnie, zrównane wyniki osiągają MapReduce i Hive (uruchomiony na silniku MapReduce), a na końcu jest Pig, który jest zdecydowanie najwolniejszy. Nie jest to systematyczna analiza, jednakże nasze subiektywne odczucia są zgodne z~tymi wynikami. Praca w Spark wymagała najmniej oczekiwania.

== Porównanie doświadczeń deweloperskich

Aby ocenić subiektywne doświadczenia dotyczące pracy z poszczególnymi technologiami, dokonaliśmy porównania na podstawie kilku aspektów. Pod względem każdego kryterium nadajemy oceny w~skali 0 (#no), ½ (#mi), 1 (#ye). Oczywiście doświadczenia niesposób ocenić w sposób obiektywny, jednakze aby uzyskać bardziej miarodajne wyniki, zebraliśmy wyniki od wszystkich członków zespołu. Analizowane przez nas aspekty to:
- *Instalacja* -- jak łatwa jest instalacja,
- *Konfiguracja* -- jak łatwa jest początkowa konfiguracja i zmiana parametrów,
- *Dokumentacja* -- czy i jakiej jakości dokumentacja jest dostępna,
- *Interfejs (API)* -- czy łatwo jest pisać kod w danej technologii,
- *Uniwersalność* -- czy dana technologia pozwala na realizację dowolnego etapu,
- *Inicjalizacja* -- jak długo trwa uruchomienie środowiska,
- *Wydajność* -- jak szybko wykonują się poszczególne zadania (patrz: wcześniejsza sekcja),
- *Stabilność* -- jak często technologia zatrzymuje się lub pokazuje błędy bez powodu,
- *Debugowanie* -- czy łatwo jest zinterpretować błędy i debugować kod rozwiązania,
- *Dashboard (GUI)* -- czy i jakiej jakości jest dostępny graficzny pulpit nawigacyjny.

#align(
  center,
  table(
    align: center + horizon,
    columns: (auto, 80pt, 80pt, 80pt, 80pt),
    tech_header(head: "Aspekt"),
    [*Instalacja*], mi, no, ye, ye,
    [*Konfiguracja*], mi, ye, mi, ye,
    [*Dokumentacja*], no, mi, ye, no,
    [*Interfejs (API)*], no, ye, ye, mi,
    [*Uniwersalność*], no, mi, ye, no,
    [*Inicjalizacja*], no, ye, no, mi,
    [*Wydajność*], mi, mi, ye, no,
    [*Stabilność*], no, mi, ye, ye,
    [*Debugowanie*], no, mi, ye, ye,
    [*Dashboard (GUI)*], ye, mi, mi, no,
    [*Podsumowanie*], [*2.5* / 10], [*6* / 10], [*8* / 10], [*5* / 10],
  ),
)

Sumarycznie, najlepszy okazał się Spark, następnie Hive, Pig i MapReduce. Zgadza się to też z~naszymi odczuciami, ponieważ zdecydowanie najgorzej pracowało nam się z MapReduce, a najlepiej z Spark. Technologia MapReduce korzysta z niewygodnego i słabo udokumentowanego API, które wymaga pisania dużej ilości kodu, podziału procesów na wiele zadań, a ponadto często zawieszało się (a~czasami wręcz cały system) bez oczywistego powodu. Natomiast Spark zezwala na pisanie kodu w Scali, Pythonie oraz SQL, jest uniwersalny i naszym zdaniem najszybszy spośród przetestowanych.

// TODO: trudność instalacji/konfiguracji, wygoda "pisania" kodu (jaki język, itd.), napotkane błędy (nie szanujemy parquet)
// @Tomek

== Przeprowadzone eksperymenty


Poniżej znajduje się podsumowanie wszystkich wykonanych przez nas eksperymentów.

=== MapReduce

#[
#set text(size: 9pt)
#let g(body) = text(fill: rgb("#58cf39"))[#body]
#let r(body) = text(fill: rgb("#FF4136"))[#body]
#align(center, table(
  columns: 10,
  align: right + horizon,
  table.header(table.cell(rowspan: 2)[*Część procesu*], table.cell(colspan: 3)[*Reducery*], table.cell(colspan: 3)[*Repliki*], table.cell(colspan: 3)[*Podział [MB]*], [*`1`*], [*`2`*], [*`3`*], [*`3`*], [*`2`*], [*`1`*], [*`128`*], [*`192`*], [*`256`*]),
  [*`charts_fmt`*], [42.010], [36.330], [42.388], [42.010], [38.410], [37.315], [42.010], [49.879], [45.850],
  [*`charts_daily_sum`*], [27.605], [25.733], [29.237], [27.605], [25.700], [28.066], [27.605], [31.406], [26.581],
  [*`daily_country_weather_1`*], [19.245], [17.634], [21.685], [19.245], [17.937], [18.240], [19.245], [20.043], [19.628],
  [*`daily_country_weather_2`*], [20.678], [21.192], [20.456], [20.678], [18.789], [20.608], [20.678], [21.029], [19.024],
))
]

=== Hive
==== Porównanie HIVE z Map-Reduce na podstawie `daily_country_weather`

#align(center, table(
    columns: 2,
    align: horizon + right,
    table.header([*Narzędzie*], [*Czas wykonania [s]*]),
    [*Map-Reduce*], [39.923],
    [*HIVE*], [41.711]
))

==== Wpływ liczby reducerów na czas wykonania

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



==== Wpływ rozmiaru podziału pliku na czas wykonania

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

=== Spark

==== Porównanie SQL i DataFrames

#align(center, table(
  align: right + horizon,
  columns: 3,
  table.header(table.cell(rowspan: 2)[*Etap*], table.cell(colspan: 2)[*Czas wykonania [s]*], [*SQL*], [*DataFrames*]),
  [`daily_country_weather`], [5.935], [4.396]
))

==== Wpływ liczby dostępnych rdzeni (`cores.max`)

#align(center, table(
  align: right + horizon,
  columns: 4,
  table.header(table.cell(rowspan: 2)[*Etap*], table.cell(colspan: 3)[*Czas wykonania [s]*], [*1 rdzeń*], [*2 rdzenie*], [*4 rdzenie*]),
  [`charts_artists`], [13.518], [11.639], [6.589],
  [`charts_genres`], [11.828], [10.731], [7.288],
  [`charts_daily_genres`], [28.657], [21.292], [18.915],
  [`charts_genre_popularity`], [6.376], [3.461], [3.381],
  [`output`], [8.459], [7.091], [6.235],
  [`daily_country_weather`], [11.050], [6.937], [5.935],
  [*Suma*], [79.888], [61.151], [48.343],
))

==== Wpływ partycjonowania (`sql.shuffle.partitions`)

#align(center, table(
  align: right + horizon,
  columns: 4,
  table.header(table.cell(rowspan: 2)[*Etap*], table.cell(colspan: 3)[*Czas wykonania [s]*], [*3 partycje*], [*5 partycji*], [*7 partycji*]),
  [`charts_artists`], [8.139], [6.589], [7.559],
  [`charts_genres`], [7.727], [7.288], [9.610],
  [`charts_daily_genres`], [20.247], [18.915], [21.631],
  [`charts_genre_popularity`], [2.769], [3.381], [3.065],
  [`output`], [6.934], [6.235], [7.127],
  [`daily_country_weather`], [7.386], [5.935], [8.476],
  [*Suma*], [53.202], [48.343], [57.468],
))

==== Wpływ pamięci dostępnej egzekutorowi (`executor.memory`)


#align(center, table(
  align: right + horizon,
  columns: 4,
  table.header(table.cell(rowspan: 2)[*Etap*], table.cell(colspan: 3)[*Czas wykonania [s]*], [*½ GB*], [*1 GB*], [*2 GB*]),
  [`charts_artists`], [6.722], [6.589], [6.817],
  [`charts_genres`], [7.305], [7.288], [7.820],
  [`charts_daily_genres`], [18.774], [18.915], [18.468],
  [`charts_genre_popularity`], [3.944], [3.381], [3.345],
  [`output`], [6.083], [6.235], [6.087],
  [`daily_country_weather`], [6.374], [5.935], [6.279],
  [*Suma*], [49.202], [48.343], [48.816],
))

==== Wpływ limitu rozgłoszenia (`sql.autoBroadcastJoinThreshold`)

#align(center, table(
  align: right + horizon,
  columns: 4,
  table.header(table.cell(rowspan: 2)[*Etap*], table.cell(colspan: 3)[*Czas wykonania [s]*], [*5 MB*], [*10 MB*], [*-1 (brak)*]),
  [`charts_artists`], [10.145], [6.589], [10.077],
  [`charts_genres`], [8.731], [7.288], [10.376],
  [`charts_daily_genres`], [22.021], [18.915], [15.703],
  [`charts_genre_popularity`], [2.278], [3.381], [3.603],  
  [`output`], [5.169], [6.235], [6.257],
  [`daily_country_weather`], [5.182], [5.935], [5.676],
  [*Suma*], [53.526], [48.343], [51.692],
))

==== Spark vs. Map-Reduce

#align(center, table(
  align: right + horizon,
  columns: 3,
  table.header(table.cell(rowspan: 2)[*Etap*], table.cell(colspan: 2)[*Czas wykonania [s]*], [*Spark*], [*Map-Reduce*]),
  [`daily_country_weather`], [4.396], [39.923]
))

==== Spark vs. HIVE

#align(center, table(
  align: right + horizon,
  columns: 3,
  table.header(table.cell(rowspan: 2)[*Etap*], table.cell(colspan: 2)[*Czas wykonania [s]*], [*Spark*], [*Hive*]),
  [`daily_country_weather`], [4.396], [49.124]
))

==== Spark vs. PIG

#align(center, table(
  align: right + horizon,
  columns: 3,
  table.header(table.cell(rowspan: 2)[*Etap*], table.cell(colspan: 2)[*Czas wykonania [s]*], [*Spark*], [*PIG*]),
  [`daily_country_weather`], [4.396], [85.355]
))

// TODO: przekopiować z poprzednich etapów wszystkie tabelki z eksperymentami + jeden paragraf
// @Kamila

== Podsumowanie

SQL spoko, mapreduce nie spoko

// TODO: opisać jakie technologie w sumie są fajne a jakie nie, co się udało a co nie, jakie są wnioski, zwyzywać hjben
// @Kuba
