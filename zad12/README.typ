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

// TODO: przekopiować z poprzednich etapów wszystkie tabelki z eksperymentami + jeden paragraf
// @Kamila

== Podsumowanie

SQL spoko, mapreduce nie spoko

// TODO: opisać jakie technologie w sumie są fajne a jakie nie, co się udało a co nie, jakie są wnioski, zwyzywać hjben
// @Kuba
