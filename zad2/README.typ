#set page(flipped: true)
#set par(justify: true)
#let blockquote(body) = block(
     inset: 8pt,
     stroke: (left: 4pt + blue),
     body
)
#show link: set text(fill: blue)
#let wyrzucone = rgb("#ffa2a2")

#align(center)[
  #text(size: 20pt, weight: "bold", )[Przetwarzanie dużych zbiorów danych (PDZD)]

  Zespół B1 (geopolityka i muzyka): \
  *Tomasz Chojnacki (260365), Kamila Iwańska (253027), Jakub Zehner (260285)*
]

= Zadanie 2 -- Dane źródłowe

Wybraliśmy cztery źródła danych, w tym trzy statyczne o różnych rozmiarach i formatach oraz jedno dynamiczne, pobierające dane z API. Wszystkie zbiory danych są publicznie dostępne oraz zostały załączone oraz opisane poniżej.

*Wiersze, które wstępnie nie będą przez nas używane są zaznaczone kolorem czerwonym.
*
== Źródło 1 -- Spotify Charts

Zbiór danych zawiera 26.2 miliona rekordów oraz 9 kolumn, takich jak: tytuł utworu, wykonawca, lista przebojów i zajęte miejsce, data, region.

- *Źródło:* #link("https://www.kaggle.com/datasets/dhruvildave/spotify-charts")[kaggle.com/datasets/dhruvildave/spotify-charts]
- *Dostępność:* dane dostępne do pobrania dla zalogowanych użytkowników
- *Typ:* statyczne
- *Rozmiar:* 3 401 MB
- *Wymiary:* 9 kolumn x 26 173 515 rekordów
- *Format:* CSV

#align(center, table(
  columns: 4,
  align: left + horizon,
  fill: (x, y) => if y == 0 { silver } else if y in (1, 4, 8) { wyrzucone } else { none },
  table.header([*Atrybut*], [*Typ*], [*Opis*], [*Przykład*]),
  [*`title`*], [`string`], [Tytuł utworu muzycznego], [`"Let Me Love You"`],
  [*`rank`*], [`integer`], [Miejsce utworu w rankingu], [`15`],
  [*`date`*], [`date`], [Data wystąpienia utworu na danym miejscu w danym rankingu], [`2017-01-01`],
  [*`artist`*], [`string`], [Pełna nazwa wykonawcy utworu muzycznego], [`"DJ Snake, Justin Bieber"`],
  [*`url`*], [`string`], [Odnośnik do utworu w serwisie Spotify], [`"https://open.spotify.com/`\ `track/6DUdDIRgLqCGq1DwkNWQTN"`],
  [*`region`*], [`category`], [Państwo, w którym nastąpiło umiejscowienie w rankingu], [`"Argentina"`],
  [*`chart`*], [`category`], [Rodzaj listy przebojów, na której utwór został umiejscowiony \ Dostępne wartości: `top200`, `viral50`], [`"top200"`],
  [*`trend`*], [`category`], [Zmiana pozycji utworu względem poprzedniego umiejscowienia \ Dostępne wartości: `MOVE_UP`, `MOVE_DOWN`, `SAME_POSITION`, `NEW_ENTRY`], [`"MOVE_DOWN"`],
  [*`streams`*], [`integer`], [Liczba odtworzeń utworu w danym regionie danego dnia], [`95010`]
))

== Źródło 2 -- Spotify Web API

API oferuje dostęp do cech audio utworów (np. tempo, tonacja, głośność, taneczność, energia, itp.) oraz metadanych (np. nazwa, wykonawca, rok wydania, itp.). Do korzystania z API wymagane jest podanie klucza, zezwalającego na około 25 zapytań na sekundę. Jako że będziemy analizować tylko popularne utwory (takie, które znalazły się na liście przebojów w co najmniej jednym kraju) nie powinno to stanowić problemu.

- *Źródło:* #link("https://developer.spotify.com/documentation/web-api/")[developer.spotify.com/documentation/web-api]
- *Dostępność:* dane dostępne poprzez klucz API, który można uzyskać po darmowej rejestracji
- *Typ:* dynamiczne
- *Rozmiar:* ---
- *Wymiary:* ---
- *Format:* JSON

=== 2.1 Tracks

#align(center, table(
  columns: 4,
  align: left + horizon,
  fill: (x, y) => if y == 0 { silver } else if y in (1, 3, 4, 6, 7, 8, 9, 11, 12, 13, 15, 16, 17, 18) { wyrzucone } else { none },
  table.header([*Atrybut*], [*Typ*], [*Opis*], [*Przykład*]),
  [*`album`*], [`object`], [Informacje o albumie, do którego należy utwór], [---],
  [*`artists`*], [`array of`\ `SimplifiedArtistObject`], [Lista artystów wykonujących utwór], [```JSON
[
   {
      "external_urls": {
         "spotify": "https://open.spotify.com/artist/1Xyo4u8uXC1ZmMpatF05PJ"
      },
      "href": "https://api.spotify.com/v1/artists/1Xyo4u8uXC1ZmMpatF05PJ",
      "id": "1Xyo4u8uXC1ZmMpatF05PJ",
      "name": "The Weeknd",
      "type": "artist",
      "uri": "spotify:artist:1Xyo4u8uXC1ZmMpatF05PJ"
   }
]
  ```],
  [*`available_markets`*], [`array of category`], [Lista rynków, w których utwór jest dostępny], [`["PL"]`],
  [*`disc_number`*], [`integer`], [Numer płyty, na której znajduje się utwór], [`1`],
  [*`duration_ms`*], [`integer`], [Długość utworu w milisekundach], [`219106`],
  [*`explicit`*], [`boolean`], [Czy utwór zawiera wulgaryzmy?], [`false`],
  [*`external_ids`*], [`object`], [Zewnętrzne identyfikatory utworu], [---],
  [*`external_urls`*], [`object`], [Zewnętrzne odnośniki do utworu], [```JSON
{
   "spotify": "https://open.spotify.com/track/0e7ipj03S05BNilyu5bRzt"
}```],
  [*`href`*], [`string`], [Odnośnik do zasobu utworu], [`"https://api.spotify.com/v1/tracks/0e7ipj03S05BNilyu5bRzt"`],
  [*`id`*], [`string`], [Identyfikator utworu], [`"0e7ipj03S05BNilyu5bRzt"`],
  [*`is_playable`*], [`boolean`], [Czy utwór jest odtwarzalny?], [`true`],
  [*`restrictions`*], [`object`], [Ograniczenia dotyczące odtwarzania utworu], [---],
  [*`name`*], [`string`], [Nazwa utworu], [`"Blinding Lights"`],
  [*`popularity`*], [`integer`], [Popularność utworu w serwisie Spotify], [`73`],
  [*`preview_url`*], [`string`], [Adres URL do fragmentu utworu do odsłuchania], [---],
  [*`track_number`*], [`integer`], [Numer utworu na płycie], [`1`],
  [*`type`*], [`category`], [Typ zasobu], [`"track"`],
  [*`uri`*], [`string`], [Identyfikator zasobu],[`"spotify:track:0e7ipj03S05BNilyu5bRzt"`],
))

#pagebreak()

=== 2.2 Artists

#align(center, table(
  columns: 4,
  align: left + horizon,
  fill: (x, y) => if y == 0 { silver } else if y in (1, 2, 6, 7, 9, 10) { wyrzucone } else { none },
  table.header([*Atrybut*], [*Typ*], [*Opis*], [*Przykład*]),
  [*`external_urls`*], [`object`], [Zewnętrzne odnośniki do zasobu], [```JSON
{
   "spotify": "https://open.spotify.com/artist/1Xyo4u8uXC1ZmMpatF05PJ"
}```],
  [*`followers`*], [`object`], [Informacje o osobach obserwujących artystę], [```JSON
{
   "href": "https://api.spotify.com/v1/artists/1Xyo4u8uXC1ZmMpatF05PJ/followers",
   "total": 0
}```],
  [*`genres`*], [`array of category`], [Lista gatunków muzycznych, z których artysta jest znany], [`["pop", "pop rock"]`],
  [*`href`*], [`string`], [Odnośnik do zasobu artysty], [`"https://api.spotify.com/v1/artists/1Xyo4u8uXC1ZmMpatF05PJ"`],
  [*`id`*], [`string`], [Identyfikator artysty], [`"1Xyo4u8uXC1ZmMpatF05PJ"`],
  [*`images`*], [`array of`\ `ImageObject`], [Lista obrazów artysty], [```JSON
[
   {
      "url": "https://i.scdn.co/image/ab67616d00001e02ff9ca10b55ce82ae553c8228",
      "height": 300,
      "width": 300
   }
]```],
  [*`name`*], [`string`], [Nazwa artysty], [`"The Weeknd"`],
  [*`popularity`*], [`integer`], [Popularność artysty w serwisie Spotify], [`60`],
  [*`type`*], [`category`], [Typ zasobu], [`"artist"`],
  [*`uri`*], [`string`], [Identyfikator zasobu], [`"spotify:artist:1Xyo4u8uXC1ZmMpatF05PJ"`],
))

== Źródło 3 -- The Weather Dataset

Zbiór danych zawiera ponad 50 milionów rekordów zawierających pomiary warunków atmosferycznych (temperatura, wilgotność, ciśnienie, itp.) z różnych stacji meteorologicznych na całym świecie. Dane obejmują zakres od 1833 do 2023 roku oraz 1235 miast z 214 krajów.

- *Źródło:* #link("https://www.kaggle.com/datasets/guillemservera/global-daily-climate-data")[kaggle.com/datasets/guillemservera/global-daily-climate-data]
- *Dostępność:* dane dostępne do pobrania dla zalogowanych użytkowników
- *Typ:* statyczne
- *Rozmiar:* 233 MB (weather) / 86 KB (cities)
- *Wymiary:* 14 kolumn x 27 635 763 rekordów (weather) / 8 kolumn x 1 245 rekordów (cities)
- *Format:* PARQUET (weather) / CSV (cities)

=== 3.1 Weather

#align(center, table(
  columns: 4,
  align: left + horizon,
  fill: (x, y) => if y == 0 { silver } else if y in (2,) { wyrzucone } else { none },
  table.header([*Atrybut*], [*Typ*], [*Opis*], [*Przykład*]),
  [*`station_id`*], [`category`], [Identyfikator stacji pogodowej, w której dokonano pomiaru], [`41515`],
  [*`city_name`*], [`category`], [Nazwa miasta, w którym dokonano pomiaru], [`"Zürich"`],
  [*`date`*], [`date`], [Data dokonania pomiaru], [`2017-07-04`],
  [*`season`*], [`category`], [Pora roku, w której dokonano pomiaru], [`"Winter"`],
  [*`avg_temp_c`*], [`float`], [Średnia temperatura w ciągu dnia (C#sym.degree)], [`17.0`],
  [*`min_temp_c`*], [`float`], [Najmniejsza temperatura w ciągu dnia (C#sym.degree)], [`-8.5`],
  [*`max_temp_c`*], [`float`], [Największa temperatura w ciągu dnia (C#sym.degree)], [`28.0`],
  [*`precipitation_mm`*], [`float`], [Ilość opadów atmosferycznych w milimetrach], [`10.0`],
  [*`snow_depth_mm`*], [`float`], [Wysokość warstwy śniegu w milimetrach], [`0.0`],
  [*`avg_wind_dir_deg`*], [`float`], [Średni kierunek wiatru w stopniach], [`210.0`],
  [*`avg_wind_speed_kmh`*], [`float`], [Średnia szybkość wiatru w kilometrach na godzinę], [`15.0`],
  [*`peak_wind_gust_kmh`*], [`float`], [Szczytowa szybkość wiatru w kilometrach na godzinę], [`20.0`],
  [*`avg_sea_level_pres_hpa`*], [`float`], [Średnie znormalizowane ciśnienie atmosferyczne w hektopaskalach], [`1012.6`],
  [*`sunshine_total_min`*], [`float`], [Łączna liczba minut, przez które stacja była oświetlona przez Słońce], [`700.0`],
))

=== 3.2 Cities

#align(center, table(
  columns: 4,
  align: left + horizon,
  fill: (x, y) => if y == 0 { silver } else if y in (2, 4, 5) { wyrzucone } else { none },
  table.header([*Atrybut*], [*Typ*], [*Opis*], [*Przykład*]),
  [*`station_id`*], [`category`], [Identyfikator stacji pogodowej, w której dokonano pomiaru], [`41515`],
  [*`city_name`*], [`category`], [Nazwa miasta], [`"Zürich"`],
  [*`country`*], [`category`], [Nazwa państwa], [`"Afghanistan"`],
  [*`state`*], [`category`], [Nazwa regionu lub `"Unknown"`], [`"Al Wadi at Jadid"`],
  [*`iso2`*], [`category`], [Dwuznakowy skrót państwa według normy ISO 3166-1 alfa-2], [`"PL"`],
  [*`iso3`*], [`category`], [Trzyznakowy skrót państwa według normy ISO 3166-1 alfa-3], [`"TUR"`],
))

== Źródło 4 -- World Development Indicators

Strona umożliwia eksport danych CSV/XLSX dla różnych wskaźników ekonomicznych, społecznych, środowiskowych, itp. z różnych krajów na świecie. Dane obejmują zakres od 1960 do 2023 roku, przy czym nas interesuje podzbiór lat zgodny z zakresem zbioru Spotify Charts.

- *Źródło:* #link("https://www.kaggle.com/datasets/shubhamgupta012/world-development-indicator")[kaggle.com/datasets/shubhamgupta012/world-development-indicator]
- *Dostępność:* dane dostępne do pobrania dla zalogowanych użytkowników
- *Typ:* statyczne
- *Rozmiar:* 205 MB
- *Wymiary:* 68 kolumn x 393 148 rekordów
- *Format:* CSV

#align(center, table(
  columns: 4,
  align: left + horizon,
  fill: (x, y) => if y == 0 { silver } else if y in (1, 5, 6, 7, 8, 14, 15) { wyrzucone } else { none },
  table.header([*Atrybut*], [*Typ*], [*Opis*], [*Przykład*]),
  [*`CountryName`*], [`category`], [Nazwa kraju], [`"Poland"`],
  [*`CountryCode`*], [`category`], [Kod kraju według normy ISO 3166-1 alfa-3], [`"POL"`],
  [*`IndicatorName`*], [`category`], [Nazwa wskaźnika geopolitycznego], [`"GDP per capita (current US$)"`],
  [*`IndicatorCode`*], [`category`], [Kod wskaźnika geopolitycznego], [`"NY.GDP.PCAP.CD"`],
  [*`1960`*], [`float`], [Wartość wskaźnika dla roku 1960], [`6173452.2`],
  [*`1961`*], [`float`], [Wartość wskaźnika dla roku 1961], [`6173452.2`],
  table.cell(colspan: 4, align(center, [*...*])),
  [*`2016`*], [`float`], [Wartość wskaźnika dla roku 2016], [`6173452.2`],
  [*`2017`*], [`float`], [Wartość wskaźnika dla roku 2017], [`6173452.2`],
  [*`2018`*], [`float`], [Wartość wskaźnika dla roku 2018], [`6173452.2`],
  [*`2019`*], [`float`], [Wartość wskaźnika dla roku 2019], [`6173452.2`],
  [*`2020`*], [`float`], [Wartość wskaźnika dla roku 2020], [`6173452.2`],
  [*`2021`*], [`float`], [Wartość wskaźnika dla roku 2021], [`6173452.2`],
  [*`2022`*], [`float`], [Wartość wskaźnika dla roku 2022], [`6173452.2`],
  [*`2023`*], [`float`], [Wartość wskaźnika dla roku 2023], [`6173452.2`],
))

== Przykłady

Źródła dynamiczne zostały przedstawione na dole.

=== Źródło 1

#align(center, table(
  columns: 6,
  align: left + horizon,
  fill: (x, y) => if y == 0 { silver } else { none },
  table.header([*`rank`*], [*`date`*], [*`url`*], [*`region`*], [*`chart`*], [*`streams`*]),
  ..csv(bytes("1,2017-01-01,https://open.spotify.com/track/6mICuAdrwEjh6Y6lroV2Kg,Argentina,top200,253019\n2,2017-01-01,https://open.spotify.com/track/7DM4BPaS7uofFul3ywMe46,Argentina,top200,223988\n3,2017-01-01,https://open.spotify.com/track/3AEZUABDXNtecAOSC1qTfo,Argentina,top200,210943\n4,2017-01-01,https://open.spotify.com/track/6rQSrBHf7HlZjtcMZ4S4bO,Argentina,top200,173865\n5,2017-01-01,https://open.spotify.com/track/58IL315gMSTD37DOZPJ2hf,Argentina,top200,153956\n6,2017-01-01,https://open.spotify.com/track/5J1c3M4EldCfNxXwrwt8mT,Argentina,top200,151140\n7,2017-01-01,https://open.spotify.com/track/1MpKZi1zTXpERKwxmOu1PH,Argentina,top200,148369\n8,2017-01-01,https://open.spotify.com/track/3QwBODjSEzelZyVjxPOHdq,Argentina,top200,143004\n9,2017-01-01,https://open.spotify.com/track/0sXvAOmXgjR2QUqLK1MltU,Argentina,top200,126389\n10,2017-01-01,https://open.spotify.com/track/20ZAJdsKB5IGbGj4ilRt2o,Argentina,top200,112012\n")).flatten()
))

#pagebreak()

== Źródło 3.1

#[
#show table.cell.where(y: 0): rotate.with(-63deg, reflow: true)
#align(center, text(size: 10pt, table(
  columns: 14,
  align: left + horizon,
  fill: (x, y) => if y == 0 { silver } else { none },
  table.header([*`station_id`*], [*`city_name`*], [*`date`*], [*`season`*], [*`avg_temp_c`*], [*`min_temp_c`*], [*`max_temp_c`*], [*`precipitation_mm`*], [*`snow_depth_mm`*], [*`avg_wind_dir_deg`*], [*`avg_wind_speed_kmh`*], [*`peak_wind_gust_kmh`*], [*`avg_sea_level_pres_hpa`*], [*`sunshine_total_min`*]),
  ..csv(bytes("03772,London,2023-08-26,Summer,15.2,11.9,19.4,2.3,,234.0,13.1,33.0,1008.8,\n03772,London,2023-08-27,Summer,15.5,11.9,19.0,4.5,,284.0,11.8,27.8,1011.5,\n03772,London,2023-08-28,Summer,16.5,12.4,20.8,1.0,,299.0,9.3,22.2,1015.5,\n03772,London,2023-08-29,Summer,16.6,12.8,20.3,,,259.0,11.2,27.8,1012.1,\n03772,London,2023-08-30,Summer,15.8,12.6,19.0,,,269.0,12.3,27.8,1009.9,\n03772,London,2023-08-31,Summer,15.8,13.0,18.7,,,224.0,13.2,33.3,1010.6,\n03772,London,2023-09-01,Autumn,17.2,14.3,20.1,,,215.0,15.9,33.3,1012.4,\n03772,London,2023-09-02,Autumn,17.8,14.4,21.8,,,242.0,10.8,24.1,1017.1,\n03772,London,2023-09-03,Autumn,17.2,14.2,20.5,,,282.0,11.4,25.9,1017.1,\n03772,London,2023-09-04,Autumn,16.5,13.2,19.8,,,285.0,12.4,25.9,1015.5,\n")).flatten()
)))
]

=== Źródło 3.2

#align(center, table(
  columns: 3,
  align: left + horizon,
  fill: (x, y) => if y == 0 { silver } else { none },
  table.header([*`station_id`*], [*`country`*], [*`iso3`*]),
  ..csv(bytes("12295,Poland,POL\n12160,Poland,POL\n12566,Poland,POL\n12385,Poland,POL\n12330,Poland,POL\n12205,Poland,POL\n12375,Poland,POL\n12424,Poland,POL\nD6170,Poland,POL\n")).flatten()
))

=== Źródło 4

#[
#show table.cell.where(x: 2): rotate.with(-63deg, reflow: true)
#align(center, table(
  columns: 8,
  align: left + horizon,
  fill: (x, y) => if y == 0 { silver } else { none },
  table.header([*`CountryCode`*], [*`IndicatorName`*], [#rotate(+63deg, reflow: true)[*`IndicatorCode`*]], [*`2017`*], [*`2018`*], [*`2019`*], [*`2020`*], [*`2021`*]),
  ..csv(bytes("POL,\"Voice and Accountability: Percentile Rank, Upper Bound of 90% Confidence Interval\",VA.PER.RNK.UPPER,76.8472900390625,76.328498840332,73.9130401611328,71.0144958496094,70.5314025878906\nPOL,Voice and Accountability: Standard Error,VA.STD.ERR,0.110909812152386,0.113874100148678,0.11335938423872,0.103448435664177,0.103527672588825\nPOL,\"Vulnerable employment, female (% of female employment) (modeled ILO estimate)\",SL.EMP.VULN.FE.ZS,13.587591,13.322336,12.989584,13.297173,11.956534\nPOL,\"Vulnerable employment, male (% of male employment) (modeled ILO estimate)\",SL.EMP.VULN.MA.ZS,18.620761,18.574463,18.373276,19.473306,19.010651\nPOL,\"Vulnerable employment, total (% of total employment) (modeled ILO estimate)\",SL.EMP.VULN.ZS,16.375862,16.225472,15.981864,16.73348,15.844036\nPOL,\"Wage and salaried workers, female (% of female employment) (modeled ILO estimate)\",SL.EMP.WORK.FE.ZS,83.92041,83.96349,84.31709,84.15019,85.55141\nPOL,\"Wage and salaried workers, male (% of male employment) (modeled ILO estimate)\",SL.EMP.WORK.MA.ZS,76.09477,76.2968,76.5343,75.69006,75.85049\nPOL,\"Wage and salaried workers, total (% of total employment) (modeled ILO estimate)\",SL.EMP.WORK.ZS,79.58518,79.72569,79.99141,79.4431,80.20528\nPOL,Women Business and the Law Index Score (scale 1-100),SG.LAW.INDX,93.75,93.75,93.75,93.75,93.75\nPOL,Women\'s share of population ages 15+ living with HIV (%),SH.DYN.AIDS.FE.ZS,21.6,21.1,20.5,19.9,19.5\n")).flatten()
))
]

=== Źródło 2.1

#text(size: 8pt)[
```JSON
{
  "album": {
    "album_type": "single",
    "total_tracks": 1,
    "available_markets": [],
    "external_urls": {
      "spotify": "https://open.spotify.com/album/0tGPJ0bkWOUmH7MEOR77qc"
    },
    "href": "https://api.spotify.com/v1/albums/0tGPJ0bkWOUmH7MEOR77qc",
    "id": "0tGPJ0bkWOUmH7MEOR77qc",
    "images": [
      {
        "url": "https://i.scdn.co/image/ab67616d0000b2737359994525d219f64872d3b1",
        "height": 640,
        "width": 640
      },
      {
        "url": "https://i.scdn.co/image/ab67616d00001e027359994525d219f64872d3b1",
        "height": 300,
        "width": 300
      },
      {
        "url": "https://i.scdn.co/image/ab67616d000048517359994525d219f64872d3b1",
        "height": 64,
        "width": 64
      }
    ],
    "name": "Cut To The Feeling",
    "release_date": "2017-05-26",
    "release_date_precision": "day",
    "type": "album",
    "uri": "spotify:album:0tGPJ0bkWOUmH7MEOR77qc",
    "artists": [
      {
        "external_urls": {
          "spotify": "https://open.spotify.com/artist/6sFIWsNpZYqfjUpaCgueju"
        },
        "href": "https://api.spotify.com/v1/artists/6sFIWsNpZYqfjUpaCgueju",
        "id": "6sFIWsNpZYqfjUpaCgueju",
        "name": "Carly Rae Jepsen",
        "type": "artist",
        "uri": "spotify:artist:6sFIWsNpZYqfjUpaCgueju"
      }
    ]
  },
  "artists": [
    {
      "external_urls": {
        "spotify": "https://open.spotify.com/artist/6sFIWsNpZYqfjUpaCgueju"
      },
      "href": "https://api.spotify.com/v1/artists/6sFIWsNpZYqfjUpaCgueju",
      "id": "6sFIWsNpZYqfjUpaCgueju",
      "name": "Carly Rae Jepsen",
      "type": "artist",
      "uri": "spotify:artist:6sFIWsNpZYqfjUpaCgueju"
    }
  ],
  "available_markets": [],
  "disc_number": 1,
  "duration_ms": 207959,
  "explicit": false,
  "external_ids": {
    "isrc": "USUM71703861"
  },
  "external_urls": {
    "spotify": "https://open.spotify.com/track/11dFghVXANMlKmJXsNCbNl"
  },
  "href": "https://api.spotify.com/v1/tracks/11dFghVXANMlKmJXsNCbNl",
  "id": "11dFghVXANMlKmJXsNCbNl",
  "name": "Cut To The Feeling",
  "popularity": 0,
  "preview_url": null,
  "track_number": 1,
  "type": "track",
  "uri": "spotify:track:11dFghVXANMlKmJXsNCbNl",
  "is_local": false
}
```]

=== Źródło 2.2

#text(size: 8pt)[```JSON
{
  "album_type": "single",
  "total_tracks": 1,
  "external_urls": {
    "spotify": "https://open.spotify.com/album/3BHYARhzjKq9WZ1z9c7zkG"
  },
  "href": "https://api.spotify.com/v1/albums/3BHYARhzjKq9WZ1z9c7zkG?market=PL&locale=pl%2Cen-US%3Bq%3D0.7%2Cen%3Bq%3D0.3",
  "id": "3BHYARhzjKq9WZ1z9c7zkG",
  "images": [
    {
      "url": "https://i.scdn.co/image/ab67616d0000b2732bd12de9222900ace4ccee62",
      "height": 640,
      "width": 640
    },
    {
      "url": "https://i.scdn.co/image/ab67616d00001e022bd12de9222900ace4ccee62",
      "height": 300,
      "width": 300
    },
    {
      "url": "https://i.scdn.co/image/ab67616d000048512bd12de9222900ace4ccee62",
      "height": 64,
      "width": 64
    }
  ],
  "name": "POLSKIE TANGO",
  "release_date": "2020-07-10",
  "release_date_precision": "day",
  "type": "album",
  "uri": "spotify:album:3BHYARhzjKq9WZ1z9c7zkG",
  "artists": [
    {
      "external_urls": {
        "spotify": "https://open.spotify.com/artist/7CJgLPEqiIRuneZSolpawQ"
      },
      "href": "https://api.spotify.com/v1/artists/7CJgLPEqiIRuneZSolpawQ",
      "id": "7CJgLPEqiIRuneZSolpawQ",
      "name": "Taco Hemingway",
      "type": "artist",
      "uri": "spotify:artist:7CJgLPEqiIRuneZSolpawQ"
    },
    {
      "external_urls": {
        "spotify": "https://open.spotify.com/artist/7afPAbg5jb45KFUSnHIMFG"
      },
      "href": "https://api.spotify.com/v1/artists/7afPAbg5jb45KFUSnHIMFG",
      "id": "7afPAbg5jb45KFUSnHIMFG",
      "name": "Lanek",
      "type": "artist",
      "uri": "spotify:artist:7afPAbg5jb45KFUSnHIMFG"
    }
  ],
  "tracks": {
    "href": "https://api.spotify.com/v1/albums/3BHYARhzjKq9WZ1z9c7zkG/tracks?offset=0&limit=50&market=PL&locale=pl,en-US;q%3D0.7,en;q%3D0.3",
    "limit": 50,
    "next": null,
    "offset": 0,
    "previous": null,
    "total": 1,
    "items": [
      {
        "artists": [
          {
            "external_urls": {
              "spotify": "https://open.spotify.com/artist/7CJgLPEqiIRuneZSolpawQ"
            },
            "href": "https://api.spotify.com/v1/artists/7CJgLPEqiIRuneZSolpawQ",
            "id": "7CJgLPEqiIRuneZSolpawQ",
            "name": "Taco Hemingway",
            "type": "artist",
            "uri": "spotify:artist:7CJgLPEqiIRuneZSolpawQ"
          },
          {
            "external_urls": {
              "spotify": "https://open.spotify.com/artist/7afPAbg5jb45KFUSnHIMFG"
            },
            "href": "https://api.spotify.com/v1/artists/7afPAbg5jb45KFUSnHIMFG",
            "id": "7afPAbg5jb45KFUSnHIMFG",
            "name": "Lanek",
            "type": "artist",
            "uri": "spotify:artist:7afPAbg5jb45KFUSnHIMFG"
          }
        ],
        "disc_number": 1,
        "duration_ms": 183858,
        "explicit": true,
        "external_urls": {
          "spotify": "https://open.spotify.com/track/7tQBvP2x04zzXSNeapGMIT"
        },
        "href": "https://api.spotify.com/v1/tracks/7tQBvP2x04zzXSNeapGMIT",
        "id": "7tQBvP2x04zzXSNeapGMIT",
        "is_playable": true,
        "name": "POLSKIE TANGO",
        "preview_url": null,
        "track_number": 1,
        "type": "track",
        "uri": "spotify:track:7tQBvP2x04zzXSNeapGMIT",
        "is_local": false
      }
    ]
  },
  "copyrights": [
    {
      "text": "© 2020 2020",
      "type": "C"
    },
    {
      "text": "℗ 2020 2020",
      "type": "P"
    }
  ],
  "external_ids": {
    "upc": "00000657896094"
  },
  "genres": [],
  "label": "2020",
  "popularity": 18,
  "is_playable": true
}
```]
