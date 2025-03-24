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

= Zadanie 3 -- Dane wynikowe

Poniżej przedstawiono zbiór wynikowy naszego przetwarzania. Dane te mogą zostać wykorzystane do analizy zależności gatunków muzycznych od wymienionych wskaźników lub wytrenowania modelu predykcyjnego sztucznej inteligencji, który będzie proponował użytkownikowi utwory muzyczne na podstawie sytuacji geopolitycznej oraz warunków atmosferycznych w danym kraju danego dnia. Zbiór będzie eksportowany do formatu CSV.

#align(center, table(
  columns: 4,
  align: left + horizon,
  fill: (x, y) => if y == 0 { silver } else { none },
  table.header([*Atrybut*], [*Typ*], [*Opis*], [*Przykład*]),
  [*`date`*], [`date`], [Dzień, którego dotyczą wartości], [2021-03-01],
  [*`country`*], [`string`], [Kraj, do którego odnoszą się dane], ["Poland"],
  [*`avg_temperature_c`*], [`float`], [Średnia temperatura w stopniach Celsjusza], [15.2],
  [*`avg_precipitation_mm`*], [`float`], [Średnia ilość opadów w milimetrach], [3.5],
  [*`wdi:rural_population_percent`*], [`float`], [Odsetek ludności wiejskiej (%) według wskaźników WDI], [30.5],
  [*`wdi:fertility_rate`*], [`float`], [Wskaźnik dzietności (liczba urodzeń na kobietę)], [1.8],
  [*`wdi:gdp_per_capita_usd`*], [`float`], [PKB per capita w USD według WDI], [12500.75],
  [*`wdi:mobile_subscriptions_per_100`*], [`float`], [Liczba subskrypcji komórkowych na 100 osób], [98.7],
  [*`wdi:refugee_population_percent`*], [`float`], [Odsetek populacji, który stanowią uchodźcy (%)], [2.1],
  [*`total_streams`*], [`integer`], [Całkowita liczba odtworzeń muzyki w danym kraju i dniu], [12500000],
  [*`genre:pop`*], [`float`], [Popularność gatunku pop (0-1)], [0.32],
  [*`genre:rap`*], [`float`], [Popularność gatunku rap (0-1)], [0.21],
  [*`genre:rock`*], [`float`], [Popularność gatunku rock (0-1)], [0.15],
  [*`genre:urbano_latino`*], [`float`], [Popularność gatunku urbano latino (0-1)], [0.18],
  [*`genre:hip_hop`*], [`float`], [Popularność gatunku hip-hop (0-1)], [0.22],
  [*`genre:trap_latino`*], [`float`], [Popularność gatunku trap latino (0-1)], [0.09],
  [*`genre:reggaeton`*], [`float`], [Popularność gatunku reggaeton (0-1)], [0.14],
  [*`genre:filmi`*], [`float`], [Popularność gatunku filmi (0-1)], [0.08],
  [*`genre:dance_pop`*], [`float`], [Popularność gatunku dance pop (0-1)], [0.12],
  [*`genre:latin_pop`*], [`float`], [Popularność gatunku latin pop (0-1)], [0.095],
  [*`genre:pop_rap`*], [`float`], [Popularność gatunku pop rap (0-1)], [0.11],
  [*`genre:musica_mexicana`*], [`float`], [Popularność gatunku musica mexicana (0-1)], [0.07],
  [*`genre:trap`*], [`float`], [Popularność gatunku trap (0-1)], [0.13],
  [*`genre:modern_rock`*], [`float`], [Popularność gatunku modern rock (0-1)], [0.085],
  [*`genre:classic_rock`*], [`float`], [Popularność gatunku classic rock (0-1)], [0.078],
  [*`genre:alternative_metal`*], [`float`], [Popularność gatunku alternative metal (0-1)], [0.06],
  [*`genre:k_pop`*], [`float`], [Popularność gatunku k-pop (0-1)], [0.175],
  [*`genre:corrido`*], [`float`], [Popularność gatunku corrido (0-1)], [0.05],
  [*`genre:norteno`*], [`float`], [Popularność gatunku norteno (0-1)], [0.045],
  [*`genre:modern_bollywood`*], [`float`], [Popularność gatunku modern Bollywood (0-1)], [0.067],
))

#align(center, table(
  columns: 11,
  align: right + horizon,
  fill: (x, y) => if y == 0 { silver } else { none },
  table.header([*Atrybut*], [*Rekord 1*], [*Rekord 2*], [*Rekord 3*], [*Rekord 4*], [*Rekord 5*], [*Rekord 6*], [*Rekord 7*], [*Rekord 8*], [*Rekord 9*], [*Rekord 10*]),
  [*`date`*], [2021-03-01], [2021-03-02], [2021-03-03], [2021-03-04], [2021-03-05], [2021-03-06], [2021-03-07], [2021-03-08], [2021-03-09], [2021-03-10],
  [*`country`*], ["Poland"], ["Poland"], ["Poland"], ["Poland"], ["Poland"], ["Poland"], ["Poland"], ["Poland"], ["Poland"], ["Poland"],
  [*`avg_temperature_c`*], [15.2], [14.8], [15.5], [16.1], [16.3], [16.7], [17.2], [17.5], [17.8], [18.1],
  [*`avg_precipitation_mm`*], [3.5], [3.2], [3.1], [3.0], [2.9], [2.8], [2.7], [2.6], [2.5], [2.4],
  [*`wdi:rural_`*...], [30.5], [30.5], [30.5], [30.5], [30.5], [30.5], [30.5], [30.5], [30.5], [30.5],
  [*`wdi:fertility_rate`*], [1.8], [1.8], [1.8], [1.8], [1.8], [1.8], [1.8], [1.8], [1.8], [1.8],
  [*`wdi:gdp_per_`*...], [12500], [12500], [12500], [12500], [12500], [12500], [12500], [12500], [12500], [12500],
  [*`wdi:mobile_`*...], [98.7], [98.7], [98.7], [98.7], [98.7], [98.7], [98.7], [98.7], [98.7], [98.7],
  [*`wdi:refugee_`*...], [2.1], [2.1], [2.1], [2.1], [2.1], [2.1], [2.1], [2.1], [2.1], [2.1],
  [*`total_streams`*], [12500000], [10000000], [11000000], [12000000], [13000000], [14000000], [15000000], [16000000], [17000000], [18000000],
  [*`genre:pop`*], [0.32], [0.31], [0.33], [0.34], [0.35], [0.36], [0.37], [0.38], [0.39], [0.24],
  [*`genre:rap`*], [0.21], [0.22], [0.23], [0.24], [0.25], [0.26], [0.27], [0.28], [0.29], [0.30],
  [*`genre:rock`*], [0.15], [0.16], [0.17], [0.18], [0.19], [0.20], [0.21], [0.22], [0.23], [0.24],
  [*`genre:urbano_latino`*], [0.18], [0.19], [0.20], [0.21], [0.22], [0.23], [0.24], [0.25], [0.26], [0.27],
  [*`genre:hip_hop`*], [0.22], [0.23], [0.24], [0.25], [0.26], [0.27], [0.28], [0.29], [0.30], [0.31],
  [*`genre:trap_latino`*], [0.09], [0.10], [0.11], [0.12], [0.13], [0.14], [0.15], [0.16], [0.17], [0.18],
  [*`genre:reggaeton`*], [0.14], [0.15], [0.16], [0.17], [0.18], [0.19], [0.20], [0.21], [0.22], [0.23],
  [*`genre:filmi`*], [0.08], [0.09], [0.10], [0.11], [0.12], [0.13], [0.14], [0.15], [0.16], [0.17],
  [*`genre:dance_pop`*], [0.12], [0.13], [0.14], [0.15], [0.16], [0.17], [0.18], [0.19], [0.20], [0.21],
  [*`genre:latin_pop`*], [0.05], [0.06], [0.07], [0.08], [0.09], [0.10], [0.11], [0.12], [0.13], [0.14],
  [*`genre:pop_rap`*], [0.11], [0.12], [0.13], [0.14], [0.15], [0.16], [0.17], [0.18], [0.19], [0.20],
  [*`genre:musica_`*...], [0.07], [0.08], [0.09], [0.10], [0.11], [0.12], [0.13], [0.14], [0.15], [0.16],
  [*`genre:trap`*], [0.13], [0.14], [0.15], [0.16], [0.17], [0.18], [0.19], [0.20], [0.21], [0.22],
  [*`genre:modern_rock`*], [0.085], [0.095], [0.105], [0.115], [0.125], [0.135], [0.145], [0.155], [0.165], [0.175],
  [*`genre:classic_rock`*], [0.078], [0.088], [0.098], [0.108], [0.118], [0.128], [0.138], [0.148], [0.158], [0.168],
  [*`genre:alternative_`*...], [0.06], [0.07], [0.08], [0.09], [0.10], [0.11], [0.12], [0.13], [0.14], [0.15],
  [*`genre:k_pop`*], [0.175], [0.185], [0.195], [0.205], [0.215], [0.225], [0.235], [0.245], [0.255], [0.265],
  [*`genre:corrido`*], [0.05], [0.055], [0.06], [0.065], [0.07], [0.075], [0.08], [0.085], [0.09], [0.095],
  [*`genre:norteno`*], [0.045], [0.05], [0.055], [0.06], [0.065], [0.07], [0.075], [0.08], [0.085], [0.09],
  [*`genre:modern_`*...], [0.067], [0.077], [0.087], [0.097], [0.107], [0.117], [0.127], [0.137], [0.147], [0.157],

))

// TODO: przykłady
