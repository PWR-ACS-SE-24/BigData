#set par(justify: true)

#align(center)[
  #text(size: 20pt, weight: "bold")[Przetwarzanie dużych zbiorów danych (PDZD)]

  Zespół B1 (geopolityka i muzyka): \
  *Tomasz Chojnacki (260365), Kamila Iwańska (253027), Jakub Zehner (260285)*
]

= Zadanie 7 -- Map-Reduce

== Wybrane procesy

Poniżej przedstawiono 3 procesy, które wybraliśmy do analizy:

#image("./img/pdzd.drawio.png")

== Proces 1 -- `ChartsFmt`

== Proces 2 -- `ChartsDailySum`

== Proces 3 -- `DailyCountryWeather`

== Benchmarki

#pagebreak()
#set page(flipped: true)

== Przykłady

=== Proces 1 -- `ChartsFmt`

==== Wejście `charts_small.csv`

#[
#set text(size: 7pt)
```
title,rank,date,artist,url,region,chart,trend,streams
Chantaje (feat. Maluma),1,2017-01-01,Shakira,https://open.spotify.com/track/6mICuAdrwEjh6Y6lroV2Kg,Argentina,top200,SAME_POSITION,253019
Vente Pa' Ca (feat. Maluma),2,2017-01-01,Ricky Martin,https://open.spotify.com/track/7DM4BPaS7uofFul3ywMe46,Argentina,top200,MOVE_UP,223988
Reggaetón Lento (Bailemos),3,2017-01-01,CNCO,https://open.spotify.com/track/3AEZUABDXNtecAOSC1qTfo,Argentina,top200,MOVE_DOWN,210943
Safari,4,2017-01-01,"J Balvin, Pharrell Williams, BIA, Sky",https://open.spotify.com/track/6rQSrBHf7HlZjtcMZ4S4bO,Argentina,top200,SAME_POSITION,173865
Shaky Shaky,5,2017-01-01,Daddy Yankee,https://open.spotify.com/track/58IL315gMSTD37DOZPJ2hf,Argentina,top200,MOVE_UP,153956
Traicionera,6,2017-01-01,Sebastian Yatra,https://open.spotify.com/track/5J1c3M4EldCfNxXwrwt8mT,Argentina,top200,MOVE_DOWN,151140
Cuando Se Pone a Bailar,7,2017-01-01,Rombai,https://open.spotify.com/track/1MpKZi1zTXpERKwxmOu1PH,Argentina,top200,MOVE_DOWN,148369
Otra vez (feat. J Balvin),8,2017-01-01,Zion & Lennox,https://open.spotify.com/track/3QwBODjSEzelZyVjxPOHdq,Argentina,top200,MOVE_DOWN,143004
La Bicicleta,9,2017-01-01,"Carlos Vives, Shakira",https://open.spotify.com/track/0sXvAOmXgjR2QUqLK1MltU,Argentina,top200,MOVE_UP,126389
Dile Que Tu Me Quieres,10,2017-01-01,Ozuna,https://open.spotify.com/track/20ZAJdsKB5IGbGj4ilRt2o,Argentina,top200,MOVE_DOWN,112012
Let Me Love You,1,2017-01-01,"DJ Snake, Justin Bieber",https://open.spotify.com/track/4pdPtRcBmOSQDlJ3Fk945m,Poland,top200,SAME_POSITION,26290
Starboy,2,2017-01-01,"The Weeknd, Daft Punk",https://open.spotify.com/track/5aAx2yezTd8zXrkmtKl66Z,Poland,top200,MOVE_UP,25198
Closer,3,2017-01-01,"The Chainsmokers, Halsey",https://open.spotify.com/track/7BKLCZ1jbUBVqRi2FVlTVw,Poland,top200,MOVE_DOWN,24642
Tuesday,4,2017-01-01,"Burak Yeter, Danelle Sandoval",https://open.spotify.com/track/7abpmGpF7PGep2rDU68GBR,Poland,top200,MOVE_UP,24630
Rockabye (feat. Sean Paul & Anne-Marie),5,2017-01-01,Clean Bandit,https://open.spotify.com/track/5knuzwU65gJK7IF5yJsuaW,Poland,top200,MOVE_UP,23163
"I Don’t Wanna Live Forever (Fifty Shades Darker)",6,2017-01-01,"ZAYN, Taylor Swift",https://open.spotify.com/track/3NdDpSvN911VPGivFlV5d0,Poland,top200,MOVE_UP,18841
Treat You Better,7,2017-01-01,Shawn Mendes,https://open.spotify.com/track/4Hf7WnR761jpxPr5D46Bcd,Poland,top200,MOVE_DOWN,18486
One Dance,8,2017-01-01,"Drake, WizKid, Kyla",https://open.spotify.com/track/1xznGGDReH1oQq0xzbwXa3,Poland,top200,MOVE_DOWN,18249
Don't Wanna Know,9,2017-01-01,"Maroon 5, Kendrick Lamar",https://open.spotify.com/track/5MFzQMkrl1FOOng9tq6R9r,Poland,top200,MOVE_UP,18122
Heathens,10,2017-01-01,Twenty One Pilots,https://open.spotify.com/track/6i0V12jOa3mr6uu4WYhUBr,Poland,top200,MOVE_UP,17823
```
]

==== Wyjście `charts_fmt_small.csv`

#[
#set text(size: 7pt)
```
Argentina,2017-01-01,6mICuAdrwEjh6Y6lroV2Kg,253019
Argentina,2017-01-01,7DM4BPaS7uofFul3ywMe46,223988
Argentina,2017-01-01,3AEZUABDXNtecAOSC1qTfo,210943
Argentina,2017-01-01,6rQSrBHf7HlZjtcMZ4S4bO,173865
Argentina,2017-01-01,58IL315gMSTD37DOZPJ2hf,153956
Argentina,2017-01-01,5J1c3M4EldCfNxXwrwt8mT,151140
Argentina,2017-01-01,1MpKZi1zTXpERKwxmOu1PH,148369
Argentina,2017-01-01,3QwBODjSEzelZyVjxPOHdq,143004
Argentina,2017-01-01,0sXvAOmXgjR2QUqLK1MltU,126389
Argentina,2017-01-01,20ZAJdsKB5IGbGj4ilRt2o,112012
Poland,2017-01-01,4pdPtRcBmOSQDlJ3Fk945m,26290
Poland,2017-01-01,5aAx2yezTd8zXrkmtKl66Z,25198
Poland,2017-01-01,7BKLCZ1jbUBVqRi2FVlTVw,24642
Poland,2017-01-01,7abpmGpF7PGep2rDU68GBR,24630
Poland,2017-01-01,5knuzwU65gJK7IF5yJsuaW,23163
Poland,2017-01-01,3NdDpSvN911VPGivFlV5d0,18841
Poland,2017-01-01,4Hf7WnR761jpxPr5D46Bcd,18486
Poland,2017-01-01,1xznGGDReH1oQq0xzbwXa3,18249
Poland,2017-01-01,5MFzQMkrl1FOOng9tq6R9r,18122
Poland,2017-01-01,6i0V12jOa3mr6uu4WYhUBr,17823
```
]

=== Proces 2 -- `ChartsDailySum`

==== Wejście `charts_fmt_small.csv`

#[
```
Argentina,2017-01-01,6mICuAdrwEjh6Y6lroV2Kg,253019
Argentina,2017-01-01,7DM4BPaS7uofFul3ywMe46,223988
Argentina,2017-01-01,3AEZUABDXNtecAOSC1qTfo,210943
Argentina,2017-01-01,6rQSrBHf7HlZjtcMZ4S4bO,173865
Argentina,2017-01-01,58IL315gMSTD37DOZPJ2hf,153956
Argentina,2017-01-01,5J1c3M4EldCfNxXwrwt8mT,151140
Argentina,2017-01-01,1MpKZi1zTXpERKwxmOu1PH,148369
Argentina,2017-01-01,3QwBODjSEzelZyVjxPOHdq,143004
Argentina,2017-01-01,0sXvAOmXgjR2QUqLK1MltU,126389
Argentina,2017-01-01,20ZAJdsKB5IGbGj4ilRt2o,112012
Poland,2017-01-01,4pdPtRcBmOSQDlJ3Fk945m,26290
Poland,2017-01-01,5aAx2yezTd8zXrkmtKl66Z,25198
Poland,2017-01-01,7BKLCZ1jbUBVqRi2FVlTVw,24642
Poland,2017-01-01,7abpmGpF7PGep2rDU68GBR,24630
Poland,2017-01-01,5knuzwU65gJK7IF5yJsuaW,23163
Poland,2017-01-01,3NdDpSvN911VPGivFlV5d0,18841
Poland,2017-01-01,4Hf7WnR761jpxPr5D46Bcd,18486
Poland,2017-01-01,1xznGGDReH1oQq0xzbwXa3,18249
Poland,2017-01-01,5MFzQMkrl1FOOng9tq6R9r,18122
Poland,2017-01-01,6i0V12jOa3mr6uu4WYhUBr,17823
```
]

==== Wyjście `charts_daily_sum_small.csv`

#[
```
Argentina,2017-01-01,1696685
Poland,2017-01-01,215444
```
]

#pagebreak()

=== Proces 3 -- `DailyCountryWeather`

==== Wejście `daily_weather_small.csv`

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

==== Wejście `cities_small.csv`

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

==== Wyjście `daily_country_weather_small.csv`

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
