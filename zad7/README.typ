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

Proces pierwszy jest trywialny i składa się z jednego zadania Map-Reduce bez reducerów. Jest on odpowiedzialny za przetworzenie wstępne pliku `charts_2017.csv`, zawierającego dane o najpopularniejszych utworach muzycznych. Proces dzieli linię CSV na poszczególne kolumny, odfiltrowuje wiersze, dla których `chart != "top200"`, a następnie przekształca je do formatu `region,date,track_id,streams`. Dane są wypisywane do osobnego pliku CSV, który jest następnie używany jako wejście do kolejnych procesów. Mierzony jest czas wykonania całego zadania oraz czas spędzony w mapperze.

#v(1fr)

#image("./img/Wilczur-Proces-1.drawio.png")

#pagebreak()

#[
#set text(size: 8pt)
```java
public static class ChartsFmtMapper extends Mapper<Object, Text, NullWritable, Text> {
  @Override
  protected void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
    long startTime = System.nanoTime();
    String line = value.toString();
    if (line.equals("title,rank,date,artist,url,region,chart,trend,streams")) {
      return;
    }

    String[] fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

    if (!fields[6].equals("top200")) {
      return;
    }

    String output = String.join(",",
      fields[5], // region
      fields[2], // date
      fields[4].substring("https://open.spotify.com/track/".length()), // track_id
      fields[8] // streams
    );

    context.write(NullWritable.get(), new Text(output));
    long endTime = System.nanoTime();
    context.getCounter(Counters.MAPPER).increment(endTime - startTime);
  }
}
```
]

== Proces 2 -- `ChartsDailySum`

Proces drugi jest bardziej złożony. Składa się z jednego mappera i jednego reducera. Ponownie gromadzone są czasy spędzone w różnych częściach zadania. Proces jest odpowiedzialny za pogrupowanie i agregację danych wyjściowych poprzedniego procesu.

Mapper dzieli plik CSV na kolumny, ustawia klucz na złączenie `region` i `date` a wartości na liczbę `streams`. Następnie reducer jest odpowiedzialny za zsumowanie wartości `streams` dla każdego klucza. Wartości są następnie wypisywane do pliku CSV.

#v(1fr)

#image("./img/Wilczur-Proces-2.drawio.png")

#v(1fr)

#[
#set text(size: 8pt)
```java
public static class ChartsDailySumMapper extends Mapper<Object, Text, Text, LongWritable> {
  @Override
  protected void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
    long startTime = System.nanoTime();
    String line = value.toString();
    String[] fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
    String region = fields[0];
    String date = fields[1];
    String streams = fields[3];

    String outKey = region + "!@#" + date;
    long streamsCount = Long.parseLong(streams);
    context.write(new Text(outKey), new LongWritable(streamsCount));
    long endTime = System.nanoTime();
    context.getCounter(Counters.MAPPER).increment(endTime - startTime);
  }
}
```

#pagebreak()

```java
public static class ChartsDailySumReducer extends Reducer<Text, LongWritable, NullWritable, Text> {
  @Override
  protected void reduce(Text key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {
    long startTime = System.nanoTime();
    String[] keyParts = key.toString().split("!@#");
    String region = keyParts[0];
    String date = keyParts[1];

    long sum = 0;
    for (LongWritable value : values) {
      sum += value.get();
    }

    String output = String.join(",",
      region,
      date,
      String.valueOf(sum)
    );

    context.write(NullWritable.get(), new Text(output));
    long endTime = System.nanoTime();
    context.getCounter(Counters.REDUCER).increment(endTime - startTime);
  }
}
```
]

== Proces 3 -- `DailyCountryWeather`

Proces 3 składa się z dwóch zadań Map-Reduce uruchomionych sekwencyjnie (wyjście pierwszego jest wejściem drugiego). Proces jest odpowiedzialny za złączenie danych pogodowych z pliku `daily_weather.csv` z danymi o miastach z pliku `cities.csv`. Dane są następnie grupowane według kraju i daty oraz obliczane są średnie wartości temperatury i opadów.

#image("./img/Wilczur-Proces-3.drawio.png")

=== `DailyCountryWeather1` (2 mappery, 1 reducer)

Pierwsze zadanie odpowiedzialne jest za złączenie tabel. Przyjmuje dwa pliki wejściowe i zwraca jeden wynikowy. Mapper danych pogodowych rozdziela wiersz CSV na kolumny, następnie zwraca jako klucz `station_id` (pole, po którym łączymy tabele) oraz jako wartość pozostałe kolumny wraz ze specjalnym dopiskiem `WEATHER`, który pozwoli w reducerze odróżnić dane z tabeli pogodowej od danych z tabeli miast. Mapper danych o miastach działa analogicznie. Jako klucz zwraca `station_id`, a jako wartość pozostałe kolumny z tabeli miast oraz dopisek `CITY`.

Reducer łączy dane z obu mapperów w jedną linię CSV. Warto zauważyć, że dla każdego klucza zwracane jest kilka wierszy wyjściowych -- jeden dla każdego wiersza z tabeli pogodowej.

#[
#set text(size: 8pt)
```java
public static class DailyCountryWeather1WeatherMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        long startTime = System.nanoTime();
        String line = value.toString();
        if (line.equals("station_id,date,avg_temp_c,precipitation_mm")) {
            return;
        }
        String[] fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        String stationId = fields[0];
        String date = fields[1].substring(0, 10);
        String temperatureC = fields.length >= 3 ? fields[2] : "";
        String precipitationMm = fields.length == 4 ? fields[3] : "";

        if (date.compareTo("2017-01-01") < 0 || date.compareTo("2021-12-31") > 0) {
            return;
        }

        context.write(new Text(stationId), new Text(String.join("!@#", "WEATHER", date, temperatureC, precipitationMm)));
        long endTime = System.nanoTime();
        context.getCounter(Counters.MAPPER_WEATHER).increment(endTime - startTime);
    }
}

public static class DailyCountryWeather1CityMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        long startTime = System.nanoTime();
        String line = value.toString();
        if (line.equals("station_id,city_name,country,state,iso2,iso3,latitude,longitude")) {
            return;
        }
        String[] fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        String stationId = fields[0];
        String country = fields[2];

        context.write(new Text(stationId), new Text(String.join("!@#", "CITY", country)));
        long endTime = System.nanoTime();
        context.getCounter(Counters.MAPPER_CITY).increment(endTime - startTime);
    }
}

public static class DailyCountryWeather1Reducer extends Reducer<Text, Text, NullWritable, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        long startTime = System.nanoTime();
        String stationId = key.toString();
        String country = null;
        ArrayList<String> dates = new ArrayList<>();
        ArrayList<String> temperaturesC = new ArrayList<>();
        ArrayList<String> precipitationsMm = new ArrayList<>();

        for (Text value : values) {
            String[] fields = value.toString().split("!@#");
            if (fields[0].equals("WEATHER")) {
                dates.add(fields[1]);
                temperaturesC.add(fields.length >= 3 ? fields[2] : "");
                precipitationsMm.add(fields.length == 4 ? fields[3] : "");
            } else if (fields[0].equals("CITY")) {
                country = fields[1];
            }
        }

        if (country == null) {
            return;
        }

        for (int i = 0; i < dates.size(); i++) {
            String date = dates.get(i);
            String temperatureC = temperaturesC.get(i);
            String precipitationMm = precipitationsMm.get(i);

            String output = String.join(",",
                    country,
                    date,
                    temperatureC,
                    precipitationMm
            );

            context.write(NullWritable.get(), new Text(output));
        }
        long endTime = System.nanoTime();
        context.getCounter(Counters.REDUCER).increment(endTime - startTime);
    }
}
```
]

=== `DailyCountryWeather2` (1 mapper, 1 reducer)

Drugie zadanie procesu jest odpowiedzialne za pogrupowanie po kraju i dacie oraz obliczenie średnich wartości temperatury i opadów. Mapper dzieli wiersz CSV na kolumny, ustawia klucz na złączenie pól `country` i `date`, a wartości to temperatury i opady. Następnie reducer jest odpowiedzialny za zsumowanie wartości temperatury i opadów dla każdego klucza oraz policzenie średnich. Wartości są następnie wypisywane do pliku CSV.

#[
#set text(size: 8pt)
```java
public static class DailyCountryWeather2Mapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        long startTime = System.nanoTime();
        String line = value.toString();
        String[] fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

        String country = fields[0];
        String date = fields[1];
        String temperatureC = fields.length >= 3 ? fields[2] : "";
        String precipitationMm = fields.length == 4 ? fields[3] : "";

        String outKey = country + "!@#" + date;
        String outValue = temperatureC + "!@#" + precipitationMm;
        context.write(new Text(outKey), new Text(outValue));
        long endTime = System.nanoTime();
        context.getCounter(Counters.MAPPER).increment(endTime - startTime);
    }
}

public static class DailyCountryWeather2Reducer extends Reducer<Text, Text, NullWritable, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        long startTime = System.nanoTime();
        String[] keyParts = key.toString().split("!@#");
        String country = keyParts[0];
        String date = keyParts[1];

        double temperatureSum = 0.0;
        int temperatureCount = 0;
        double precipitationSum = 0.0;
        int precipitationCount = 0;

        for (Text value : values) {
            String[] valueParts = value.toString().split("!@#");
            String temperatureC = valueParts.length >= 1 ? valueParts[0] : "";
            if (!temperatureC.isEmpty()) {
                temperatureSum += Double.parseDouble(temperatureC);
                temperatureCount++;
            }
            String precipitationMm = valueParts.length == 2 ? valueParts[1] : "";
            if (!precipitationMm.isEmpty()) {
                precipitationSum += Double.parseDouble(precipitationMm);
                precipitationCount++;
            }
        }

        if (temperatureCount == 0) {
            return;
        }

        double averageTemperatureC = temperatureSum / temperatureCount;
        double averagePrecipitationMm = precipitationCount > 0 ? precipitationSum / precipitationCount : 0.0;

        String output = String.join(",",
                country,
                date,
                String.format("%.2f", averageTemperatureC),
                String.format("%.2f", averagePrecipitationMm)
        );
        context.write(NullWritable.get(), new Text(output));
        long endTime = System.nanoTime();
        context.getCounter(Counters.REDUCER).increment(endTime - startTime);
    }
}
```
]

== Benchmarki

Dokonaliśmy pomiaru czasu wykonania każdego procesu oraz czasu spędzonego w mapperach i~reducerach w zależności od parametrów. Jako parametry do porównania przyjęliśmy:

- `reducers` -- liczba reducerów (1, 2, 3);
- `replication` -- liczba replikacji plików wejściowych (1, 2, 3);
- `splitMb` -- rozmiar podziału pliku wejściowego w MB (128, 192, 256).

Zdecydowaliśmy się nie przyjmować liczby mapperów jako parametru, ponieważ wartość ta jest jedynie sugestią dla silnika i może zostać przez niego zignorowana (wg dokumentacji#footnote(link("https://hadoop.apache.org/docs/current/api/org/apache/hadoop/mapred/JobConf.html#setNumMapTasks-int-"))). Rzeczywista liczba mapperów zależy od liczby podziałów pliku wejściowego (`InputSplits`) i łącznego rozmiaru plików wejściowych. Jako że na ten drugi współczynnik nie mamy wpływu, zdecydowaliśmy się dokonać pomiaru w zależności od rozmiaru input split.

Jako baseline użyliśmy konfiguracji: 1 reducer, 3 replikacje, 128 MB podziału.

Wszystkie czasy podane są w *milisekundach*, zaokrąglone do całości.

#[
#set text(size: 9pt)
#let g(body) = text(fill: rgb("#58cf39"))[#body]
#let r(body) = text(fill: rgb("#FF4136"))[#body]
#align(center, table(
  columns: 10,
  align: right + horizon,
  table.header(table.cell(rowspan: 2)[*Część procesu*], table.cell(colspan: 3)[*Reducery*], table.cell(colspan: 3)[*Repliki*], table.cell(colspan: 3)[*Podział [MB]*], [*`1`*], [*`2`*], [*`3`*], [*`3`*], [*`2`*], [*`1`*], [*`128`*], [*`192`*], [*`256`*]),
  [*`ChartsFmtJob`*], [42010], g[36330], r[42388], [42010], g[38410], g[37315], [42010], r[49879], r[45850],
  [`ChartsFmtMapper`], [80752], g[68190], r[92046], [80752], g[77898], g[70990], [80752], r[105406], r[99304],
  [*`ChartsDailySumJob`*], [27605], g[25733], r[29237], [27605], g[25700], r[28066], [27605], r[31406], g[26581],
  [`ChartsDailySumMapper`], [24509], g[23329], r[32315], [24509], g[24086], g[23626], [24509], r[28582], r[25345],
  [`ChartsDailySumReducer`], [1330], r[1717], r[3107], [1330], g[1235], g[1216], [1330], g[1147], g[1323],
  [*`DailyCountryWeather1Job`*], [19245], g[17634], r[21685], [19245], g[17937], g[18240], [19245], r[20043], r[19628],
  [`DailyCountryWeather1WeatherMapper`], [1035], r[1103], r[1203], [1035], r[1068], r[1196], [1035], r[1122], r[1151],
  [`DailyCountryWeather1CityMapper`], [54], g[47], r[63], [54], r[55], g[46], [54], r[65], r[67],
  [`DailyCountryWeather1Reducer`], [759], r[1288], r[2199], [759], r[858], g[727], [759], r[770], r[1003],
  [*`DailyCountryWeather2Job`*], [20678], r[21192], g[20456], [20678], g[18789], g[20608], [20678], r[21029], g[19024],
  [`DailyCountryWeather2Mapper`], [894], r[1265], r[2158], [894], r[930], g[884], [894], g[867], g[865],
  [`DailyCountryWeather2Reducer`], [821], r[1473], r[2829], [821], g[778], g[804], [821], g[755], r[859],
))
]

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
