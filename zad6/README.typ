#set par(justify: true)

#align(center)[
#text(size: 20pt)[Zadanie 6 -- Poprawione]

*Treść oryginalnego sprawozdania dostępna poniżej.*
]

Ulepszyliśmy możliwości akwizycji przyrostowej danych na następujące sposoby:

== Porównanie sum kontrolnych

Zamiast usuwać z HDFS wszystkich plików na początku procesu akwizycji, skrypt pobiera z HDFS specjalny plik `/digests` zawierający sumy kontrolne (hash SHA-256) wszystkich plików wejściowych. Następnie przy wgrywaniu konkretnych plików z danymi wejściowymi, skrypt porównuje obliczony skrót pliku lokalnego ze skrótem pliku w HDFS. Jeżeli skróty się zgadzają, przechodzimy do przetwarzania następnego pliku. Jeżeli nie, skrypt usuwa starą wersję pliku w HDFS, wgrywa nową dokonując odpowiednich wpisów w dzienniku oraz aktualizuje plik `/digests`.

#[
#set text(size: 8pt)
```Python
def load_digests() -> None:
    try:
        with client.read("/digests", encoding="utf-8") as f:
            lines = f.read().splitlines()
            for line in lines:
                p, d = line.split(":")
                digests[p] = d
    except:
        pass

def update_digest(path: str, digest: str) -> None:
    digests[path] = digest
    content = "\n".join(f"{p}:{h}" for p, h in digests.items())
    client.write("/digests", content, overwrite=True, encoding="utf-8")

def get_digest(path: str) -> str:
    hasher = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest()
```
```
[2025-05-05 19:06:29] charts_small.csv exists, but is outdated, deleting...
[2025-05-05 19:06:30] charts_small.csv uploading...
[2025-05-05 19:06:30] charts_small.csv uploaded successfully in 611.805 ms, 1.000 MB
[2025-05-05 19:06:30] charts_small.csv status: {'accessTime': 1746471990189, 'blockSize': 134217728, 'childrenNum': 0, 'fileId': 16530, 'group': 'supergroup', 'length': 2849, 'modificationTime': 1746471990765, 'owner': 'root', 'pathSuffix': '', 'permission': '644', 'replication': 3, 'storagePolicy': 0, 'type': 'FILE'}
[2025-05-05 19:06:31] charts_small.csv digest: 27895629d87b31acac1e3541a6824831bb5b47d4e60fe150e59bdc1e7f83281e

[2025-05-05 19:06:36] charts_2017.csv already uploaded and up to date, skipping...
[2025-05-05 19:06:36] daily_weather_2017.csv already uploaded and up to date, skipping...
[2025-05-05 19:06:36] cities.csv already uploaded and up to date, skipping...
[2025-05-05 19:06:38] WDIData.csv already uploaded and up to date, skipping...
```
]

== Pobieranie przyrostowe danych dynamicznych z API

Dla danych dynamicznych (pobieranie ze Spotify API artystów dla utworów oraz gatunków dla artystów), zapisywane są identyfikatory już pobranych zasobów. W momencie ponownego uruchomienia skryptu, jeżeli dany zasób został już pobrany, nie jest on ponownie zażądany od API. W ten sposób zmniejszamy liczbę zapytań do API oraz czas przetwarzania.

#[
#set text(size: 8pt)
```Python
try:
    writer.sql("COPY artists_from_tracks FROM '/root/data/artists_from_tracks.csv' (FORMAT CSV, HEADER TRUE)")
except:
    pass
track_ids = set(row[0] for row in writer.sql("SELECT DISTINCT track_id FROM artists_from_tracks").fetchall())



while (row := cursor.fetchone()) is not None:
    track_id = row[0].split("/")[-1]
    if track_id in track_ids:
        continue
```
]

#pagebreak()

#align(center)[
  #text(size: 20pt, weight: "bold")[Przetwarzanie dużych zbiorów danych (PDZD)]

  Zespół B1 (geopolityka i muzyka): \
  *Tomasz Chojnacki (260365), Kamila Iwańska (253027), Jakub Zehner (260285)*
]

= Zadanie 6 -- Akwizycja danych

== Skrypt do akwizycji

W celu umożliwienia wgrania danych do systemu HDFS, napisano zestaw skryptów Python z wykorzystaniem biblioteki `hdfs`#footnote(link("https://pypi.org/project/hdfs")). Program automatyzuje cały proces akwizycji danych, w tym lokalną konwersję formatu, wgranie do HDFS oraz utrzymanie dziennika zdarzeń. Skrypt na początku usuwa folder `input` w HDFS, a następnie tworzy go na nowo oraz inicjalizuje plik dziennika `logs.txt`. Użycie funkcji `log` zapisuje dane do pliku `logs.txt` w systemie HDFS oraz wypisuje je lokalnie do standardowego wyjścia. Ogólny przebieg akwizycji przedstawia poniższy fragment kodu:
```python
convert_to_parquet("charts.csv")
convert_to_parquet("cities.csv")
convert_to_parquet("WDIData.csv")

fetch_artists_from_tracks()
fetch_genres_from_artists()

upload_input_file("charts.parquet")
upload_input_file("cities.parquet")
upload_input_file("daily_weather.parquet")
upload_input_file("WDIData.parquet")
upload_input_file("artists_from_tracks.parquet")
upload_input_file("genres_from_artists.parquet")

```

=== Dane statyczne

Wszystkie dane statyczne zostały lokalnie przekonwertowane z formatu CSV do Parquet. W tym celu wykorzystano bibliotekę `duckdb`#footnote(link("https://duckdb.org/docs/stable/clients/python/overview.html")). W celu oszczędzenia czasu, jeżeli w systemie plików znajduje się już przekonwertowany plik, nie dokonuje się przetwarzania.

Pliki w formacie Parquet są sekwencyjnie wgrywane do HDFS. Proces wgrywania jest ewidencjonowany w dzienniku wraz z datą, godziną, statusem, czasem trwania, rozmiarem pliku i~ewentualnymi błędami. Dla każdego wgranego pliku ustawiona jest replikacja na poziomie 3.

=== Dane dynamiczne

Dla danych dynamicznych, odpowiednie funkcje wysyłają na podstawie wczytanego poprzednio źródła statycznego `charts.parquet` zapytania HTTP do proxy API i zapisują odpowiedzi w bazie in-memory DuckDB. Po wykonaniu wszystkich zapytań, dane są konwertowane do formatu Parquet i~wgrywane do HDFS. Logi z każdego zapytania (kod statusu HTTP, odpowiedź) są zapisywane w~dzienniku, wraz z postępem wgrywania gotowego pliku.

Takie przetwarzanie jest wykonywane dla dwóch naszych źródeł dynamicznych: uzyskania listy artystów dla utworu oraz uzyskania listy gatunków dla artysty. Dla danych zduplikowanych nie są wysyłane zapytania (kilka utworów tego samego artysty). Odpowiednie ścieżki Spotify API to:

- #link("https://developer.spotify.com/documentation/web-api/reference/get-track", "https://api.spotify.com/v1/tracks/{id}")
- #link("https://developer.spotify.com/documentation/web-api/reference/get-an-artist", "https://api.spotify.com/v1/artists/{id}")

== Dane w systemie

=== Opis plików

#table(
  columns: 4,
  align: horizon + right,
  table.header([*Nazwa pliku*], [*Rozmiar [MB]*], [*Czas wgrywania [ms]*], [*Liczba replik*]),
  [`charts.parquet`], [1040], [8236], [3],
  [`cities.parquet`], [1], [2094], [3],
  [`daily_weather.parquet`], [234], [4800], [3],
  [`WDIData.parquet`], [69], [2439], [3],
  [`artists_from_tracks.parquet`], [1], [626], [3],
  [`genres_from_artists.parquet`], [1], [124], [3]
)

=== HDFS

#image("img/root.png")
#image("img/input.png")
#image("img/nodes.png")

#pagebreak()

=== Dziennik akwizycji

#text(size: 8pt)[
```
[2025-04-28 16:33:31] charts.csv already converted to parquet, skipping...
[2025-04-28 16:33:31] cities.csv already converted to parquet, skipping...
[2025-04-28 16:33:31] WDIData.csv already converted to parquet, skipping...
[2025-04-28 16:33:32] Fetching artists from tracks...
[2025-04-28 16:33:34] 1DbEa84a9aKnM9kzdhsFT1 200 {'id': '1DbEa84a9aKnM9kzdhsFT1', 'artists': [{'id': 'cacf92be8ab2b42b05c97f122c26531c'}]}
[2025-04-28 16:33:34] 10no4DqPt5nYevs5qIL3fM 200 {'id': '10no4DqPt5nYevs5qIL3fM', 'artists': [{'id': '08e4c278b3eb5088547c1fd143a9d42a'}, {'id': '8871cb1ef089b8fb4c7a2b348dfd10dc'}]}
[2025-04-28 16:33:34] 3UPsdBBqs9vnVF0fYTGA8n 200 {'id': '3UPsdBBqs9vnVF0fYTGA8n', 'artists': [{'id': 'f064865ed19fcdbc23356f1eab22f4df'}, {'id': '18f327c0eb1a48bc524d245f39200de0'}, {'id': '627816b99e31f84dca7c64e053f88c00'}]}
[2025-04-28 16:33:34] 10Igtw8bSDyyFs7KIsKngZ 200 {'id': '10Igtw8bSDyyFs7KIsKngZ', 'artists': [{'id': '86517b0d724548d323c8dc0a9397de31'}, {'id': 'ac12821d44454fe129bca637b585ae60'}]}
[2025-04-28 16:33:34] 65NwOZqoXny4JxqAPlfxRF 404
[2025-04-28 16:33:34] 32lm3769IRfcnrQV11LO4E 200 {'id': '32lm3769IRfcnrQV11LO4E', 'artists': [{'id': '9f4f617de5b520095581f84912cc4cc9'}, {'id': '78f90d7c3bccd5de871662d1d54a2db7'}, {'id': 'bc9bcf75104135e4f41655247e43b99a'}]}
[2025-04-28 16:33:34] 0RGUIOZtmOXTWOy5EjvQbP 200 {'id': '0RGUIOZtmOXTWOy5EjvQbP', 'artists': [{'id': '5d37177f3fc58ad4cdaea3dbd249cdb0'}]}
[2025-04-28 16:33:35] 2ekn2ttSfGqwhhate0LSR0 200 {'id': '2ekn2ttSfGqwhhate0LSR0', 'artists': [{'id': '2444914d57fd37fed305eaa75f75f8e9'}]}
[2025-04-28 16:33:35] 2xGjteMU3E1tkEPVFBO08U 200 {'id': '2xGjteMU3E1tkEPVFBO08U', 'artists': [{'id': '5c5472b69824dc41a3ccb93bb10cd6ea'}, {'id': '82c5b7da7ed39dc349122a09966fb938'}]}
[2025-04-28 16:33:35] 3Vo4wInECJQuz9BIBMOu8i 200 {'id': '3Vo4wInECJQuz9BIBMOu8i', 'artists': [{'id': '5da1abae03e5366919646f9b5c2e5c99'}, {'id': 'c3c11aa0c4e265d926b4dab11182fb38'}]}
[2025-04-28 16:33:35] 67XrooSCHpPxHN81XXyLDU 200 {'id': '67XrooSCHpPxHN81XXyLDU', 'artists': [{'id': '411338e0f25ccde378be3188c422e159'}]}
```
]
#align(center)[_*2029 wierszy dalej...*_]
#text(size: 8pt)[
```
[2025-04-28 16:46:01] 5OELUCYgOHKFAvCERnAvfS 200 {'id': '5OELUCYgOHKFAvCERnAvfS', 'artists': [{'id': '0b4e3dbc0a1f079322f88590acdf6a7c'}]}
[2025-04-28 16:46:01] 3hLuHKzG1cmlRpq53ZVWd8 200 {'id': '3hLuHKzG1cmlRpq53ZVWd8', 'artists': [{'id': 'abb884f6958a1bdfaea8dd48574571d8'}]}
[2025-04-28 16:46:01] 6IAqflHsPVm4EpYghXauX7 200 {'id': '6IAqflHsPVm4EpYghXauX7', 'artists': [{'id': '91ab481365a4dca3ea68e674b14aff19'}]}
[2025-04-28 16:46:01] 6jcG3yZ0e2CSI6omkYO1ut 200 {'id': '6jcG3yZ0e2CSI6omkYO1ut', 'artists': [{'id': 'b7526dab87be0ed16c9f86d332085681'}, {'id': '1612eea3a56cf5a183f8b5fba78980f1'}]}
[2025-04-28 16:46:01] 65uxT4ZGe6eOABj1g5V2Fj 200 {'id': '65uxT4ZGe6eOABj1g5V2Fj', 'artists': [{'id': 'fb9db19103b06c4579bd13516a151a65'}]}
[2025-04-28 16:46:01] 4RZJObXQzsKQLd25LBjBG8 200 {'id': '4RZJObXQzsKQLd25LBjBG8', 'artists': [{'id': '217eee33215b2da498c7648f647d1c70'}, {'id': 'e4c0dff2fe75cd73dc5c2db3e8e9e4c2'}]}
[2025-04-28 16:46:01] 1UHcnP31sAKOJFrEnyRNUz 404
[2025-04-28 16:46:02] 7iKDsPfLT0d5mu2htfMKBZ 200 {'id': '7iKDsPfLT0d5mu2htfMKBZ', 'artists': [{'id': '0d055beb31fd94ce0399a1981d82afc5'}]}
[2025-04-28 16:46:02] 2IY537C2ecmUMJ46bYQggp 200 {'id': '2IY537C2ecmUMJ46bYQggp', 'artists': [{'id': 'fe89720f1f997e90e7e90144e9786e9e'}]}
[2025-04-28 16:46:02] Artists from tracks fetched successfully
[2025-04-28 16:46:02] Fetching genres from artists...
[2025-04-28 16:46:02] f064865ed19fcdbc23356f1eab22f4df 200 {'id': 'f064865ed19fcdbc23356f1eab22f4df', 'genres': ['dutch hip hop', 'uk dancehall', 'belgian dance', 'belgian edm', 'hiplife', 'ghanaian hip hop', 'dutch pop', 'dutch trap', 'pop urbaine', 'nigerian pop', 'big room', 'francoton', 'uk hip hop', 'electro house', 'edm', 'french hip hop', 'melodic drill', 'dance pop', 'turkish trap', 'basshall', 'surinamese pop', 'dutch rock', 'ghanaian pop', 'azontobeats', 'progressive electro house', 'pop dance', 'tropical house', 'afroswing', 'azonto', 'afropop', 'antilliaanse rap', 'dutch rap pop', 'rap francais', 'afro dancehall', 'swedish dancehall']}
[2025-04-28 16:46:03] 9a7f503e143392edde7836acda016feb 200 {'id': '9a7f503e143392edde7836acda016feb', 'genres': ['arabic hip hop', 'dominican pop', 'dembow', 'colombian pop', 'puerto rican pop', 'spanish pop', 'trap latino', 'romanian pop', 'trap argentino', 'pop urbaine', 'trap', 'miami hip hop', 'electro latino', 'francoton', 'trap triste', 'cubaton', 'slap house', 'latin hip hop', 'pop reggaeton', 'pop rap', 'panamanian pop', 'french hip hop', 'rap', 'dance pop', 'urbano latino', 'melodic rap', 'argentine hip hop', 'boy band', 'latin pop', 'modern salsa', 'reggaeton colombiano', 'pop venezolano', 'salsa puertorriquena', 'rap dominicano', 'salsa peruana', 'reggaeton flow', 'salsa', 'reggae fusion', 'trap boricua', 'pop', 'latin viral pop', 'perreo', 'latin arena pop', 'rap francais', 'mexican pop', 'tropical', 'rap latina', 'urban contemporary', 'reggaeton', 'romanian house']}
[2025-04-28 16:46:03] 91dfa9ac16b91122ff53cf497277e141 200 {'id': '91dfa9ac16b91122ff53cf497277e141', 'genres': ['dancehall', 'jamaican dancehall']}
[2025-04-28 16:46:04] e1d487384fc1b97718d9c15585a47469 200 {'id': 'e1d487384fc1b97718d9c15585a47469', 'genres': ['german dance', 'pop rock', 'edm', 'pop', 'neo mellow', 'belgian edm', 'pop dance', 'tropical house', 'deep house', 'deep euro house', 'dance pop']}
[2025-04-28 16:46:04] cc002e5a09d5538ec65b477fcf148471 200 {'id': 'cc002e5a09d5538ec65b477fcf148471', 'genres': ['eurovision', 'russelater', 'norwegian pop']}
[2025-04-28 16:46:04] 15e64332fa6467da336344522e75dcce 200 {'id': '15e64332fa6467da336344522e75dcce', 'genres': ['arabic hip hop', 'indie liguria', 'italian pop', 'ghanaian hip hop', 'italian hip hop', 'drill italiana', 'rap algerien', 'grime', 'pop urbaine', 'nigerian pop', 'francoton', 'uk hip hop', 'r&b italiano', 'french hip hop', 'albanian pop', 'italian underground hip hop', 'albanian hip hop', 'rap sardegna', 'rap siciliano', 'trap italiana', 'rap napoletano', 'rap tunisien', 'afropop', 'milan indie', 'rap francais', 'afro dancehall', 'italian indie pop', 'swedish dancehall', 'rap genovese', 'greek trap']}
[2025-04-28 16:46:05] b7f47cd199037387f450c1ade8993e5e 200 {'id': 'b7f47cd199037387f450c1ade8993e5e', 'genres': ['dutch hip hop', 'dutch rap pop', 'dutch pop']}
[2025-04-28 16:46:05] 433eb53c76e79391c97d86f64cb79307 200 {'id': '433eb53c76e79391c97d86f64cb79307', 'genres': ['pop rock', 'la indie', 'modern rock', 'rock', 'modern alternative rock']}
```
]
#align(center)[_*173 wiersze dalej...*_]
#text(size: 8pt)[
```
[2025-04-28 16:47:16] b6727c8631dde7eede2e876d681bc059 200 {'id': 'b6727c8631dde7eede2e876d681bc059', 'genres': ['dutch hip hop', 'polish hip hop', 'polish pop', 'polish alternative', 'french hip hop', 'rap conscient', 'polish alternative rap', 'polish trap', 'pop urbaine', 'polish alternative rock']}
[2025-04-28 16:47:17] 3d0402e17d73e3e4a255c10e54110f1f 200 {'id': '3d0402e17d73e3e4a255c10e54110f1f', 'genres': ['dutch hip hop', 'ukrainian indie', 'polish hip hop', 'rap montrealais', 'polish pop', 'russian alt pop', 'canadian hip hop', 'melodic rap', 'polish alternative rap', 'polish trap', 'polish electronica', 'toronto rap', 'trap']}
[2025-04-28 16:47:17] f8a26e0e1652fd7a4dbf50432462aeee 200 {'id': 'f8a26e0e1652fd7a4dbf50432462aeee', 'genres': ['polish hip hop', 'polish pop', 'polish alternative rap']}
[2025-04-28 16:47:17] 241be088c20e5eff54a29b437fdb1686 200 {'id': '241be088c20e5eff54a29b437fdb1686', 'genres': ['trap soul', 'grime', 'afroswing', 'uk hip hop']}
[2025-04-28 16:47:17] Genres from artists fetched successfully
[2025-04-28 16:47:17] charts.parquet uploading...
[2025-04-28 16:47:26] charts.parquet uploaded successfully in 8236.180 ms, 1040.000 MB
[2025-04-28 16:47:27] charts.parquet status: {'accessTime': 1745858839317, 'blockSize': 134217728, 'childrenNum': 0, 'fileId': 16394, 'group': 'supergroup', 'length': 1089985677, 'modificationTime': 1745858846637, 'owner': 'root', 'pathSuffix': '', 'permission': '644', 'replication': 3, 'storagePolicy': 0, 'type': 'FILE'}
[2025-04-28 16:47:27] cities.parquet uploading...
[2025-04-28 16:47:30] cities.parquet uploaded successfully in 2094.967 ms, 1.000 MB
[2025-04-28 16:47:30] cities.parquet status: {'accessTime': 1745858849467, 'blockSize': 134217728, 'childrenNum': 0, 'fileId': 16395, 'group': 'supergroup', 'length': 69376, 'modificationTime': 1745858850139, 'owner': 'root', 'pathSuffix': '', 'permission': '644', 'replication': 3, 'storagePolicy': 0, 'type': 'FILE'}
[2025-04-28 16:47:30] daily_weather.parquet uploading...
[2025-04-28 16:47:35] daily_weather.parquet uploaded successfully in 4800.690 ms, 234.000 MB
[2025-04-28 16:47:35] daily_weather.parquet status: {'accessTime': 1745858850767, 'blockSize': 134217728, 'childrenNum': 0, 'fileId': 16396, 'group': 'supergroup', 'length': 244403425, 'modificationTime': 1745858855518, 'owner': 'root', 'pathSuffix': '', 'permission': '644', 'replication': 3, 'storagePolicy': 0, 'type': 'FILE'}
[2025-04-28 16:47:35] WDIData.parquet uploading...
[2025-04-28 16:47:38] WDIData.parquet uploaded successfully in 2439.374 ms, 69.000 MB
[2025-04-28 16:47:39] WDIData.parquet status: {'accessTime': 1745858857336, 'blockSize': 134217728, 'childrenNum': 0, 'fileId': 16397, 'group': 'supergroup', 'length': 71474647, 'modificationTime': 1745858858495, 'owner': 'root', 'pathSuffix': '', 'permission': '644', 'replication': 3, 'storagePolicy': 0, 'type': 'FILE'}
[2025-04-28 16:47:39] artists_from_tracks.parquet uploading...
[2025-04-28 16:47:40] artists_from_tracks.parquet uploaded successfully in 626.549 ms, 1.000 MB
[2025-04-28 16:47:41] artists_from_tracks.parquet status: {'accessTime': 1745858860224, 'blockSize': 134217728, 'childrenNum': 0, 'fileId': 16398, 'group': 'supergroup', 'length': 83210, 'modificationTime': 1745858860724, 'owner': 'root', 'pathSuffix': '', 'permission': '644', 'replication': 3, 'storagePolicy': 0, 'type': 'FILE'}
[2025-04-28 16:47:41] genres_from_artists.parquet uploading...
[2025-04-28 16:47:42] genres_from_artists.parquet uploaded successfully in 124.181 ms, 1.000 MB
[2025-04-28 16:47:42] genres_from_artists.parquet status: {'accessTime': 1745858862096, 'blockSize': 134217728, 'childrenNum': 0, 'fileId': 16399, 'group': 'supergroup', 'length': 20371, 'modificationTime': 1745858862190, 'owner': 'root', 'pathSuffix': '', 'permission': '644', 'replication': 3, 'storagePolicy': 0, 'type': 'FILE'}
```
]
