#set par(justify: true)
#set page(flipped: true)

#align(center)[
  #text(size: 20pt, weight: "bold")[Przetwarzanie dużych zbiorów danych (PDZD)]

  Zespół B1 (geopolityka i muzyka): \
  *Tomasz Chojnacki (260365), Kamila Iwańska (253027), Jakub Zehner (260285)*
]

= Zadanie 5 -- Hadoop

1. Nie była wymagana instalacja Docker Desktop, Git, podsystemu Linux, Ubuntu ani Terminala Windows z uwagi na to, że wszystkie powyższe elementy były już skonfigurowane na opisywanej maszynie. Zostało to potwierdzone na poniższych zrzutach ekranu.

#align(center, image("img/docker-git-cmd.png", width: 50%))
#align(center, image("img/docker-empty.png", width:  65%))

#pagebreak()

2. Zgodnie z instrukcją, pobrano skrypty uruchomieniowe. Następnie skryptowi `compose-up.sh` nadano stałe wartości parametrów:
```bash
hadoop_version="3.3.0"
slaves="3"  # TRZY WĘZŁY ROBOCZE
hdfs_path="/tmp/hadoop"
hadoop_log_path="/tmp/hadoop_logs"
hbase_log_path="/tmp/hbase_logs"
hive_log_path="/tmp/hive_logs"
sqoop_log_path="/tmp/sqoop_logs"
maria_root_password="mariadb"
maria_data_path="./maria_data"
```
Wynik uruchomienia skryptu `compose-up.sh` został przedstawiony poniżej:

#align(center, image("img/compose-up.png", width: 85%))

#pagebreak()

3. Następnie uruchomiono skrypt `hadoop-start.sh`, który uruchomił wszystkie usługi. Wynik jego wykonania został przedstawiony poniżej:

#align(center, grid(columns: 2,
  image("img/hadoop-start-1.png", height: 35%),
  image("img/hadoop-start-2.png", height: 35%)
))

Uruchomienie kontenerów potwierdzono w aplikacji Docker Desktop:

#align(center, image("img/docker-with-nodes.png", width: 60%))

#pagebreak()

Działanie usług potwierdzono poprzez wejście w ich panele internetowe, odpowiednio *Hadoop*, *HDFS* oraz panel *węzła*:
#align(center, grid(columns: 2, rows: 2,
  image("img/hadoop-dashboard.png", width: 100%),
  image("img/hdfs-dashboard.png", width: 100%),
  image("img/node-info.png", width: 100%),
))

#pagebreak()

4. Pełne działanie Hadoop potwierdzono uruchamiając gotowy przykład operacji MapReduce estymujący wartość liczby $pi$:

#grid(columns: 2,
  image("img/map-reduce-1.png", width: 100%),
  image("img/map-reduce-2.png", width: 100%)
)
