#set par(justify: true)
#set page(flipped: true)

#align(center)[
  #text(size: 20pt, weight: "bold")[Przetwarzanie dużych zbiorów danych (PDZD)]

  Zespół B1 (geopolityka i muzyka): \
  *Tomasz Chojnacki (260365), Kamila Iwańska (253027), Jakub Zehner (260285)*
]

= Zadanie 8 -- Instalacja HIVE

== 1. Instalacja HIVE

Nie było potrzeby osobnej instalacji HIVE, ponieważ został on zainstalowany wraz z systemem Hadoop na liście 5. Niestety, nie udało się uruchomić HIVE poprzez skrypt `hive-start.sh`, z uwagi na poruszony na poprzednich zajęciach projektowych błąd ze schematami, powodujący crash serwisu `mariadb`. Aby naprawić problem, dokonano ręcznej instalacji bazy MariaDB za pomocą następującego polecenia:

```bash
docker exec -it mariadb bash -c "mariadb-install-db --user=mysql --basedir=/usr --datadir=/var/lib/mysql"
```

Dalsze kroki udało się wykonać bezproblemowo.

== 2. Uruchomienie HIVE

W celu włączenia HIVE, uruchomiono zapewniony skrypt:

#align(center, image("./img/iu-1.png", width: 90%))
#align(center, image("./img/iu-2.png", width: 80%))

Przy uruchomieniu należało podać hasło `mariadb`.

Aby połączyć się z bazą danych utworzono skrypt `beeline.sh`, o następującej treści:
```bash
#!/bin/env bash
docker exec -it master bash -c "beeline -u 'jdbc:hive2://localhost:10000 hive org.apache.hive.jdbc.HiveDriver'"
```

Poniżej przedstawiono uruchomienie skryptu `beeline.sh`:
#align(center, image("./img/beeline.png", width: 95%))

== 3. Przykładowe zadanie

Aby przetestować działanie HIVE, wykonano przykładowe zapytanie, które zostało uruchomione na silniku wykonawczym Map-Reduce. W celu przygotowania do wykonania zapytania użyto następujących poleceń:
```sql
create database data;
use data;
create table customer (name string);
```

Następnie wykonano zapytanie:
#align(center, image("./img/insert.png", width: 95%))

W celu sprawdzenia, czy zapytanie zostało wykonane poprawnie, użyto polecenia `SELECT`, przejrzano logi HIVE oraz panel Hadoop:
#align(center, image("./img/select.png", width: 50%))
#align(center, image("./img/logs.png", width: 50%))
#align(center, image("./img/hadoop.png", width: 50%))
