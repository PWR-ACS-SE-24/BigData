#set par(justify: true)
#set page(flipped: true)

#align(center)[
  #text(size: 20pt, weight: "bold")[Przetwarzanie dużych zbiorów danych (PDZD)]

  Zespół B1 (geopolityka i muzyka): \
  *Tomasz Chojnacki (260365), Kamila Iwańska (253027), Jakub Zehner (260285)*
]

= Zadanie 10 -- Instalacja Spark

== 1. Uruchomienie Spark

Aby uruchomić Spark na platformie Yarn, wykorzystano instrukcje z wykładu#footnote(link("https://hjben.github.io/spark-cluster")). Wyedytowano plik `compose-up.sh`, aby ustawić odpowiednie parametry:
```bash
hadoop_version="3.3.5"
spark_version="3.4.0"
slaves="3"
jupyter_workspace_path="/tmp/spark-notebook"
hdfs_path="/tmp/hdfs"
hadoop_log_path="/tmp/hadoop-logs"
spark_log_path="/tmp/spark-logs"
```

Następnie, Spark uruchamiamy komendą `./compose-up.sh`:
#align(center, image("img/cu.png", width: 32%))

Następnie, uruchamiamy Jupyter Notebook, za pomocą `./jupyer.sh` i tworzymy sesję Spark poniższym kodem:
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("TestApp").master("yarn")\
    .config("spark.cores.max", "4").config("spark.sql.shuffle.partitions", "5").getOrCreate()
```
#align(center, image("img/jupytersh.png", width: 55%))
#align(center, image("img/jupyterspark.png", width: 65%))

Działanie Spark widzimy w panelu Hadoop:
#align(center, image("img/hadoop.png", width: 65%))

Następnie korzystając z wykorzystywanego wcześniej skryptu ładujemy dane do HDFS oraz uruchamiamy przykładowe zadanie:

#align(center, image("img/count.png", width: 40%))
#align(center, image("img/spark.png", width: 90%))
