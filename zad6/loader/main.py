import math
import os
from datetime import datetime
from time import time_ns
import duckdb
from hdfs import InsecureClient

client = InsecureClient("http://10.0.2.3:9870", user="root")

client.delete("/input", recursive=True)
client.makedirs("/input")
client.write("/logs.txt", "", overwrite=True, replication=4, encoding="utf-8")

def log(message: str) -> None:
    message = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}\n"
    print(message, end="", flush=True)
    client.write("/logs.txt", message, append=True, encoding="utf-8")

def upload_input_file(name: str) -> None:
    size = os.path.getsize(f"/root/data/{name}")
    log(f"{name} uploading...")
    start = time_ns()
    try:
        client.upload(f"/input/{name}", f"/root/data/{name}")
    except Exception as e:
        log(f"{name} upload failed !!!")
        log(e)
        return
    end = time_ns()
    try:
        client.set_replication(f"/input/{name}", 4)
    except Exception as e:
        log(f"{name} replication failed !!!")
        log(e)
        return
    log(f"{name} uploaded successfully in {(end - start) / 1_000_000:.3f} ms, {math.ceil(size / (1024 * 1024)):.3f} MB")
    log(f"{name} status: {client.status(f"/input/{name}")}")

def convert_to_parquet(name: str) -> None:
    pq_name = os.path.splitext(name)[0] + ".parquet"
    if os.path.exists(f"/root/data/{pq_name}"): # TODO
        log(f"{name} already converted to parquet, skipping...")
        return
    log(f"{name} converting to parquet...")
    duckdb.sql(f"""COPY (SELECT * FROM '/root/data/{name}') TO '/root/data/{pq_name}' (FORMAT PARQUET)""")
    log(f"{name} converted to parquet successfully")

convert_to_parquet("charts.csv")
convert_to_parquet("cities.csv")
convert_to_parquet("WDIData.csv")

upload_input_file("charts.parquet")
upload_input_file("cities.parquet")
upload_input_file("daily_weather.parquet")
upload_input_file("WDIData.parquet")
