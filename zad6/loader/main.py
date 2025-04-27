import math
import os
from datetime import datetime
from time import time_ns
import duckdb
from hdfs import InsecureClient
import requests

client = InsecureClient("http://10.0.2.3:9870", user="root")

client.delete("/input", recursive=True)
client.makedirs("/input")
client.write("/logs.txt", "", overwrite=True, encoding="utf-8")

def log(message: str, *, file_only: bool = False) -> None:
    message = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}\n"
    if not file_only:
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
        client.set_replication(f"/input/{name}", 3)
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

def fetch_artists_from_tracks() -> None:
    log("Fetching artists from tracks...")
    duckdb.sql("CREATE TABLE IF NOT EXISTS artists_from_tracks (track_id VARCHAR, artist_id VARCHAR)")
    duckdb.sql("TRUNCATE artists_from_tracks")
    cursor = duckdb.sql("SELECT DISTINCT url FROM '/root/data/charts.parquet'")
    while True:
        row = cursor.fetchone()
        if row is None:
            break
        track_id = row[0].split("/")[-1]
        response = requests.get(f"http://localhost:1234/v1/tracks/{track_id}")
        if response.status_code != 200:
            log(f"{track_id} {response.status_code}", file_only=True)
            continue
        data = response.json()
        log(f"{track_id} {response.status_code} {data}", file_only=True)
        for artist in data["artists"]:
            duckdb.sql(f"INSERT INTO artists_from_tracks VALUES (?, ?)", params=(track_id, artist["id"]))
    duckdb.sql("COPY artists_from_tracks TO '/root/data/artists_from_tracks.parquet' (FORMAT PARQUET)")
    duckdb.sql("DROP TABLE artists_from_tracks")
    log("Artists from tracks fetched successfully")

def fetch_genres_from_artists() -> None:
    log("Fetching genres from artists...")
    duckdb.sql("CREATE TABLE IF NOT EXISTS genres_from_artists (artist_id VARCHAR, genre VARCHAR)")
    duckdb.sql("TRUNCATE genres_from_artists")
    cursor = duckdb.sql("SELECT DISTINCT artist_id FROM '/root/data/artists_from_tracks.parquet'")
    while True:
        row = cursor.fetchone()
        if row is None:
            break
        artist_id = row[0]
        response = requests.get(f"http://localhost:1234/v1/artists/{artist_id}")
        if response.status_code != 200:
            log(f"{artist_id} {response.status_code}", file_only=True)
            continue
        data = response.json()
        log(f"{artist_id} {response.status_code} {data}", file_only=True)
        for genre in data["genres"]:
            duckdb.sql(f"INSERT INTO genres_from_artists VALUES (?, ?)", params=(artist_id, genre))
    duckdb.sql("COPY genres_from_artists TO '/root/data/genres_from_artists.parquet' (FORMAT PARQUET)")
    duckdb.sql("DROP TABLE genres_from_artists")
    log("Genres from artists fetched successfully")

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
