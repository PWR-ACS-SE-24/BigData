import math
import os
import hashlib
from datetime import datetime
from time import time_ns
import duckdb
from hdfs import InsecureClient
import requests

client = InsecureClient("http://10.0.2.3:9870", user="root")
digests: dict[str, str] = {}

def initialize() -> None:
    client.makedirs("/input")
    client.write("/logs.txt", "", overwrite=True, encoding="utf-8")

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

def log(message: str, *, file_only: bool = False) -> None:
    message = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}\n"
    if not file_only:
        print(message, end="", flush=True)
    client.write("/logs.txt", message, append=True, encoding="utf-8")

def upload_input_file(name: str) -> None:
    digest = get_digest(f"/root/data/{name}")
    if name in digests and digests[name] == digest:
        log(f"{name} already uploaded and up to date, skipping...")
        return
    size = os.path.getsize(f"/root/data/{name}")
    if client.status(f"/input/{name}", strict=False) is not None:
        log(f"{name} exists, but is outdated, deleting...")
        client.delete(f"/input/{name}")
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
    update_digest(name, digest)
    log(f"{name} digest: {digest}")

def fetch_artists_from_tracks(charts_type: str) -> None:
    log("Fetching artists from tracks...")
    with duckdb.connect() as reader, duckdb.connect() as writer:
        writer.sql("CREATE TABLE IF NOT EXISTS artists_from_tracks (track_id VARCHAR, artist_id VARCHAR)")
        writer.sql("TRUNCATE artists_from_tracks")
        try:
            writer.sql("COPY artists_from_tracks FROM '/root/data/artists_from_tracks.csv' (FORMAT CSV, HEADER TRUE)")
        except:
            pass
        track_ids = set(row[0] for row in writer.sql("SELECT DISTINCT track_id FROM artists_from_tracks").fetchall())
        cursor = reader.sql(f"SELECT DISTINCT url FROM '/root/data/{charts_type}.csv'")
        while (row := cursor.fetchone()) is not None:
            track_id = row[0].split("/")[-1]
            if track_id in track_ids:
                continue
            response = requests.get(f"http://localhost:1234/v1/tracks/{track_id}")
            if response.status_code != 200:
                log(f"{track_id} {response.status_code}", file_only=True)
                continue
            data = response.json()
            log(f"{track_id} {response.status_code} {data}", file_only=True)
            for artist in data["artists"]:
                writer.sql("INSERT INTO artists_from_tracks VALUES (?, ?)", params=(track_id, artist["id"]))
        writer.sql("COPY artists_from_tracks TO '/root/data/artists_from_tracks.csv' (FORMAT CSV, HEADER TRUE)")
        writer.sql("DROP TABLE artists_from_tracks")
        log("Artists from tracks fetched successfully")

def fetch_genres_from_artists() -> None:
    log("Fetching genres from artists...")
    with duckdb.connect() as reader, duckdb.connect() as writer:
        writer.sql("CREATE TABLE IF NOT EXISTS genres_from_artists (artist_id VARCHAR, genre VARCHAR)")
        writer.sql("TRUNCATE genres_from_artists")
        try:
            writer.sql("COPY genres_from_artists FROM '/root/data/genres_from_artists.csv' (FORMAT CSV, HEADER TRUE)")
        except:
            pass
        artist_ids = set(row[0] for row in writer.sql("SELECT DISTINCT artist_id FROM genres_from_artists").fetchall())
        cursor = reader.sql("SELECT DISTINCT artist_id FROM '/root/data/artists_from_tracks.csv'")
        while (row := cursor.fetchone()) is not None:
            artist_id = row[0]
            if artist_id in artist_ids:
                continue
            response = requests.get(f"http://localhost:1234/v1/artists/{artist_id}")
            if response.status_code != 200:
                log(f"{artist_id} {response.status_code}", file_only=True)
                continue
            data = response.json()
            log(f"{artist_id} {response.status_code} {data}", file_only=True)
            for genre in data["genres"]:
                writer.sql("INSERT INTO genres_from_artists VALUES (?, ?)", params=(artist_id, genre))
        writer.sql("COPY genres_from_artists TO '/root/data/genres_from_artists.csv' (FORMAT CSV, HEADER TRUE)")
        writer.sql("DROP TABLE genres_from_artists")
        log("Genres from artists fetched successfully")

if __name__ == "__main__":
    initialize()
    load_digests()
    log(f"Digests: {digests}")

    upload_input_file("charts_small.csv")
    upload_input_file("charts_2017.csv")
    upload_input_file("daily_weather_small.csv")
    upload_input_file("daily_weather_2017.csv")

    upload_input_file("charts.csv")
    upload_input_file("daily_weather.csv")
    upload_input_file("cities.csv")
    upload_input_file("WDIData.csv")

    # fetch_artists_from_tracks("charts_2017")
    # upload_input_file("artists_from_tracks.csv")

    # fetch_genres_from_artists()
    # upload_input_file("genres_from_artists.csv")
