from hashlib import md5
from fastapi import FastAPI, HTTPException
import duckdb

track_to_artists_dict: dict[str, set[str]] = {}

for track_id, artist in duckdb.sql("""
SELECT track_id, UNNEST(artists) AS artist FROM (
    SELECT
        track_id,
        string_split(MAX(artists)[3:-3], ''', ''') AS artists
    FROM '/root/data/api.csv'
    GROUP BY track_id
)
""").fetchall():
    if track_id not in track_to_artists_dict:
        track_to_artists_dict[track_id] = set()
    track_to_artists_dict[track_id].add(md5(artist.encode()).hexdigest())

artist_to_genres_dict: dict[str, set[str]] = {}
for row in duckdb.sql("""
SELECT artists, artist_genres FROM '/root/data/api.csv'
GROUP BY artists, artist_genres
""").fetchall():
    artists = row[0][2:-2]
    if artists == "":
        artists = []
    else:
        artists = artists.split("', '")
    genres = row[1][2:-2]
    if genres == "":
        genres = []
    else:
        genres = genres.split("', '")
    for artist in artists:
        artist_id = md5(artist.encode()).hexdigest()
        if artist_id not in artist_to_genres_dict:
            artist_to_genres_dict[artist_id] = set()
        for genre in genres:
            artist_to_genres_dict[artist_id].add(genre)

app = FastAPI()

@app.get("/v1/tracks/{track_id}")
def get_track(track_id: str):
    if track_id not in track_to_artists_dict:
        raise HTTPException(status_code=404)
    return {
        "id": track_id,
        "artists": [
            {
                "id": artist_id,
            } for artist_id in track_to_artists_dict[track_id]
        ]
    }

@app.get("/v1/artists/{artist_id}")
def get_artist(artist_id: str):
    if artist_id not in artist_to_genres_dict:
        raise HTTPException(status_code=404)
    return {
        "id": artist_id,
        "genres": [
            genre for genre in artist_to_genres_dict[artist_id]
        ]
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=1234)
