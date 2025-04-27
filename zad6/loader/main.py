from hdfs import InsecureClient
from datetime import datetime

client = InsecureClient("http://10.0.2.3:9870", user="root")

client.delete("/input", recursive=True)
client.makedirs("/input")
client.write("/logs.txt", "", overwrite=True, replication=4, encoding="utf-8")

def log(message: str) -> None:
    message = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}\n"
    print(message, end="", flush=True)
    client.write("/logs.txt", message, append=True, encoding="utf-8")

def upload_input_file(name: str) -> None:
    log(f"{name} uploading...")
    try:
        client.upload(f"/input/{name}", f"/root/data/{name}")
    except Exception as e:
        log(f"{name} upload failed !!!")
        log(e)
        return
    try:
        client.set_replication(f"/input/{name}", 4)
    except Exception as e:
        log(f"{name} replication failed !!!")
        log(e)
        return
    log(f"{name} uploaded successfully")

upload_input_file("cities.csv")
