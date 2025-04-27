from hdfs import InsecureClient

client = InsecureClient("http://10.0.2.3:9870", user="root")
client.download("/logs.txt", "/root/loader/logs.txt", overwrite=True)
