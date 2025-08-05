
import pandas as pd
from kafka import KafkaProducer
import json
from time import sleep

#start zookeeper 
#zookeeper-server-start.sh ./config/zookeeper.properties

#start kafka server
#kafka-server-start.sh ./config/server.properties
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

topic_name = "DevOps"

with open("./Big_Data/Source/DevOps.json", "r") as file:
    data = json.load(file)

    # Loop infinitely, sending data every 2 hours
    while True:
        # Send each row as a JSON message
        for row in data:
            producer.send(topic=topic_name, value=row)
            print("Sending...", row)

        sleep(7200)  # Pause before next send cycle