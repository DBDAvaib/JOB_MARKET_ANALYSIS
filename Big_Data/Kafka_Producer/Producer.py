
import pandas as pd
from kafka import KafkaProducer
import json
from time import sleep

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

topic_name = "New_Job_Postings"

# Loop infinitely, sending data every 100 seconds
while True:
    # Read the CSV into a DataFrame
    df = pd.read_csv("/home/sunbeam/Desktop/JOB_MARKET_ANALYSIS/JOB_MARKET_ANALYSIS/Big_Data/Source/Final_Combined_data.csv")

    # Send each row as a JSON message
    for index, row in df.iterrows():
        data = row.to_dict()
        producer.send(topic=topic_name, value=data)
        print("Sending...", data)

    sleep(100)  # Pause before next send cycle
