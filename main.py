from fastapi import FastAPI
from src.database import transform, insert_row, create_dataset, create_table
from pydantic import BaseModel
from google.cloud import pubsub_v1
import json

project_id = "playground-olavo-387508"
topic_id = "process_data"
sub_id = "raw-message-sub" 

# Pub/Sub consumer timeout
timeout = 3.0

app = FastAPI()




@app.post("/process/")
async def process_item():

    # Modify the data as needed and publish it
    def process_msg(message):
        raw_data = message.data.decode("utf-8")
        json_data = json.loads(raw_data)
        processed_data = transform(json_data)
        print(message)
        print(f"Processed message: {processed_data}")

        # insert data into BigQuery
        job_result = insert_row("my_dataset", "my_table", processed_data)
        if job_result == []:
           result = f"The following row: {processed_data} has been added into BigQuery."
        else:
            result = job_result
        
        print(result)

        # Publish the modified data to a new topic

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_id)
        message_id = publisher.publish(topic_path, data=result.encode("utf-8"))
        print(f"Published message: {result}. {message_id}")
    
        # Acknowledge the received message
        message.ack()


    # Subscribe to the Pub/Sub topic and start receiving messages
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, sub_id)
    streaming_pull_future = subscriber.subscribe(subscription_path,process_msg)
    with subscriber:
        try:                
            streaming_pull_future.result(timeout)
        except TimeoutError:
            streaming_pull_future.cancel()


