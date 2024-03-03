# receiver.py
import os
import json
import logging
from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud import pubsub_v1

load_dotenv()
logging.basicConfig(level=logging.INFO)

project_id = os.getenv('PROJECT_ID')
dispatcher_topic_name = os.getenv('DISPATCHER_TOPIC_NAME')
dispatcher_topic_path = pubsub_v1.PublisherClient().topic_path(project_id, dispatcher_topic_name)
client = bigquery.Client()

def receiver_function(event, context):
    message = json.loads(event.data.decode('utf-8'))
    success_data = message.get('data')
    failed_ids = message.get('failed_ids', [])
    
    # Assuming a BigQuery table exists to accept the success_data format
    if success_data:
        dataset = os.getenv('DATASET')
        table = os.getenv('TABLE')
        dataset_id = f"{project_id}.{dataset}"
        table_id = table
        table_ref = client.dataset(dataset_id).table(table_id)
        errors = client.insert_rows_json(table_ref, success_data)
        if errors:
            print(f"Encountered errors while inserting rows: {errors}")

    # Handle retry for failed_ids if necessary
    if failed_ids:
        retry_message = json.dumps({"tcg_ids": failed_ids})
        pubsub_v1.PublisherClient().publish(dispatcher_topic_path, retry_message.encode('utf-8'))
