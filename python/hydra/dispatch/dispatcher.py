# dispatcher.py
import os
import json
import logging
from dotenv import load_dotenv
from google.cloud import pubsub_v1

load_dotenv()
logging.basicConfig(level=logging.INFO)

publisher = pubsub_v1.PublisherClient()
project_id = os.getenv('PROJECT_ID')
task_topic_name = os.getenv('TASK_TOPIC_NAME')
task_topic_path = publisher.topic_path(project_id, task_topic_name)

def dispatcher_function(event, context):
    """Distributes incoming messages into smaller batches and publishes them."""
    try:
        message = json.loads(event['data'].decode('utf-8'))
        tcg_ids = message['tcg_ids']
        url = message['url']
        
        batch_size = int(os.getenv('BATCH_SIZE', 40))
        batches = [tcg_ids[i:i + batch_size] for i in range(0, len(tcg_ids), batch_size)]
        
        for batch in batches:
            batch_message = json.dumps({"tcg_ids": batch, "url": url})
            publisher.publish(task_topic_path, batch_message.encode('utf-8'))
            logging.info(f"Published batch of size {len(batch)}")
    except Exception as e:
        logging.error(f"Failed to dispatch message: {e}")
