import os
import json
import logging
from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud import pubsub_v1

load_dotenv()
logging.basicConfig(level=logging.INFO)
publisher = pubsub_v1.PublisherClient()

def fetch_tcg_ids():
    """Fetches all tcg_ids from BigQuery."""
    TABLE = os.getenv("TABLE")
    client = bigquery.Client()
    query = f"""
        SELECT DISTINCT CAST(productId AS INT64) AS tcg_id
        FROM `{TABLE}`
        WHERE language = 'ENGLISH'
    """
    query_job = client.query(query)
    results = query_job.result()
    return [str(row['tcg_id']) for row in results]

def publish_batch(topic_path, tcg_ids, url, batch_size=1000):
    """Publishes tcg_ids in batches to avoid exceeding Pub/Sub message size limits."""
    total_ids = len(tcg_ids)
    for i in range(0, total_ids, batch_size):
        batch_data = tcg_ids[i:i+batch_size]
        message = json.dumps({"tcg_ids": batch_data, "url": url})
        publisher.publish(topic_path, message.encode("utf-8")).result()
