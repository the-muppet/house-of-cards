# worker.py 
import os
import json
import logging
import requests
from dotenv import load_dotenv
from google.cloud import pubsub_v1

load_dotenv()
logging.basicConfig(level=logging.INFO)

# Initialize the Publisher client and define topic paths
publisher = pubsub_v1.PublisherClient()
project_id = os.getenv("PROJECT_ID")
success_topic_path = publisher.topic_path(project_id, os.getenv("SUCCESS_TOPIC_ID"))
failed_topic_path = publisher.topic_path(project_id, os.getenv("FAILED_TOPIC_ID"))
api_url = os.getenv('API_URL')


def publish_message(topic_path, message):
    """Publishes a message to a specified Pub/Sub topic."""
    publisher.publish(topic_path, json.dumps(message).encode("utf-8"))

# Worker function
def worker_function(event, context):
    """Processes a batch of ids by making POST requests and handling responses."""
    batch = json.loads(event.data.decode("utf-8"))["tcg_ids"]
    pricing_data = []
    batch_size = len(batch)
    id_pool = batch  # Initialize using full batch

    for tcg_id in batch:
        try:
            url = f"{api_url}"
            payload = {
                "filters": {
                    "term": {
                        "sellerStatus": "Live",
                        "channelId": 0,
                        "language": ["English"],
                        "printing": [],
                        "verified-seller": True,
                    },
                    "range": {"quantity": {"gte": 1}},
                    "exclude": {"channelExclusion": 0},
                },
                "from": 0,
                "size": 10,
                "sort": {"field": "price", "order": "asc"},
                "context": {"shippingCountry": "US", "cart": {}},
                "aggregations": ["listingType"],
            }
            response = requests.post(url, json=payload)

            # Check for non-200 response
            if response.status_code != 200:
                logging.error(
                    f"Error processing tcg_id {tcg_id}: {response.status_code}"
                )
                # Send non-processed ids back to Reciever
                publish_message(
                    failed_topic_path,
                    {
                        "status_code": response.status_code,
                        "left_to_process": batch_size,
                        "remaining_ids": id_pool,
                    },
                )
                # Stop processing further ids
                logging.warning(f"{batch_size} ids left to process")
                break

            # If successful, process data and remove tcg_id from pool
            data = response.json()
            for item in data.get("results", []):
                for product in item.get("results", []):
                    # Collect pricing data
                    pricing_data.append(
                        {
                            "tcg_id": tcg_id,
                            "tcg_sku": product.get("productConditionId"),
                            "sellerId": product.get("sellerId"),
                            "sellerName": product.get("sellerName"),
                            "price": product.get("price"),
                        }
                    )
                    # Remove the successfully processed id
                    batch_size -= 1
                    id_pool.remove(tcg_id)
                    logging.info(f"Processed tcg_id {tcg_id}")
        except Exception as e:
            # Stop processing on exception and handle as a failure
            logging.error(f"Error processing tcg_id {tcg_id}: {e}")
            break

    # After processing, send any collected pricing data to the receiver function
    if pricing_data:
        publish_message(success_topic_path, {"data": pricing_data})
        logging.info(f"Sent {len(pricing_data)} pricing records to Receiver")
    else:
        logging.info("No pricing data to send")
