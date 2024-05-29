import re
import os
import json
import aiohttp
import asyncio
import logging
import argparse
import aiofiles
from tqdm import tqdm
import logging.handlers
from datetime import date
from fuzzywuzzy import process
from google.cloud import bigquery
import xml.etree.ElementTree as ET
from asyncio import Queue, Semaphore, sleep

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

PROJECT_ID = os.getenv("PROJECT_ID")
BASE_DATASET_ID = os.getenv("BASE_DATASET_ID")
BASE_URL = "http://www.tcgplayer.com"
seen_sellers = set()

def create_bq_client():
    """Creates a BigQuery client."""
    return bigquery.Client(project=PROJECT_ID)

async def create_dataset(client, dataset_id):
    """Creates a BigQuery dataset if it doesn't exist."""
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        logging.info(f"Dataset {dataset_id} already exists.")
    except Exception as e:
        try:
            client.create_dataset(dataset_ref)
            logging.info(f"Created dataset {dataset_id}.")
        except Exception as e:
            logging.error(f"Error creating dataset {dataset_id}: {e}")


async def create_dataset_tables(client, dataset_id):
    """Creates BigQuery tables for the given dataset."""
    def create_table(table_id, schema):
        table_ref = client.dataset(dataset_id).table(table_id)
        try:
            table = bigquery.Table(table_ref, schema=schema)
            client.create_table(table)
            logging.info(f"Created table {table_id} in dataset {dataset_id}.")
        except Exception as e:
            logging.error(f"Error creating table {table_id}: {e}")
        
    products_schema = [
        bigquery.SchemaField("product_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("totalResults", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField(
            "conditions",
            "RECORD",
            mode="REPEATED",
            fields=[
                bigquery.SchemaField("value", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("count", "INTEGER", mode="NULLABLE"),
            ],
        ),
        bigquery.SchemaField(
            "listingTypes",
            "RECORD",
            mode="REPEATED",
            fields=[
                bigquery.SchemaField("value", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("count", "INTEGER", mode="NULLABLE"),
            ],
        ),
        bigquery.SchemaField(
            "printings",
            "RECORD",
            mode="REPEATED",
            fields=[
                bigquery.SchemaField("value", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("count", "INTEGER", mode="NULLABLE"),
            ],
        ),
    ]

    listings_schema = [
        bigquery.SchemaField("product_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("seller_key", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("tcg_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("printing", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("condition", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("direct_quantity", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("quantity", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("price", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("shipping_price", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("listing_date", "DATE", mode="NULLABLE")
    ]

    sellers_schema = [
        bigquery.SchemaField("seller_key", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("seller_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("seller_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("seller_rating", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("seller_sales", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("verified", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("gold_star", "BOOLEAN", mode="NULLABLE"),
    ]

    create_table("products", products_schema)
    create_table("listings", listings_schema)
    create_table("sellers", sellers_schema)

async def fetch_sitemap(url):
    """Fetch the XML sitemap from the given URL."""
    try:
        async with aiohttp.request("GET", url) as response:
            response.raise_for_status()
            text = await response.text(encoding="utf-8")
            logging.info(f"Successfully fetched sitemap from {url}")
            return text.replace("ï»¿", "").strip()
    except aiohttp.ClientResponseError as e:
        logging.error(f"Error fetching sitemap: {e}")
        return None


async def parse_sitemap_index(xml_content):
    """Parse sitemap index to extract category sitemap URLs."""
    try:
        root = await asyncio.to_thread(ET.fromstring, xml_content)
        sitemap_urls = [
            {
                "Category": elem.text.split("/sitemap/")[1].split(".")[0],
                "Link": elem.text,
            }
            for elem in root.findall(
                ".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc"
            )
        ]
        logging.info(
            f"Successfully parsed sitemap index. Found {len(sitemap_urls)} sitemap URLs."
        )
        return sitemap_urls
    except ET.ParseError as e:
        logging.error(f"Error parsing sitemap index XML: {e}")
        return [] 


async def fetch_and_parse_categories_from_sitemap(sitemap_index_url):
    """Fetches and parses category information from the sitemap index."""
    sitemap_index_content = await fetch_sitemap(sitemap_index_url)
    if sitemap_index_content:
        try:
            sitemap_file = "sitemap_index.xml"
            async with aiofiles.open(sitemap_file, "w", encoding="utf-8") as file:
                await file.write(sitemap_index_content)
            logging.info(f"Saved sitemap index to {sitemap_file}")
        except Exception as e:
            logging.error(f"Error saving sitemap index: {e}")

        categories = await parse_sitemap_index(sitemap_index_content)
        logging.info(
            f"Successfully fetched and parsed categories from {sitemap_index_url}"
        )
        return categories
    else:
        logging.error(
            f"Error fetching sitemap index content from {sitemap_index_url}"
        )
        return []


def find_best_category_match(categories, search_term):
    """Finds the best category match for a given search term."""
    try:
        category_names = [category["Category"] for category in categories]
        best_match, score = process.extractOne(search_term, category_names)
        logging.info(
            f"Best match for '{search_term}' is '{best_match}' with score {score}"
        )
        return next(
            (category for category in categories if category["Category"] == best_match),
            None,
        )
    except Exception as e:
        logging.error(f"Error finding best category match: {e}")
        return None


def extract_product_ids(xml_content):
    """Extract product IDs from a category sitemap."""
    try:
        product_ids = []
        root = ET.fromstring(xml_content)
        for url in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}url"):
            url_text = url.find("{http://www.sitemaps.org/schemas/sitemap/0.9}loc").text
            product_id = re.search(r"/product/(\d+)/", url_text)
            if product_id:
                product_ids.append(product_id.group(1))
        logging.info(f"Extracted {len(product_ids)} product IDs from the sitemap.")
        return product_ids
    except Exception as e:
        logging.error(f"Error extracting product IDs: {e}")
        return []


async def fetch_and_extract_product_ids_from_sitemap(sitemap_url):
    """Fetches sitemap content and extracts product IDs."""
    try:
        sitemap_content = await fetch_sitemap(sitemap_url)
        if sitemap_content:
            return extract_product_ids(sitemap_content)
        else:
            logging.error(f"Error fetching sitemap content from: {sitemap_url}")
            return []
    except Exception as e:
        logging.error(f"Error in fetch_and_extract_product_ids_from_sitemap: {e}")
        return []


def write_json_to_file(data, file_name):
    """Write data to a JSON file."""
    try:
        with open(file_name, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4)
        logging.info(f"Successfully wrote data to {file_name}")
    except Exception as e:
        logging.error(f"Error writing to JSON file: {e}")


async def fetch_product_data(product_id, session, semaphore):
    """Fetches and extracts ramp data for a product"""
    url = f"https://mp-search-api.tcgplayer.com/v1/product/{product_id}/listings"
    headers = {
        "authority": "mp-search-api.tcgplayer.com",
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9",
        "cache-control": "no-cache",
        "content-type": "application/json",
        "origin": "https://www.tcgplayer.com",
        "pragma": "no-cache",
        "referer": "https://www.tcgplayer.com/",
        "sec-ch-ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Microsoft Edge";v="122"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
    }
    data = {
        "filters": {
            "term": {"sellerStatus": "Live", "channelId": 0, "language": ["English"]},
            "range": {"quantity": {"gte": 1}},
            "exclude": {"channelExclusion": 0},
        },
        "from": 0,
        "size": 50,
        "sort": {"field": "price", "order": "asc"},
        "context": {"shippingCountry": "US", "cart": {}},
        "aggregations": ["listingType"],
    }
    async with semaphore:
        try:
            async with session.post(
                url, headers=headers, data=json.dumps(data)
            ) as response:
                if response.ok:
                    await sleep(0.2)
                    try:
                        response = await response.json(encoding="utf-8")
                        listings = response["results"][0]["results"]
                        product_data = {
                            "product_id": product_id,
                            "totalResults": response["results"][0]["totalResults"],
                            "conditions": {
                                condition["value"]: condition["count"]
                                for condition in response["results"][0]["aggregations"][
                                    "condition"
                                ]
                            },
                            "listingTypes": {
                                listing_type["value"]: listing_type["count"]
                                for listing_type in response["results"][0]["aggregations"][
                                    "listingType"
                                ]
                            },
                            "printings": {
                                printing["value"]: printing["count"]
                                for printing in response["results"][0]["aggregations"][
                                    "printing"
                                ]
                            },
                        }
                        listing_data = [
                            {
                                "product_id": product_id,
                                "seller_key": listing["sellerKey"],
                                "tcg_id": int(listing["productConditionId"]),
                                "printing": listing["printing"],
                                "condition": listing["condition"],
                                "direct_quantity": listing["directInventory"],
                                "quantity": listing["quantity"],
                                "price": listing["price"],
                                "shipping_price": listing["sellerShippingPrice"],
                            }
                            for listing in listings
                        ]
                        seller_data = {}
                        for listing in listings:
                            seller_key = listing["sellerKey"]
                            if seller_key not in seen_sellers: 
                                seen_sellers.add(seller_key)
                                seller_data[seller_key] = {
                                    "seller_key": seller_key,
                                    "seller_id": listing["sellerId"],
                                    "seller_name": listing["sellerName"],
                                    "seller_rating": listing["sellerRating"],
                                    "seller_sales": listing["sellerSales"],
                                    "verified": listing["verifiedSeller"],
                                    "gold_star": listing["goldSeller"],
                                }
                        return product_data, listing_data, seller_data
                    except Exception as e:
                        logging.error(
                            f"Error parsing JSON response for product {product_id}: {e}"
                        )
                        return None, None, None
                else:
                    logging.error(
                        f"Failed to fetch product data for ID {product_id}: {response.status}"
                    )
                    return None, None, None
        except Exception as e:
            logging.error(f"Error fetching product data for ID {product_id}: {e}")
            return None, None, None
    

async def worker(product_queue, data_queue, session, progress_bar, semaphore):
       while True:
           product_id = await product_queue.get()
           try:
               product_data, listing_data, seller_data_dict = await fetch_product_data(
                   product_id, session, semaphore
               )
               if isinstance(product_data, int):
                   await product_queue.put(product_id)
               else:
                   await data_queue.put((product_data, listing_data, seller_data_dict))
           except Exception as e:
               logging.error(
                   f"Error fetching product data for ID {product_id}: {e}"
               )
               await product_queue.put(product_id)
               await sleep(10)
           finally:
               product_queue.task_done()
               progress_bar.update(1)

async def _enqueue_products(product_ids, product_queue):
    """Adds product IDs to the queue."""
    for product_id in product_ids:
        await product_queue.put(product_id)

async def fetch_and_write_product_data(product_ids, category_name, category_folder, workers=50, session=None):
    """Fetches and writes product data to files using workers."""
    product_file_name = os.path.join(category_folder, "product_data.jsonl")
    listing_file_name = os.path.join(category_folder, "listings.jsonl")
    seller_file_name = os.path.join(category_folder, "sellers.jsonl")
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=100)) as session:
        product_queue = Queue(maxsize=100000)
        semaphore = Semaphore(50)
        data_queue = asyncio.Queue()
        tasks = []
        with tqdm(total=len(product_ids), desc=f"Fetching {category_name.capitalize()} Data") as progress_bar:
            for _ in range(workers):
                task = asyncio.create_task(
                    worker(
                        product_queue, data_queue, session, progress_bar, semaphore
                    )
                )
                tasks.append(task)

            tasks.append(asyncio.create_task(write_data(data_queue, product_file_name, listing_file_name, seller_file_name)))

            await _enqueue_products(product_ids, product_queue)
            await product_queue.join()
            await data_queue.join()

        logging.info(
                f"Saved data to: {product_file_name}, {listing_file_name}, and {seller_file_name}"
            )
        
async def write_data(data_queue, product_file_name, listing_file_name, seller_file_name):
    while True:
        product_data, listing_data, seller_data_dict = await data_queue.get()
        if product_data:
            async with aiofiles.open(product_file_name, "a", encoding="utf-8") as product_file:
                await product_file.write(json.dumps(product_data) + "\n")

        if listing_data:
            async with aiofiles.open(listing_file_name, "a", encoding="utf-8") as listing_file:
                for listing in listing_data:
                    await listing_file.write(json.dumps(listing) + "\n")

        if seller_data_dict:
            for seller_key, seller_data in seller_data_dict.items():
                if seller_key not in seen_sellers: 
                    seen_sellers.add(seller_key)
                    async with aiofiles.open(seller_file_name, "a", encoding="utf-8") as seller_file:
                        await seller_file.write(json.dumps(seller_data) + "\n")

        data_queue.task_done()


async def write_data(data_queue, product_file_name, listing_file_name, seller_file_name):
    while True:
        product_data, listing_data, seller_data_dict = await data_queue.get()
        if product_data:
            async with aiofiles.open(product_file_name, "a", encoding="utf-8") as product_file:
                await product_file.write(json.dumps(product_data) + "\n")

        if listing_data:
            async with aiofiles.open(listing_file_name, "a", encoding="utf-8") as listing_file:
                for listing in listing_data:
                    await listing_file.write(json.dumps(listing) + "\n")

        if seller_data_dict is not None:
            async with aiofiles.open(seller_file_name, "a", encoding="utf-8") as seller_file:
                for seller in seller_data_dict.values():
                    await seller_file.write(json.dumps(seller) + "\n")
        data_queue.task_done()

async def scrape_category(category, workers=50, session=None):
    """Scrapes product details for a selected category."""
    try:
        category_name = category["Category"]
        logging.info(f"Scraping category: {category_name}")

        # 1. Create Category Folder
        category_folder = category_name
        os.makedirs(category_folder, exist_ok=True)
        logging.info(f"Created category folder: {category_folder}")

        # 2. Fetch and Save Category Sitemap
        category_sitemap_filename = os.path.join(
            category_folder, f"{category_name}_sitemap.xml"
        )
        try:
            category_sitemap_content = await fetch_sitemap(category["Link"])
            if category_sitemap_content:
                async with aiofiles.open(
                    category_sitemap_filename, "w", encoding="utf-8"
                ) as f:
                    await f.write(category_sitemap_content)
                logging.info(f"Saved category sitemap to: {category_sitemap_filename}")
            else:
                logging.error(f"Failed to fetch category sitemap: {category['Link']}")
                return
        except Exception as e:
            logging.error(f"Error fetching/saving category sitemap: {e}")
            return

        # 3. Extract and Save Product IDs
        product_ids = extract_product_ids(category_sitemap_content)
        if not product_ids:
            logging.error(f"No product IDs found in: {category_sitemap_filename}")
            return
        product_ids_filename = os.path.join(category_folder, "product_ids.json")
        try:
            with open(product_ids_filename, "w") as f:
                json.dump(product_ids, f)
                logging.info(f"Saved product IDs to: {product_ids_filename}")
        except Exception as e:
            logging.error(f"Error saving product IDs: {e}")
            return

        # 4. Fetch and Write Product Data
        await fetch_and_write_product_data(product_ids, category_name, category_folder, workers=50, session=session)

    except Exception as e:
        logging.error(f"Error scraping category: {e}")
        return


async def main(search_term, workers=50):
    parser = argparse.ArgumentParser(
        description="Web scrape with category selection via fuzzy match."
    )
    parser.add_argument(
        "category_name", type=str, help="The category name to scrape, e.g., pokemon"
    )
    parser.add_argument(
        "workers", type=int, default=10, help="Number of concurrent workers to use"
    )
    args = parser.parse_args()
    logging.info("Starting scraper")

    try:
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=100)) as client:
            categories = await fetch_and_parse_categories_from_sitemap(
                f"{BASE_URL}/sitemap/index.xml"
            )

            if not categories:
                logging.error("Failed to fetch categories.")
                return

            search_term = args.category_name
            best_match = find_best_category_match(categories, search_term)

            if best_match:
                workers = args.workers
                await scrape_category(best_match, workers, client)
            else:
                logging.error(f"No suitable match found for category: {search_term}")
    except Exception as e:
        logging.exception(f"An unexpected error occurred: {e}")
    finally:
        await client.close()
        logging.info("Finished")


if __name__ == "__main__":
    import sys
    asyncio.run(main(sys.argv[1], int(sys.argv[2])))