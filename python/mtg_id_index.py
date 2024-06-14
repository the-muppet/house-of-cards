import json
import lzma
import uuid
import logging
import requests
import tempfile
import argparse
import pandas as pd
from uuid import UUID
from typing import Optional
from dataclasses import dataclass, field

logging.basicConfig(filename='mtg_index.log', level=logging.ERROR)


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", action="store_true", help="Save data to a CSV file.")
    parser.add_argument("--bq", action="store_true", help="Save data to BigQuery.")
    parser.add_argument(
        "--jsonl", action="store_true", help="Save data to a JSONL file."
    )
    parser.add_argument(
        "--use_adc", action="store_true", help="Use Application Default Credentials."
    )
    parser.add_argument(
        "--svc_acc",
        type=str,
        default="",
        help="Path to service account credential file.",
    )
    parser.add_argument(
        "--bq_dest", type=str, default="", help="Destination dataset.table in BigQuery."
    )
    return parser.parse_args()


class UUIDEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, UUID):
            return str(obj)
        return json.JSONEncoder.default(self, obj)

@dataclass
class Config:
    save_csv: bool = False
    csv_filename: Optional[str] = field(default_factory=lambda: "mtg_index.csv")
    save_bq: bool = False
    bq_dest: str = ""
    save_jsonl: bool = False
    jsonl_filename: Optional[str] = field(default_factory=lambda: "mtg_index.jsonl")
    use_adc: bool = True
    service_acc_path: str = ""

class MtgIdManager:
    def __init__(self, urls):
        self.urls = urls
        self.json_data = []
        self.sku_list = []
        self.identifiers_data = {}
        self.set_data = []
        self.joined_df = pd.DataFrame()


    def fetch_and_decompress_data(self):
        print("Fetching required data...")
        with requests.Session() as session:
            for url in self.urls:
                response = session.get(url)
                with tempfile.NamedTemporaryFile(mode="w+b") as temp_file:
                    temp_file.write(lzma.decompress(response.content))
                    temp_file.seek(0)
                    self.json_data.append(json.load(temp_file))


    def process_sku_data(self):
        print("Processing SKUs...")
        sku_data = self.json_data[0]["data"]
        self.sku_list = []
        try:
            for item_list in sku_data.values():
                for sub_item in item_list:
                    sku_item = {
                        "skuId": sub_item["skuId"],
                        "productId": sub_item["productId"],
                        "condition": sub_item["condition"],
                        "printing": sub_item["printing"],
                        "language": sub_item["language"],
                    }
                    self.sku_list.append(sku_item)
        except KeyError as e:
            logging.error(f"Error processing SKU data: {e}")
        except TypeError as e:
            logging.error(f"Error processing SKU data: {e}")
            
    def process_identifiers_data(self):
        print("Processing identifiers..")
        self.identifiers_data = self.json_data[1]["data"]

    def process_set_data(self):
        print("Processing set codes...")
        self.set_data = self.json_data[2]["data"]

    def merge_data(self):
        print("Merging dataframes...")
        id_df = pd.DataFrame(
            [
                {
                    "uuid": uuid,
                    "setCode": entry.get("setCode", ""),
                    "name": entry.get("name", ""),
                    "tcgplayerProductId": entry.get("tcgplayerProductId", ""),
                    "scryfallId": entry.get("identifiers", {}).get("scryfallId", None) if "scryfallId" in entry.get("identifiers", {}) else None,
                    **entry.get("identifiers", {}),
                }
                for uuid, entry in self.identifiers_data.items()
            ]
        )

        id_df['scryfallId'] = id_df['scryfallId'].apply(
            lambda x: str(uuid.UUID(x)) if isinstance(x, str) and uuid.UUID(x) else None
        )

        name_mapping = {item["code"]: item["name"] for item in self.set_data}
        id_df["setCode"] = id_df["setCode"].map(name_mapping)
        id_df.rename(columns={"setCode": "edition"}, inplace=True)

        ids = pd.DataFrame(self.sku_list)
        ids["productId"] = ids["productId"].astype(float)
        id_df["tcgplayerProductId"] = pd.to_numeric(
            id_df["tcgplayerProductId"], errors="coerce"
        )

        self.joined_df = pd.merge(
            ids, id_df, left_on="productId", right_on="tcgplayerProductId", how="left"
        )

        self.joined_df.drop_duplicates(
            subset=["skuId", "condition", "language", "printing"],
            keep="first",
            inplace=True
        )

    @staticmethod
    def compute_sku(
        scryfall_id: str, condition: str, language: str, printing: str
    ) -> tuple[uuid.UUID, str | None]:
        
        scryfall_namespace = uuid.UUID(scryfall_id)

        conditions = {
            "nm": ["near mint", "NM", "nm"],
            "sp": ["lightly played", "slightly played", "LP", "SP"],
            "mp": ["moderately played", "MP", "mp"],
            "hp": ["heavily played", "HP", "hp"],
            "po": ["damaged", "poor", "PO", "po"],
        }

        condition_mapping = {
            cond.lower(): code for code, conds in conditions.items() for cond in conds
        }
        condition_code = condition_mapping.get(condition.lower(), "")

        name = f"{condition_code}_{language.lower()}_"

        if printing.lower() == "etched":
            name += "etched"
        elif printing.lower() == "foil":
            name += "foil"
        else:
            name += "nonfoil"
        sku_uuid = uuid.uuid5(scryfall_namespace, name)
        return sku_uuid, None
    

    def generate_skuuids(self):
        print("Computing skuuids...")
        error_rows = []
        
        def generate_sku_row(x):
            if pd.isna(x["scryfallId"]) or not isinstance(x["scryfallId"], str):
                error_rows.append(x.to_dict())
                logging.error(f"Invalid or missing Scryfall ID: {x.to_dict()}")
                return None

            try:
                sku_uuid, error = self.compute_sku(
                    x["scryfallId"], x["condition"], x["language"], x["printing"]
                )
                if error:
                    logging.error(f"Error computing skuuid: {error}")
                else:
                    return sku_uuid
            except ValueError as err:
                error_rows.append(x.to_dict())
                logging.error(f"Invalid Scryfall ID: {x.to_dict()}")
                return None

        self.joined_df["skuuid"] = self.joined_df.apply(generate_sku_row, axis=1)

        if error_rows:
            error_df = pd.DataFrame(error_rows)
            error_df.to_csv('errors.csv', index=False)

    def format_df(self):
        column_order = [
            "skuuid",
            "scryfallId",
            "name",
            "edition",
            "language",
            "condition",
            "printing",
            "skuId",
            "productId",
            "tcgplayerEtchedProductId",
            "mcmId",
            "cardKingdomId",
            "cardKingdomFoilId",
        ]
        self.joined_df = self.joined_df[column_order]
        return self.joined_df

    def save_to_csv(self, filename):
        print("Saving data to CSV...")
        self.joined_df.to_csv(filename, index=False)
        print(f"Data successfully saved to {filename}")

    def save_to_jsonl(self, filename):
        print("Saving data to JSONL...")
        with open(filename, "w", encoding="utf-8") as f:
            for _, row in self.joined_df.iterrows():
                f.write(json.dumps(row.to_dict(), cls=UUIDEncoder) + "\n")
        print(f"Data successfully saved to {filename}")

    def save_to_bigquery(self, config):
        from google.cloud import bigquery
        import pandas_gbq

        print("Saving data to BigQuery...")

        credentials = None
        if config.use_adc:
            bq = bigquery.Client()
        else:
            from google.oauth2 import service_account
            credentials = service_account.Credentials.from_service_account_file(
                config.service_acc_path
            )
            bq = bigquery.Client(credentials=credentials, project=credentials.project_id)

        try:
            pandas_gbq.to_gbq(
                dataframe=self.joined_df,
                destination_table=config.bq_dest,
                project_id=bq.project,
                if_exists='replace',
                credentials=credentials,
            )
            print(f"Data successfully saved to BigQuery: {config.bq_dest}")
        except Exception as e:
            print(f"Failed to save to BigQuery: {e}")


    def run_options(self):
        config = Config()
        print("Where would you like to save the index?")
        print("Enter a combination of the following options or 'e' to exit:")
        print(" 1 - Save as JSONL")
        print(" 2 - Save as CSV")
        print(" 3 - Save to BigQuery")
        print("Example: '13' to save as JSONL and to BigQuery")
        choice = input("Your choice: ").lower()

        if "e" in choice:
            print("Exiting...")
            exit()

        if "1" in choice:
            config.save_jsonl = True
            filename = input(
                "Enter filename for JSONL (default 'mtg_index.jsonl'): "
            ).strip()
            if filename:
                config.jsonl_filename = filename

        if "2" in choice:
            config.save_csv = True
            filename = input(
                "Enter filename for CSV (default 'mtg_index.csv'): "
            ).strip()
            if filename:
                config.csv_filename = filename

        if "3" in choice:
            config.save_bq = True
            dest = input(
                "Enter BigQuery destination in 'dataset.table' format: "
            ).strip()
            if dest:
                config.dest = dest
            adc_choice = input(
                "Use Application Default Credentials (ADC)? (y/n): "
            ).lower()
            config.use_adc = adc_choice == "y"
            if not config.use_adc:
                config.service_acc_path = input(
                    "Enter the service account JSON filepath: "
                )

        return config

    def run(self):
        config = self.run_options()
        self.fetch_and_decompress_data()
        self.process_sku_data()
        self.process_identifiers_data()
        self.process_set_data()
        self.merge_data()
        self.generate_skuuids()
        self.format_df()

        if config.save_csv:
            self.save_to_csv(config.csv_filename)
        if config.save_bq:
            self.save_to_bigquery(config)
        if config.save_jsonl:
            self.save_to_jsonl(config.jsonl_filename)

        print("Script Finished.")


if __name__ == "__main__":
    args = parse_arguments()
    urls = [
        "https://www.mtgjson.com/api/v5/TcgplayerSkus.json.xz",
        "https://www.mtgjson.com/api/v5/AllIdentifiers.json.xz",
        "https://www.mtgjson.com/api/v5/SetList.json.xz",
    ]
    manager = MtgIdManager(urls)
    manager.run()
