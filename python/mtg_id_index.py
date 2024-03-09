import json
import lzma
import uuid
import requests
import tempfile
import argparse
import pandas as pd
from uuid import UUID


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


class Config:
    def __init__(self):
        self.save_csv = False
        self.csv_filename = "mtg_index.csv"
        self.save_bq = False
        self.bq_dest = ""
        self.save_jsonl = False
        self.jsonl_filename = "mtg_index.jsonl"
        self.use_adc = True
        self.service_acc_path = ""


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
        self.sku_list = [
            {
                "skuId": sub_item["skuId"],
                "productId": sub_item["productId"],
                "condition": sub_item["condition"],
                "printing": sub_item["printing"],
                "language": sub_item["language"],
            }
            for item_list in sku_data.values()
            for sub_item in item_list
        ]

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
                    "scryfallId": entry.get("identifiers", {}).get("scryfallId", ""),
                    **entry.get("identifiers", {}),
                }
                for uuid, entry in self.identifiers_data.items()
            ]
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

    @staticmethod
    def compute_sku(
        scryfall_id: uuid.UUID, condition: str, language: str, printing: str
    ) -> tuple[uuid.UUID, str | None]:
        try:
            scryfall_namespace = uuid.UUID(scryfall_id)
        except ValueError as err:
            return None, f"Invalid scryfall id: {err}"

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

    def generate_sku_uuids(self):
        print("Computing sku_uuids...")

        def generate_sku_row(x):
            if pd.isna(x["scryfallId"]) or not isinstance(x["scryfallId"], str):
                return None

            sku_uuid, error = self.compute_sku(
                x["scryfallId"], x["condition"], x["language"], x["printing"]
            )
            if error:
                print(error)
            else:
                return sku_uuid

        self.joined_df["sku_uuid"] = self.joined_df.apply(generate_sku_row, axis=1)

    def format_df(self):
        column_order = [
            "sku_uuid",
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
        self.generate_sku_uuids()
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
