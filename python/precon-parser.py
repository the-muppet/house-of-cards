"""
Main module for processing Magic: The Gathering Preconstructed products and pricing information.
This script performs various operations like downloading deck and SKU files,
extracting and pre-processing them, and (optional) calculating a price index for the decks*.
*Full, Near Mint TCGplayer MTG Catalog CSV export is required for price index calculation and TCG upload formatting options.
"""

import re
import os
import lzma
import json
import shutil
import pathlib
import tarfile
import logging
import argparse
import tempfile
import requests
import numpy as np
import pandas as pd

from singleton_decorator import singleton
from tkinter import Tk, filedialog
from typing import Any, Dict, List, Optional
from multiprocessing import Manager
from concurrent.futures import ThreadPoolExecutor


logging.basicConfig(level=logging.INFO)

WORKERS = 12


def read_json_file(file_path: pathlib.Path) -> Dict:
    """Read a JSON file and return its 'data' field as a dictionary."""
    with file_path.open(encoding="utf-8") as fp:
        return json.load(fp).get("data")


class Downloader:
    """
    Class to download and extract compressed files from a given URL.
    Attributes:
        file_path (Optional[pathlib.Path]): The path where the downloaded and extracted file is stored.
    Methods:
        __init__(self, url: str, output_dir: pathlib.Path): Initialize a Downloader instance.
        _download_compressed_file(self, url: str, output_dir: pathlib.Path) -> Optional[pathlib.Path]: Download a compressed file.
        _expand_xz_compressed_file(self, compressed_file: pathlib.Path) -> pathlib.Path: Extract a .xz compressed file.
    """

    def __init__(self, url: str, output_dir: pathlib.Path):
        """
        Initialize the Downloader instance with a URL and output directory.
        Parameters:
            url (str): The URL from which to download the compressed file.
            output_dir (pathlib.Path): The directory where the downloaded and extracted files will be stored.
        """
        self.file_path = self._download_compressed_file(url, output_dir)

    def _download_compressed_file(
        self, url: str, output_dir: pathlib.Path
    ) -> Optional[pathlib.Path]:
        """
        Download a compressed file from a given URL and store it in the output directory.
        Parameters:
            url (str): The URL from which to download the compressed file.
            output_dir (pathlib.Path): The directory where the downloaded file will be stored.
        Returns:
            Optional[pathlib.Path]: The path where the downloaded and extracted file is stored, or None if the download fails.
        """
        response = requests.get(url, stream=True)
        if response.status_code == requests.codes.ok:
            file_name = os.path.basename(url)
            output_path = output_dir / file_name
            with output_path.open("wb") as fp:
                fp.write(response.raw.read())
            return self._expand_xz_compressed_file(output_path)

    def _expand_xz_compressed_file(self, compressed_file: pathlib.Path) -> pathlib.Path:
        """
        Extract a .xz compressed file and store it in its parent directory.
        Parameters:
            compressed_file (pathlib.Path): The path of the .xz compressed file to be extracted.
        Returns:
            pathlib.Path: The path of the extracted files.
        Raises:
            NotImplementedError: If the file type is not supported for extraction.
        """
        if ".tar" in compressed_file.suffixes and ".xz" in compressed_file.suffixes:
            with tarfile.open(compressed_file) as fp:
                fp.extractall(compressed_file.parent)
            return compressed_file.with_suffix("").parent / compressed_file.stem
        if ".xz" in compressed_file.suffixes:
            with lzma.open(compressed_file, "rb") as f_in:
                out_file = compressed_file.with_suffix("")
                with open(out_file, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
            return out_file
        raise NotImplementedError()


class DecksPreProcessor:
    """
    Class which splits downloaded deck files into 'Commander' decks and 'Constructed' decks.
    Attributes:
        commander_decks (List[pathlib.Path]): A list of paths to the commander decks.
        constructed_decks (List[pathlib.Path]): A list of paths to the constructed decks.
        _all_deck_files (pathlib.Path): A path to the directory containing all deck files.
    Methods:
        __init__(self, all_deck_files: pathlib.Path) -> None: Initialize the DecksPreProcessor instance.
        get_decks(self) -> List[pathlib.Path]: Return a list of all decks.
        _generate_commander_precon_files(self) -> None: Generate a list of commander precon files.
        _generate_constructed_precon_files(self) -> None: Generate a list of constructed precon files.
    """

    commander_decks: List[pathlib.Path]
    constructed_decks: List[pathlib.Path]
    _all_deck_files: pathlib.Path

    def __init__(self, all_deck_files: pathlib.Path) -> None:
        """
        Initialize the DecksPreProcessor with the path to all deck files.
        Parameters:
            all_deck_files (pathlib.Path): The path to the directory containing all deck files.
        """
        self._all_deck_files = all_deck_files
        self._generate_commander_precon_files()
        self._generate_constructed_precon_files()

    def get_decks(self) -> List[pathlib.Path]:
        """
        Return a list of all decks, both commander and constructed.
        Returns:
            List[pathlib.Path]: A list containing the paths of all decks.
        """
        return self.commander_decks + self.constructed_decks

    def _generate_commander_precon_files(self) -> None:
        """
        Generate a list of commander preconstructed deck files based on their content.
        The method populates the `commander_decks` attribute with paths to decks that contain a 'commander' key.
        """
        valid_files = []
        for deck_file in self._all_deck_files.glob("*/*.json"):
            data = read_json_file(deck_file)
            if data.get("commander"):
                valid_files.append(deck_file)
        self.commander_decks = valid_files

    def _generate_constructed_precon_files(self) -> None:
        """
        Generate a list of constructed preconstructed deck files.
        The method populates the `constructed_decks` attribute with paths to decks that don't contain a 'commander' key.
        """
        self.constructed_decks = list(
            set(self._all_deck_files.glob("*/*.json")).difference(self.commander_decks)
        )


class DecksDirExpander:
    """
    Class to sanitize directory names and extract decks into the current working directory.
    Methods:
        __init__(self, all_deck_files: pathlib.Path): Initialize a DecksDirExpander instance.
        _sanitize_name(name: str): Sanitize a directory name.
        _extract_decks(all_deck_files: pathlib.Path): Extract decks into sanitized directories.
    """

    def __init__(self, all_deck_files: pathlib.Path) -> None:
        """
        Initialize the DecksDirExpander instance with a path to all_deck_files.
        Parameters:
            all_deck_files (pathlib.Path): The path to the directory containing all deck files.
        """
        self._extract_decks(all_deck_files)

    @staticmethod
    def _sanitize_name(name: str) -> str:
        """
        Remove special characters from a given directory name.
        Parameters:
            name (str): The original directory name.
        Returns:
            str: The sanitized directory name.
        """
        name_regex = r"[^-_.() 0-9A-Za-z]"
        return re.sub(name_regex, "", name)

    @staticmethod
    def _extract_decks(all_deck_files: pathlib.Path) -> None:
        """
        Extract decks from all_deck_files into sanitized directories in the current working directory.
        Parameters:
            all_deck_files (pathlib.Path): The path to the directory containing all deck files.
        """
        for deck_file_path in all_deck_files.glob("*/*.json"):
            relative_path = deck_file_path.relative_to(all_deck_files)

            sanitized_parent = DecksDirExpander._sanitize_name(
                relative_path.parent.name
            )
            sanitized_filename = DecksDirExpander._sanitize_name(relative_path.name)

            output_dir = pathlib.Path(os.getcwd()) / sanitized_parent
            output_dir.mkdir(parents=True, exist_ok=True)

            output_path = output_dir / sanitized_filename

            shutil.move(deck_file_path, output_path)


class SkuProcessor:
    """
    Class to extract SKU data from a downloaded TCGPlayerSkus.json.xz file.
    Attributes:
        skus_df (pd.DataFrame): DataFrame containing the SKU data.
    Methods:
        __init__(self, tcg_player_skus: pathlib.Path): Initialize a SkuProcessor instance.
        _extract_skus(self, tcg_player_skus: pathlib.Path) -> pd.DataFrame: Extract SKU data.
    """

    def __init__(self, tcg_player_skus: pathlib.Path) -> None:
        """
        Initialize the SkuProcessor instance with a path to the TCGPlayerSkus file.
        Parameters:
            tcg_player_skus (pathlib.Path): The path to the TCGPlayerSkus.json.xz file.
        """
        self.skus_df = self._extract_skus(tcg_player_skus)

    def _extract_skus(self, tcg_player_skus: pathlib.Path) -> pd.DataFrame:
        """
        Extract SKU data from the given TCGPlayerSkus file and return it as a DataFrame.
        Parameters:
            tcg_player_skus (pathlib.Path): The path to the TCGPlayerSkus.json.xz file.
        Returns:
            pd.DataFrame: DataFrame containing the SKU data.
        """
        data = read_json_file(tcg_player_skus)
        sku_list = [
            {
                "uuid": uuid,
                "condition": sku_detail["condition"],
                "printing": sku_detail["printing"],
                "product_id": sku_detail["productId"],
                "sku_id": sku_detail["skuId"],
            }
            for uuid, sku_details in data.items()
            for sku_detail in sku_details
            if sku_detail["condition"] == "NEAR MINT"
            and sku_detail["language"] == "ENGLISH"
        ]
        df = pd.DataFrame(sku_list)
        df.set_index("uuid", inplace=True)
        return df


@singleton
class SetCodeProcessor:
    """
    Singleton Class to convert set codes to set names using the Scryfall API.
    Attributes:
        _scryfall_api_url (str): URL to the Scryfall API for sets.
        _mapping (Dict[str, str]): Dictionary mapping set codes to set names.
    Methods:
        __init__(): Initialize a SetCodeProcessor instance.
        convert_to_english(self, set_code: str) -> str: Convert a set code to its English name.
        _download(self, set_code: str) -> str: Download set name from Scryfall API.
    """

    _scryfall_api_url = "https://api.scryfall.com/sets/{}"

    def __init__(self):
        """
        Initialize the SetCodeProcessor singleton instance.
        """
        self._mapping = {}

    def convert_to_english(self, set_code: str) -> str:
        """
        Convert a set code to its English set name.
        Parameters:
            set_code (str): The set code to convert.
        Returns:
            str: The English name of the set.
        """
        if set_code not in self._mapping:
            self._mapping[set_code] = self._download(set_code)
        return self._mapping[set_code]

    def _download(self, set_code: str) -> str:
        """
        Download the set name from Scryfall API for the given set code.
        Parameters:
            set_code (str): The set code to download its name.
        Returns:
            str: The English name of the set.
        """
        sf_data = requests.get(self._scryfall_api_url.format(set_code)).json()
        return sf_data.get("name")


class PreconstructedDeck:
    """
    Class that represents a preconstructed deck and generates a pandas dataframe for the deck.
    Attributes:
        deck (pd.DataFrame): A pandas dataframe representing the deck.
        _deck (List[List[str]]): A list of lists representing the deck.
        _deck_file (pathlib.Path): The path to the deck file.
        _valid_columns (List[str]): A list of valid column names for the deck dataframe.
    Methods:
        __init__(self, deck_file: pathlib.Path, sku_df: pd.DataFrame) -> None:
            Initializes a PreconstructedDeck object.
        to_csv(self, tcg: bool = False):
            Returns a CSV string representation of the deck dataframe.
        _get_df(self) -> pd.DataFrame:
            Returns a pandas dataframe representing the deck.
        _process_deck(self, data: Dict[str, Any]) -> None:
            Processes the deck data and adds it to the _deck list.
        _add_sku_data(self, sku_df: pd.DataFrame) -> None:
            Adds SKU data to the deck dataframe.
        _add_pricing_data(self, source_csv_path: str) -> None:
            Adds pricing data to the deck dataframe.
    """

    deck: pd.DataFrame
    _deck: List[List[str]]
    _deck_file: pathlib.Path
    _valid_columns = [
        "release",
        "deckname",
        "uuid",
        "product_id",
        # "sku_id", -- Index 4
        "ck_id",
        "mcm_id",
        "name",
        "setCode",
        "setName",
        "printing",
        "condition",
        "quantity",
        "number",
        "rarity",
    ]

    def __init__(self, deck_file: pathlib.Path, sku_df: pd.DataFrame) -> None:
        """
        Initialize the PreconstructedDeck instance with a deck file and SKU DataFrame.
        Parameters:
            deck_file (pathlib.Path): Path to the deck file.
            sku_df (pd.DataFrame): DataFrame containing SKU data.
        """
        self.type = None
        self.code = None
        self.name = None
        self.number = None
        self.rarity = None
        self._deck = []
        self._deck_file = deck_file
        with self._deck_file.open(encoding="utf-8") as fp:
            data = json.load(fp).get("data")

        self.type = data.get("type")
        self.code = data.get("code")
        self.name = data.get("name")
        self.number = data.get("number")
        self.rarity = data.get("rarity")
        self._process_deck(data)
        self._add_sku_data(sku_df)

    def to_csv(self, tcg: bool = False) -> str:
        """
        Generate a CSV string representation of the deck DataFrame.
        Parameters:
            tcg (bool): If True, generate a TCG-compatible CSV.
        Returns:
            str: The CSV string representation of the deck DataFrame.
        """
        if not tcg:
            return self.deck.to_csv(index=False, encoding="utf-8", lineterminator="\n")
        tcg_columns_mapping = {
            "TCGplayer Id": "sku_id",
            "Product Line": None,
            "Set Name": "setName",
            "Product Name": "name",
            "Title": "deckname",
            "Number": "number",
            "Rarity": "rarity",
            "Condition": "Condition",
            "TCG Market Price": "TCG Market Price",
            "TCG Direct Low": "TCG Direct Low",
            "TCG Low Price With Shipping": None,
            "TCG Low Price": "TCG Low Price",
            "Total Quantity": None,
            "Add to Quantity": "quantity",
            "TCG Marketplace Price": None,
            "Photo URL": None,
        }
        tcg_columns = list(tcg_columns_mapping.keys())

        reordered_df = self.deck.rename(
            columns={v: k for k, v in tcg_columns_mapping.items() if v is not None}
        )

        for col in tcg_columns:
            if col not in reordered_df.columns:
                reordered_df[col] = np.nan

        reordered_df = reordered_df.dropna(axis=0, subset=["TCGplayer Id"])

        return reordered_df[tcg_columns].to_csv(
            index=False, encoding="utf-8", lineterminator="\n"
        )

    def _get_df(self) -> pd.DataFrame:
        """
        Generate and return a DataFrame representation of the deck.
        Returns:
            pd.DataFrame: The deck DataFrame.
        """
        return pd.DataFrame(self._deck, columns=self._valid_columns)

    def _process_deck(self, data: Dict[str, Any]) -> None:
        """
        Process the raw deck data and populate the _deck list.
        Parameters:
            data (Dict[str, Any]): The raw deck data.
        """
        deck_name = self._deck_file.stem.split("_", 1)[0]
        release = self._deck_file.stem.split("_")[-1]

        for section in ("commander", "mainBoard", "sideBoard"):
            for card in data.get(section, []):
                if any(key not in card for key in ("uuid", "name", "setCode")):
                    continue

                identifiers = card.get("identifiers", {})
                card_kingdom_id = identifiers.get("cardKingdomId", 0)
                card_kingdom_id = (
                    int(card_kingdom_id) if card_kingdom_id is not None else 0
                )
                self._deck.append(
                    [
                        release,
                        deck_name,
                        card.get("uuid"),
                        int(identifiers.get("tcgplayerProductId", -1)),
                        card_kingdom_id,
                        identifiers.get("mcmId"),
                        card.get("name"),
                        card.get("setCode"),
                        SetCodeProcessor().convert_to_english(card.get("setCode")),
                        "FOIL" if card.get("isFoil") else "NON FOIL",
                        "NEAR MINT",
                        card.get("count", 1),
                        card.get("number"),
                        card.get("rarity"),
                    ]
                )

    def _add_sku_data(self, sku_df: pd.DataFrame) -> None:
        """
        Add SKU data to the deck DataFrame.
        Parameters:
            sku_df (pd.DataFrame): DataFrame containing SKU data.
        """
        self.deck = self._get_df().merge(
            sku_df,
            on=("uuid", "printing", "condition", "product_id"),
            how="left",
        )
        self.deck.insert(4, "sku_id", self.deck.pop("sku_id"))

    def _add_pricing_data(self, source_csv_path: str) -> None:
        """
        Add pricing data to the deck DataFrame from a source CSV file.
        Parameters:
            source_csv_path (str): Path to the source CSV file containing pricing data.
        """
        try:
            source_df = pd.read_csv(
                source_csv_path,
                dtype={0: str},
                usecols=[
                    "TCGplayer Id",
                    "Number",
                    "Rarity",
                    "Condition",
                    "TCG Market Price",
                    "TCG Direct Low",
                    "TCG Low Price",
                ],
                low_memory=False,
            )
        except FileNotFoundError:
            logging.error(f"File {source_csv_path} not found.")
            return

        if "TCGplayer Id" not in source_df.columns:
            logging.error("'TCGplayer Id' column not found in source CSV.")
            return

        source_df.rename(columns={"Condition": "Source_Condition"}, inplace=True)
        source_df["TCGplayer Id"] = pd.to_numeric(
            source_df["TCGplayer Id"], errors="coerce"
        )
        self.deck = pd.merge(
            self.deck, source_df, left_on="sku_id", right_on="TCGplayer Id", how="left"
        )
        self.deck.drop(
            columns=["TCGplayer Id", "setCode", "printing", "condition"], inplace=True
        )
        self.deck.rename(columns={"Source_Condition": "Condition"}, inplace=True)
        final_columns = [
            "release",
            "deckname",
            "uuid",
            "product_id",
            "sku_id",
            "ck_id",
            "mcm_id",
            "name",
            "setName",
            "Condition",
            "Rarity",
            "Number",
            "quantity",
            "TCG Market Price",
            "TCG Direct Low",
            "TCG Low Price",
        ]
        self.deck = self.deck[final_columns]


def vectorized_after_fees(prices: np.ndarray) -> np.ndarray:
    """
    Calculate the net amount after applying TCGplayer Direct fees based on the input price.
    Parameters:
        - `prices` (np.ndarray): The original prices before applying fees.
    Returns:
        - `np.ndarray`: The net amounts after fees are applied.
    """
    conditions = [prices < 3, (prices >= 3) & (prices < 20), prices >= 20]
    calculations = [prices * 0.5, prices * 0.8855 - 1.39, prices * 0.8855 - 4.05]

    return np.select(conditions, calculations)


def calculate_price_index(
    deck_dfs: Dict[str, pd.DataFrame],
    deck_codes: Dict[str, str],
    output_file: pathlib.Path,
) -> None:
    """
    Calculate and save a price index for each deck to a CSV file.
    Parameters:
        deck_dfs (Dict[str, pd.DataFrame]): Dictionary mapping deck names to their DataFrames.
        deck_codes (Dict[str, str]): Dictionary mapping deck names to their codes.  # New parameter
        output_file (pathlib.Path): Path where the output CSV will be saved.
    """

    price_index_data = []
    after_fees_sums = []

    for deck_name, df in deck_dfs.items():
        deck_code = deck_codes.get(deck_name, "N/A")
        df["quantity"] = df["quantity"].astype(int)
        df["TCG Market Price"] = df["TCG Market Price"].astype(float)
        df["TCG Direct Low"] = df["TCG Direct Low"].astype(float)

        market_value = round((df["quantity"] * df["TCG Market Price"]).sum(), 2)
        direct_value = round((df["quantity"] * df["TCG Direct Low"]).sum(), 2)

        price_index_data.append([deck_code, deck_name, market_value, direct_value])

        after_fees_sum = round(df["After Fees"].sum(), 2)
        after_fees_sums.append((deck_name, after_fees_sum))

    price_index_df = pd.DataFrame(
        price_index_data,
        columns=["Release", "Deck Name", "Market Value", "Total Direct Value"],
    )
    after_fees_df = pd.DataFrame(after_fees_sums, columns=["Deck Name", "After Fees"])
    price_index_df = pd.merge(price_index_df, after_fees_df, on="Deck Name", how="left")
    price_index_df = price_index_df.sort_values("Release", ascending=True)

    price_index_df["Deck After Fees"] = price_index_df.groupby("Deck Name")[
        "After Fees"
    ].cumsum()

    price_index_df.to_csv(output_file, index=False, encoding="utf-8")
    print(f"Price index has been saved in {os.getcwd()}.")


def write_to_csv(deck_instance: PreconstructedDeck, csv_data: str) -> None:
    """
    Write the deck information to a CSV file.
    Parameters:
        deck_instance (PreconstructedDeck): Instance of PreconstructedDeck containing deck details.
        csv_data (str): String representation of the CSV content to write.
    """
    current_dir = pathlib.Path(os.getcwd())
    precons_dir = current_dir / "Precons"
    precons_dir.mkdir(exist_ok=True)
    type_dir = precons_dir / deck_instance.type
    type_dir.mkdir(exist_ok=True)
    code_dir = type_dir / deck_instance.code
    code_dir.mkdir(exist_ok=True)
    csv_file_path = code_dir / f"{deck_instance.name.replace(' ', '-')}.csv"
    with csv_file_path.open("w", encoding="utf-8") as fp:
        fp.write(csv_data)


def process_deck(
    deck: PreconstructedDeck,
    deck_dfs: Dict[str, pd.DataFrame],
    tcg_player_sku_df: pd.DataFrame,
    file_path: Optional[pathlib.Path],
    deck_codes: Dict[str, str],
    args: argparse.Namespace,
) -> None:
    """
    Process a single deck and add its DataFrame to the `deck_dfs` dictionary.
    Parameters:
        deck (PreconstructedDeck): The PreconstructedDeck instance to be processed.
        deck_dfs (Dict[str, pd.DataFrame]): Dictionary holding the DataFrames of all processed decks.
        tcg_player_sku_df (pd.DataFrame): DataFrame of TCGPlayer SKU data.
        file_path (Optional[pathlib.Path]): Path to the source file for pricing data, if applicable.
        deck_codes (Dict[str, str]): Dictionary mapping deck names to their release codes.
        args (argparse.Namespace): Parsed command line arguments.
    """
    deck_instance = PreconstructedDeck(deck, tcg_player_sku_df)
    deck_codes[deck_instance.name] = deck_instance.code

    if args.priced and file_path:
        deck_instance._add_pricing_data(file_path)
        deck_instance.deck["TCG Direct Low"] = (
            deck_instance.deck["TCG Direct Low"].astype(float).fillna(0)
        )
        deck_instance.deck["After Fees"] = vectorized_after_fees(
            deck_instance.deck["TCG Direct Low"].values
        )

    deck_csv = deck_instance.to_csv(tcg=args.tcg)
    deck_name = deck_instance.name
    print(f"Processing {deck_name}")
    deck_dfs[deck_name] = deck_instance.deck
    deck_codes[deck_name] = deck_instance.code

    write_to_csv(
        deck_instance,
        deck_csv,
    )


def wrapper_process_deck(args_tuple):
    """
    Wrapper function for `process_deck` to enable multiprocessing.
    Parameters:
        args_tuple (tuple): Tuple containing arguments for `process_deck`.
    """
    return process_deck(*args_tuple)


def main():
    """
    Main function to orchestrate deck and price processing.
    Handles argument parsing, downloading of data, and initiates deck processing.
    """
    current_dir = pathlib.Path(os.getcwd())
    parser = argparse.ArgumentParser(description="Deck and Price processing.")
    parser.add_argument(
        "-priced", action="store_true", help="Enable price merging from a source CSV."
    )

    parser.add_argument(
        "-tcg", action="store_true", help="Save the csv's in TCG column ordering."
    )

    args = parser.parse_args()

    file_path = None
    if args.priced:
        Tk().withdraw()
        file_path = filedialog.askopenfilename(
            title="Select the source CSV", filetypes=[("CSV files", "*.csv")]
        )
        if not pathlib.Path(file_path).is_file():
            logging.error(f"File {file_path} does not exist. Exiting.")
            return

    temp_dir = pathlib.Path(tempfile.TemporaryDirectory().name)
    print("Creating temporary directory for downloaded resources.")
    temp_dir.mkdir(exist_ok=True, parents=True)

    tcg_player_sku_file = Downloader(
        "https://mtgjson.com/api/v5/TcgplayerSkus.json.xz", temp_dir
    ).file_path
    print("Downloading tcgplayer sku file from mtgjson...")
    tcg_player_sku_df = SkuProcessor(tcg_player_sku_file).skus_df

    all_deck_files_dir = Downloader(
        "https://mtgjson.com/api/v5/AllDeckFiles.tar.xz", temp_dir
    ).file_path
    print("Downloading precon decklist files from mtgjson...")
    DecksDirExpander(all_deck_files_dir)

    decks = DecksPreProcessor(all_deck_files_dir.parent).get_decks()

    manager = Manager()
    deck_dfs = manager.dict()
    deck_codes = manager.dict()

    args_list = [
        (deck, deck_dfs, tcg_player_sku_df, file_path, deck_codes, args)
        for deck in decks
    ]

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        executor.map(lambda args: wrapper_process_deck(args), args_list)

    deck_dfs = dict(deck_dfs)
    deck_codes = dict(deck_codes)

    calculate_price_index(deck_dfs, deck_codes, current_dir.joinpath("price_index.csv"))

    dest_path = os.path.join(os.getcwd(), "Precons")
    print(f"CSV files have been saved in {dest_path}.")


if __name__ == "__main__":
    main()
