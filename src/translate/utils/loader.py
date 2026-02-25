import os
import json
import pandas as pd
from enum import Enum
from .paths import METADATA_ARTIFACT_DIR, SILVER_DIR
from typing import Union
from pathlib import Path

class BronzeDataName(Enum):
    CUSTOMERS = "customers"
    GEOLOCATION = "geolocation"
    ORDER_ITEMS = "order_items"
    ORDER_PAYMENTS = "order_payments"
    ORDER_REVIEWS = "order_reviews"
    ORDERS = "orders"
    PRODUCTS = "products"
    SELLERS = "sellers"
    CATEGORY = "product_category_name_translation"

class SilverDataName(Enum):
    CLEAN_REVIEWS = "clean_comments.tsv"
    CLEAN_REVIEWS_TEXT_ONLY = "clean_comments_text_only.tsv"
    CATEGORY = "product_categories.tsv"
    PRODUCTS = "products.tsv"
    ENG_REVIEWS_WITH_SENTI = "eng_reviews_with_senti.tsv"

def resolve_dataset_path(file: Union[BronzeDataName, SilverDataName, str]) -> Path:
    if isinstance(file, BronzeDataName):
        with open(os.path.join(METADATA_ARTIFACT_DIR, 'bronze_paths.json'), 'r') as f:
            paths_dict = json.load(f)
        return Path(paths_dict[file.value])
    elif isinstance(file, SilverDataName):
        return Path(SILVER_DIR) / file.value
    elif isinstance(file, str):
        return Path(file)
    else:
        raise TypeError(f"Unsupported type: {type(file)}")
    
def get_dataset(file: Union[BronzeDataName, SilverDataName, str]) -> tuple[pd.DataFrame, str]:
    path = resolve_dataset_path(file)

    if not path.exists():
        raise FileNotFoundError(f"Check path: {path}")

    dataset = load_file(path)
    return dataset, str(path)

def load_file(path: Union[str, Path]) -> pd.DataFrame:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    suffix = path.suffix.lower()

    if suffix == '.tsv':
        return pd.read_csv(path, sep='\t', keep_default_na=False)
    elif suffix == '.csv':
        return pd.read_csv(path, keep_default_na=False)
    else:
        raise ValueError(f"Unsupported file extension: {suffix}")
