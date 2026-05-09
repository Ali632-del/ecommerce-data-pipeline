import re
from pathlib import Path

import numpy as np
import pandas as pd

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import get_config, get_logger

logger = get_logger("transform")
cfg    = get_config()


# ─────────────────────────────────────────────────────────────────────────────
# Helper utilities
# ─────────────────────────────────────────────────────────────────────────────

def clean_price(value) -> float | None:
    """Strip currency symbols, commas, quotes → float."""
    if pd.isna(value):
        return None
    cleaned = re.sub(r"[^\d.]", "", str(value))
    try:
        return float(cleaned)
    except ValueError:
        return None


def ratings_tier(rating: float) -> str:
    """Bucket float rating into a descriptive tier."""
    if pd.isna(rating):
        return "unknown"
    if rating >= 4.5:
        return "excellent"
    if rating >= 4.0:
        return "good"
    if rating >= 3.0:
        return "average"
    return "poor"


def price_bucket(price: float) -> str:
    """Segment products into price bands (INR)."""
    if pd.isna(price):
        return "unknown"
    if price < 500:
        return "budget"
    if price < 2000:
        return "mid_range"
    if price < 5000:
        return "premium"
    return "luxury"


# ─────────────────────────────────────────────────────────────────────────────
# Core transformation steps
# ─────────────────────────────────────────────────────────────────────────────

def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Cast columns to correct dtypes; drop pipeline metadata cols."""
    logger.debug("Enforcing schema on %d rows", len(df))

    df["product_id"]    = pd.to_numeric(df["product_id"],    errors="coerce")
    df["rating"]        = pd.to_numeric(df["rating"],        errors="coerce")
    df["ratings_count"] = pd.to_numeric(df["ratings_count"], errors="coerce")
    df["initial_price"] = pd.to_numeric(df["initial_price"], errors="coerce")
    df["discount"]      = pd.to_numeric(df["discount"],      errors="coerce")

    # Drop internal pipeline columns before storing
    drop_cols = [c for c in ["_ingested_at", "_batch_id"] if c in df.columns]
    df.drop(columns=drop_cols, inplace=True)

    return df


def clean_prices(df: pd.DataFrame) -> pd.DataFrame:
    """Parse final_price string (e.g. '₹3,995.00') → float."""
    df["final_price"] = df["final_price"].apply(clean_price)
    # Fallback: use initial_price when final_price is missing
    df["final_price"].fillna(df["initial_price"], inplace=True)
    return df


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Per-column missing-value strategy:
      - discount        : fill with column median
      - seller_name     : fill with 'Unknown Seller'
      - what_customers_said : fill with 0.0 (no sentiment score)
      - videos          : fill with 'none'
      - variations      : fill with 'none'
      - seller_information: fill with 'unknown'

    Uses assignment (not inplace) for Pandas 2.x Copy-on-Write compatibility.
    """
    fill_strategy = cfg["processing"]["discount_fill_strategy"]

    fill_val = df["discount"].median() if fill_strategy == "median" else 0.0
    df["discount"] = df["discount"].fillna(fill_val)

    # Ensure string columns are object dtype before filling with string literals
    for col in ["seller_name", "videos", "variations", "seller_information"]:
        df[col] = df[col].astype(object)

    df["seller_name"]      = df["seller_name"].fillna("Unknown Seller")
    df["videos"]           = df["videos"].fillna("none")
    df["variations"]       = df["variations"].fillna("none")
    df["seller_information"] = df["seller_information"].fillna("unknown")

    # Numeric fill — cast first to avoid StringDtype conflict
    df["what_customers_said"] = pd.to_numeric(
        df["what_customers_said"], errors="coerce"
    ).fillna(0.0)

    return df


def feature_engineering(df: pd.DataFrame) -> pd.DataFrame:
    """
    Derived columns that power warehouse analytics:

    discount_amount     : initial_price - final_price
    is_discounted       : bool — product has any discount
    price_bucket        : 'budget' | 'mid_range' | 'premium' | 'luxury'
    ratings_tier        : 'excellent' | 'good' | 'average' | 'poor'
    has_seller          : bool — seller_name is known
    has_variations      : bool — product has variants
    category_slug       : lower-case, hyphen-normalised category
    title_word_count    : proxy for listing completeness
    rating_x_count      : weighted popularity score
    """
    df["discount_amount"]  = (df["initial_price"] - df["final_price"]).clip(lower=0)
    df["is_discounted"]    = df["discount"] > 0
    df["price_bucket"]     = df["final_price"].apply(price_bucket)
    df["ratings_tier"]     = df["rating"].apply(ratings_tier)
    df["has_seller"]       = df["seller_name"].ne("Unknown Seller")
    df["has_variations"]   = df["variations"].ne("none")
    df["category_slug"]    = (
        df["category"].str.lower().str.strip().str.replace(r"\s+", "-", regex=True)
    )
    df["title_word_count"] = df["title"].str.split().str.len()
    df["rating_x_count"]   = (df["rating"] * np.log1p(df["ratings_count"])).round(4)

    return df


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    df = df.drop_duplicates(subset=["product_id"], keep="last")
    logger.info("Deduplication removed %d rows", before - len(df))
    return df


def validate(df: pd.DataFrame) -> pd.DataFrame:
    """Enforce value constraints; log but keep borderline rows."""
    low  = cfg["processing"]["rating_min"]
    high = cfg["processing"]["rating_max"]
    bad_ratings = df["rating"].between(low, high) | df["rating"].isna()
    n_bad = (~bad_ratings).sum()
    if n_bad:
        logger.warning("%d rows with out-of-range ratings — clamping", n_bad)
        df["rating"] = df["rating"].clip(low, high)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Orchestrate full pipeline
# ─────────────────────────────────────────────────────────────────────────────

def run_transformation(raw_dir: str | None = None) -> pd.DataFrame:
    """
    Execute the full transformation pipeline.
    Returns the processed DataFrame (also writes Parquet to processed_data).
    """
    raw_path  = Path(raw_dir or cfg["paths"]["raw_data_lake"])
    proc_path = Path(cfg["paths"]["processed_data"])
    proc_path.mkdir(parents=True, exist_ok=True)

    parquet_files = list(raw_path.rglob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No Parquet files found under {raw_path}")

    logger.info("Loading %d raw Parquet files…", len(parquet_files))
    df = pd.concat(
        [pd.read_parquet(f) for f in parquet_files],
        ignore_index=True,
    )
    logger.info("Raw rows loaded: %d", len(df))

    # Sequential transformation steps
    steps = [
        ("schema",          enforce_schema),
        ("price_clean",     clean_prices),
        ("missing_values",  handle_missing_values),
        ("features",        feature_engineering),
        ("deduplication",   deduplicate),
        ("validation",      validate),
    ]
    for name, fn in steps:
        logger.info("→ Step: %s", name)
        df = fn(df)

    logger.info("Transformation complete. Output rows: %d", len(df))

    # Write processed output
    out_file = proc_path / "products_cleaned.parquet"
    df.to_parquet(out_file, index=False, engine="pyarrow")
    logger.info("Processed data written → %s", out_file)

    return df


if __name__ == "__main__":
    run_transformation()
