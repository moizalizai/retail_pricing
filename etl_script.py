# etl_orchestrator.py
import os
import sys
import pandas as pd
from silver_utils import (
    SILVER_COLS,
    standardize_brand_title, map_categories, normalize_currency,
    normalize_units, compute_effective_price, validate_schema, make_silver_keys, 
    read_raw_latest, upsert_to_silver
)
from retailer_walmart import normalize_walmart
from retailer_ebay import normalize_ebay

# --- Extract (pull raw to Azure/raw or your Bronze table) ---
# Keep your existing pull scripts as-is
import n_walmart_pull   # your existing pull script module
import n_ebay_pull      # your existing pull script module

# --- Transform+Load (retailer → pre-silver → silver) ---
from retailer_walmart import normalize_walmart
from retailer_ebay import normalize_ebay
from silver_utils import (
    read_raw_latest,     # implement: read latest Walmart/eBay raw snapshot from Azure
    validate_schema,
    normalize_currency,  # shared canonical rules if you use them
    normalize_units,
    standardize_brand_title,
    map_categories,
    compute_effective_price,
    make_silver_keys,
    upsert_to_silver,
)

def to_silver(df_pre: pd.DataFrame | None) -> pd.DataFrame:
    """Convert a pre-silver dataframe to the silver contract. Safe on None/empty."""
    if df_pre is None or len(df_pre) == 0:
        # return an empty frame with the right columns so downstream won’t crash
        return pd.DataFrame(columns=SILVER_COLS)

    df = df_pre.copy()
    df = standardize_brand_title(df)
    df = map_categories(df)
    df = normalize_currency(df)
    df = normalize_units(df)
    df = compute_effective_price(df)
    df = validate_schema(df)
    df = make_silver_keys(df)
    return df

def main():
    run_id = os.getenv("GITHUB_RUN_ID", str(uuid.uuid4()))

    # 1) Read latest raw
    df_wm_raw = read_raw_latest("walmart")
    df_eb_raw = read_raw_latest("ebay")

    # 2) Map raw → pre-silver (per retailer)
    df_wm_pre = normalize_walmart(df_wm_raw)   # must return a DataFrame (even if empty)
    df_eb_pre = normalize_ebay(df_eb_raw)

    # 3) Silver transform (optional if you let upsert_to_silver do it)
    df_wm_silver = to_silver(df_wm_pre)
    df_eb_silver = to_silver(df_eb_pre)

    # 4) Write silver (per retailer so retailer_id is set correctly in _finalize)
    upsert_to_silver(
        df_wm_silver, retailer_id="walmart", source_endpoint="pull", ingest_run_id=run_id,
        table="products_v1", key_cols=("retailer_id","native_item_id")
    )
    upsert_to_silver(
        df_eb_silver, retailer_id="ebay", source_endpoint="pull", ingest_run_id=run_id,
        table="products_v1", key_cols=("retailer_id","native_item_id")
    )

if __name__ == "__main__":
    main()
