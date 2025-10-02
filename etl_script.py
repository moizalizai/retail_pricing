# etl_orchestrator.py
import os, uuid
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
    read_raw_latest,
    make_silver_keys,     # implement: read latest Walmart/eBay raw snapshot from Azure
    validate_schema,
    normalize_currency,  # shared canonical rules if you use them
    normalize_units,
    standardize_brand_title,
    map_categories,
    compute_effective_price,
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
    run_id = make_ingest_run_id()

    # 1) WRITE RAW
    wm_blob = wm.run_pull_and_write_raw(run_id)   # ✅ Walmart RAW
    eb_blob = eb.run_pull_and_write_raw(run_id)   # ✅ eBay RAW
    print("RAW blobs:", wm_blob, eb_blob)

    # 2) READ RAW BACK
    df_wm_raw = read_raw_latest("walmart")
    df_eb_raw = read_raw_latest("ebay")
    print("RAW shapes:", df_wm_raw.shape, df_eb_raw.shape)

    # 3) PRE-SILVER
    df_wm_pre = normalize_walmart(df_wm_raw)
    df_eb_pre = normalize_ebay(df_eb_raw)

    # 4) SILVER
    upsert_to_silver(df_wm_pre, retailer_id="walmart", source_endpoint="pull",
                     ingest_run_id=run_id, table="products_v1",
                     key_cols=("retailer_id","native_item_id"))
    upsert_to_silver(df_eb_pre, retailer_id="ebay", source_endpoint="pull",
                     ingest_run_id=run_id, table="products_v1",
                     key_cols=("retailer_id","native_item_id"))

if __name__ == "__main__":
    main()
