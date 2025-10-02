# etl_orchestrator.py
import os, uuid
import sys
from datetime import datetime
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

import n_walmart_pull as wm   # <-- add this
import n_ebay_pull as eb      # <-- and this

from silver_utils import read_raw_latest, upsert_to_silver
from retailer_walmart import normalize_walmart
from retailer_ebay import normalize_ebay

def make_ingest_run_id() -> str:
    """Works in GitHub Actions and locally."""
    rid = os.getenv("GITHUB_RUN_ID") or os.getenv("RUN_ID")
    attempt = os.getenv("GITHUB_RUN_ATTEMPT")
    if rid:
        return f"{rid}-{attempt}" if attempt else str(rid)
    # local fallback
    return datetime.utcnow().strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8]

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

def assert_nonempty(name, df):
    if df is None or df.empty:
        raise RuntimeError(f"{name}: empty after step")

def main():
    run_id = make_ingest_run_id()
    wm_run_id = f"{run_id}-wm"
    eb_run_id = f"{run_id}-eb"

    # 1) WRITE RAW
    wm_blob = wm.run_pull_and_write_raw(wm_run_id)   # must return blob path
    eb_blob = eb.run_pull_and_write_raw(eb_run_id)   # must return blob path
    print("RAW blobs:", wm_blob, eb_blob)

    # 2) READ RAW BACK
    df_wm_raw = read_raw_latest("walmart")
    df_eb_raw = read_raw_latest("ebay")
    print("RAW shapes:", df_wm_raw.shape, df_eb_raw.shape)
    assert_nonempty("walmart_raw", df_wm_raw)
    assert_nonempty("ebay_raw", df_eb_raw)

    # Optional sanity on stamps
    if "retailer_id" in df_wm_raw.columns:
        print("wm retailer_id:", df_wm_raw["retailer_id"].dropna().astype(str).str.lower().value_counts().to_dict())
    if "retailer_id" in df_eb_raw.columns:
        print("eb retailer_id:", df_eb_raw["retailer_id"].dropna().astype(str).str.lower().value_counts().to_dict())

    # 3) PRE-SILVER
    df_wm_pre = normalize_walmart(df_wm_raw)
    df_eb_pre = normalize_ebay(df_eb_raw)
    print("PRE-SILVER shapes:", df_wm_pre.shape, df_eb_pre.shape)
    assert_nonempty("walmart_pre", df_wm_pre)
    assert_nonempty("ebay_pre", df_eb_pre)

    # 4) SILVER
    upsert_to_silver(
        df_wm_pre, retailer_id="walmart", source_endpoint="pull",
        ingest_run_id=wm_run_id, table="products_v1",
        key_cols=("retailer_id","native_item_id")
    )
    upsert_to_silver(
        df_eb_pre, retailer_id="ebay", source_endpoint="pull",
        ingest_run_id=eb_run_id, table="products_v1",
        key_cols=("retailer_id","native_item_id")
    )

if __name__ == "__main__":
    main()
