# etl_orchestrator.py
import os
import sys
import pandas as pd

# --- Extract (pull raw to Azure/raw or your Bronze table) ---
# Keep your existing pull scripts as-is
import pull_walmart   # your existing pull script module
import pull_ebay      # your existing pull script module

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

def to_silver(df_pre: pd.DataFrame) -> pd.DataFrame:
    # If your retailer_* already returns silver-shaped rows and _finalize handles this,
    # you can skip these shared steps. Otherwise, keep canonical rules here.
    df = df_pre.copy()
    # df = normalize_currency(df, target="USD")
    # df = normalize_units(df)
    # df = standardize_brand_title(df)
    # df = map_categories(df, "configs/category_map.csv")
    # df = compute_effective_price(df)
    # df = make_silver_keys(df)
    validate_schema(df)
    return df

def main():
    # 1) Pull raw snapshots (Extract)
    pull_walmart.main()  # writes raw walmart snapshot to Azure
    pull_ebay.main()     # writes raw ebay snapshot to Azure

    # 2) Read latest raw (or point to specific snapshot paths)
    df_wm_raw = read_raw_latest("walmart")   # implement helper to fetch latest raw
    df_eb_raw = read_raw_latest("ebay")

    # 3) Retailer transforms
    df_wm_silver_pre = normalize_walmart(df_wm_raw)
    df_eb_silver_pre = normalize_ebay(df_eb_raw)

    # 4) Shared canonical + Load
    for df in (df_wm_silver_pre, df_eb_silver_pre):
        df_final = to_silver(df)
        upsert_to_silver(df_final, table="silver_products", keys=("retailer_id","native_item_id"))

if __name__ == "__main__":
    sys.exit(main())
