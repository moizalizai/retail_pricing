# retailer_walmart.py
import pandas as pd
from silver_utils import SILVER_COLS, _to_float, _to_int, _choose_regular, _promo_and_depth, _landed

# Raw Walmart → silver column names (first non-null wins)
COALESCE_MAP = {
    # identifiers
    "native_item_id": ["walmart_item_id", "itemId", "usItemId"],
    "upc": ["upc", "gtin", "ean"],

    # descriptive
    "title_raw": ["title_raw", "name", "productName"],
    "brand_raw": ["brand_raw", "brandName", "brand"],
    "model_raw": ["model_raw", "modelNumber", "model"],
    "category_raw_path": ["category_raw_path", "categoryPath", "category.path"],

    # links
    "product_url": ["product_url", "productUrl", "productTrackingUrl", "canonicalUrl"],

    # pricing/currency (strings; downstream will coerce)
    "currency": ["currency"],
    "price_current": [
        "price_current",
        "salePrice",
        "price",
        "currentPrice",
        "primaryOffer.offerPrice.price",
    ],
    "price_regular": [
        "price_regular",
        "listPrice",
        "wasPrice",
        "primaryOffer.listPrice.price",
        "msrp",
    ],
    "msrp": ["msrp"],
    "shipping_cost": ["shipping_cost", "shipping.price.price", "shippingCost"],

    # availability & ratings
    "availability_message": ["availability_message", "stock", "availabilityStatus"],
    "in_stock_flag": ["in_stock_flag", "availableOnline", "inStock"],
    "rating_avg": ["rating_avg", "customerRating", "averageRating"],
    "rating_count": ["rating_count", "numReviews", "reviewCount", "customerRatingCount", "numberOfReviews"],
}

def _apply_mapping(df: pd.DataFrame, mapping: dict, dtypes: dict | None = None) -> pd.DataFrame:
    df = df.copy()
    dtypes = dtypes or {}
    for target, sources in mapping.items():
        dtype = dtypes.get(target, "string")
        df[target] = coalesce_cols(df, sources, dtype=dtype)
    return df

def _default_currency(df: pd.DataFrame, default="USD") -> pd.DataFrame:
    df = df.copy()
    if "currency" in df.columns:
        cur = df["currency"].astype("string").str.strip().replace({"": None})
        df["currency"] = cur.fillna(default).str.upper()
    else:
        df["currency"] = default
    return df

def _derive_stock(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # If in_stock_flag missing/empty, derive from availability_message heuristics
    need = "in_stock_flag" not in df.columns or df["in_stock_flag"].isna().all()
    if need and "availability_message" in df.columns:
        def _mk(v):
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return None
            t = str(v).strip().lower()
            if t.startswith("avail"):  # "Available", "Available Online"
                return True
            if t.startswith("out"):    # "Out of stock"
                return False
            return None
        df["in_stock_flag"] = df["availability_message"].map(_mk)
    return df

def normalize_walmart(df_raw: pd.DataFrame) -> pd.DataFrame:
    if df_raw is None or df_raw.empty:
        return pd.DataFrame(columns=SILVER_COLS)
    r = df_raw.copy()

    # IMPORTANT: don’t do: r = r.rename(..., inplace=True)  # returns None
    # Either: r.rename(..., inplace=True)  OR  r = r.rename(...)

    out = pd.DataFrame({
        "snapshot_date": r.get("snapshot_date"),
        "captured_at":   r.get("captured_at"),
        "retailer_id":   "walmart",
        "native_item_id": r.get("walmart_item_id") if "walmart_item_id" in r else r.get("itemId"),
        "upc":            r.get("upc"),
        "title_raw":      r.get("name"),
        "brand_raw":      r.get("brandName"),
        "model_raw":      r.get("modelNumber") if "modelNumber" in r else None,
        "category_raw_path": r.get("categoryPath"),
        "product_url":    r.get("productUrl") if "productUrl" in r else r.get("productTrackingUrl"),
        "currency":       "USD",
        "price_current":  (r.get("price_current") if "price_current" in r else r.get("salePrice")),
        "price_regular":  (r.get("price_regular") if "price_regular" in r else r.get("msrp")),
        "msrp":           r.get("msrp"),
        # leave these for upsert_to_silver to compute:
        "price_regular_chosen": None,
        "regular_price_source": None,
        "shipping_cost":   r.get("shipping_cost"),
        "landed_price":    None,
        "promo_flag":      r.get("rollback") if "rollback" in r else None,
        "promo_detect_method": None,
        "discount_depth":  None,
        "in_stock_flag":   r.get("availableOnline"),
        "availability_message": r.get("stock") if "stock" in r else None,
        "rating_avg":      (r.get("customerRating") if "customerRating" in r else r.get("averageRating")),
        "rating_count":    (r.get("numReviews") if "numReviews" in r else r.get("reviewCount")),
        "source_endpoint": "items:responseGroup=full",
        "ingest_run_id":   None,
        "ingest_status":   "ok",
    })

    # Light numeric coercion is OK; the pipeline will re-coerce too
    for c in ["price_current","price_regular","msrp","shipping_cost","rating_avg"]:
        if c in out.columns and out[c] is not None:
            out[c] = out[c].map(_to_float)
    if "rating_count" in out.columns and out["rating_count"] is not None:
        out["rating_count"] = out["rating_count"].map(_to_int)

    return out

