# retailer_walmart.py
import pandas as pd
from silver_utils import coalesce_cols, SILVER_COLS

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
    """
    Map Walmart raw columns into silver contract names.
    Returns a pre-silver DataFrame; downstream silver pipeline will coerce types,
    compute promo/landed, validate, and finalize.
    """
