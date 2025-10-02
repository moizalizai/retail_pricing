# retailer_ebay.py

# retailer_ebay.py
import pandas as pd
from silver_utils import coalesce_cols, SILVER_COLS

# Map raw eBay fields → your silver column names (first non-null wins)
COALESCE_MAP = {
    # identifiers
    "native_item_id": ["ebay_item_id", "itemId", "legacyItemId"],

    # descriptive
    "title_raw": ["title_raw", "title", "name", "itemTitle"],
    "brand_raw": ["brand_raw", "brand", "brandName", "itemBrand"],
    "model_raw": ["model_raw", "mpn", "model"],
    "category_raw_path": ["category_raw_path", "categoryPath", "leafCategoryName"],

    # refs & links
    "product_url": ["itemWebUrl", "viewItemURL", "itemWebURL", "product_url"],

    # identifiers (GTINs)
    "upc": ["upc", "ean", "gtin", "productId"],

    # pricing/currency (strings; downstream will coerce)
    "currency": [
        "currency",
        "currentPrice.currency",
        "price.currency",
        "sellingStatus.currentPrice.currencyId",
    ],
    "price_current": [
        "price_current",
        "currentPrice",
        "currentPrice.value",
        "price",
        "sellingStatus.currentPrice.value",
    ],
    "price_regular": [
        "price_regular",
        "originalPrice",
        "marketingPrice.originalPrice.value",
        "wasPrice",
    ],
    "msrp": [
        "msrp",
        "manufacturerSuggestedRetailPrice",
        "MnfctPrice",
    ],
    "shipping_cost": [
        "shipping_cost",
        "shippingServiceCost",
        "shippingServiceCost.value",
        "shippingOptions.shippingCost.value",
    ],

    # availability & ratings
    "availability_message": [
        "availability_message",
        "availabilityStatus",
        "itemAvailabilityMessage",
        "availability",
    ],
    "in_stock_flag": [
        "in_stock_flag",
        "availabilityInStock",
        "inStock",
    ],
    "rating_avg": ["rating_avg", "ratingStar", "reviewRating", "rating"],
    "rating_count": ["rating_count", "reviewCount", "ratingCount"],
}

def _apply_mapping(df: pd.DataFrame, mapping: dict, dtypes: dict | None = None) -> pd.DataFrame:
    df = df.copy()
    dtypes = dtypes or {}
    for target, sources in mapping.items():
        dtype = dtypes.get(target, "string")
        df[target] = coalesce_cols(df, sources, dtype=dtype)
    return df

def _derive_in_stock(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # if in_stock_flag not present, try to infer from availability_message
    if "in_stock_flag" not in df.columns or df["in_stock_flag"].isna().all():
        avail = df.get("availability_message")
        if avail is not None:
            def _mk(v):
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                t = str(v).strip().upper()
                if "IN_STOCK" in t or t == "IN STOCK":
                    return True
                if "OUT_OF_STOCK" in t or t == "OUT OF STOCK":
                    return False
                return None
            df["in_stock_flag"] = avail.map(_mk)
    return df

def _default_currency(df: pd.DataFrame, default="USD") -> pd.DataFrame:
    df = df.copy()
    if "currency" in df.columns:
        cur = df["currency"].astype("string").str.strip().replace({"": None})
        df["currency"] = cur.fillna(default).str.upper()
    else:
        df["currency"] = default
    return df

def normalize_ebay(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Map eBay raw columns into silver contract column names.
    Leave type coercion/promo/landed to downstream silver pipeline.
    """
    if df_raw is None or df_raw.empty:
        # Return an empty frame with silver columns so downstream doesn't crash
        return pd.DataFrame(columns=SILVER_COLS)

    df = _apply_mapping(df_raw, COALESCE_MAP)

    # Ensure currency default & try stock inference if needed
    df = _default_currency(df, default="USD")
    df = _derive_in_stock(df)

    # Keep only columns that are part of silver (others will be re-added/filled later)
    keep = [c for c in SILVER_COLS if c in df.columns]
    # If some essential fields from mapping aren't in SILVER_COLS (e.g., price_regular),
    # they already are included in SILVER_COLS; this keeps things clean.
    return df[keep] if keep else df