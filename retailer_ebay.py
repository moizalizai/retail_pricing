# retailer_ebay.py

# retailer_ebay.py
import pandas as pd
from silver_utils import (
    SILVER_COLS, coalesce_cols, _to_float, _to_int
)

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
    Map eBay raw/pre-bronze records into the pre-silver schema.
    Leave currency normalization, FX, landed, promo-depth, and finalization
    to the silver pipeline (upsert_to_silver).
    """
    if df_raw is None or df_raw.empty:
        return pd.DataFrame(columns=SILVER_COLS)

    r = df_raw.copy()

    # Short-hand coalescer (first non-null across provided columns)
    def pick(cols, dtype="string"):
        return coalesce_cols(r, cols, dtype=dtype)

    n = len(r)
    out = pd.DataFrame({
        # metadata (if absent, silver will fill)
        "snapshot_date":         pick(["snapshot_date"]),
        "captured_at":           pick(["captured_at"]),
        "retailer_id":           pd.Series(["ebay"] * n, index=r.index, dtype="string"),

        # identifiers
        "native_item_id":        pick(["ebay_item_id", "itemId", "legacyItemId"]),
        "upc":                   pick(["upc", "ean", "gtin"]),

        # descriptive
        "title_raw":             pick(["title", "name", "title_raw"]),
        "brand_raw":             pick(["brand", "itemBrand", "brand_raw"]),
        "model_raw":             pick(["mpn", "model", "model_raw"]),
        "category_raw_path":     pick(["category_raw_path", "primaryCategory", "categoryPath", "category"]),
        "product_url":           pick(["itemWebUrl", "viewItemURL", "itemWebURL", "product_url"]),

        # pricing (numeric coercion below)
        "currency":              pick(["currency"]),
        "price_current":         pick(["price_current", "price", "currentPrice"]),
        "price_regular":         pick(["price_regular", "originalPrice"]),
        "msrp":                  pick(["msrp"]),

        # shipping / landed
        "shipping_cost":         pick(["shipping_cost", "shippingServiceCost"]),
        "landed_price":          pd.Series([None] * n, index=r.index),

        # promo (let silver compute final method/depth; set an explicit flag if obvious)
        "promo_flag":            pd.Series([None] * n, index=r.index),
        "promo_detect_method":   pd.Series([None] * n, index=r.index),
        "discount_depth":        pd.Series([None] * n, index=r.index),

        # availability
        "in_stock_flag":         pd.Series([None] * n, index=r.index),
        "availability_message":  pick(["availability_message", "availabilityStatus"]),

        # ratings
        "rating_avg":            pick(["rating_avg"]),
        "rating_count":          pick(["rating_count"]),

        # lineage
        "source_endpoint":       pd.Series(["browse:item"] * n, index=r.index, dtype="string"),
        "ingest_run_id":         pick(["ingest_run_id"]),
        "ingest_status":         pick(["ingest_status"]),
    }, index=r.index)

    # Defaults / light coercions (the silver pipeline will re-coerce/validate again)
    # Currency default → USD if missing/blank
    cur = out["currency"].astype("string")
    out["currency"] = cur.where(cur.notna() & (cur.str.len() > 0), "USD")

    # Numeric fields
    for c in ["price_current", "price_regular", "msrp", "shipping_cost", "rating_avg", "discount_depth", "landed_price"]:
        out[c] = out[c].map(_to_float)
    out["rating_count"] = out["rating_count"].map(_to_int)

    # Availability inference
    msg_up = out["availability_message"].astype("string").str.upper()
    def _stock_flag(s):
        if isinstance(s, str):
            if "IN_STOCK" in s: return True
            if "OUT_OF_STOCK" in s: return False
        return None
    out["in_stock_flag"] = msg_up.map(_stock_flag)

    # Explicit promo hint (helps silver choose "explicit" when applicable)
    def _explicit_promo(row):
        pc, pr = row["price_current"], row["price_regular"]
        try:
            return (pc is not None) and (pr is not None) and (float(pc) < float(pr))
        except Exception:
            return None
    out["promo_flag"] = out.apply(_explicit_promo, axis=1)

    # Ensure all expected columns exist (safe if some were missing upstream)
    for c in SILVER_COLS:
        if c not in out.columns:
            out[c] = None

    # Return pre-silver (do NOT finalize or write here)
    return out[SILVER_COLS]