# retailer_ebay.py

import uuid
import pandas as pd

from silver_utils import (
    SILVER_COLS,
    _to_float,
    _to_int,
    _choose_regular,
    _promo_and_depth,
    _landed,
    _today_iso,
    _now_utc_iso,
    _finalize,
)

def _blank_series(df: pd.DataFrame, value=None) -> pd.Series:
    return pd.Series([value] * len(df), index=df.index)

def coalesce(df: pd.DataFrame, *cols: str) -> pd.Series:
    """
    Column-wise coalesce for DataFrames: returns the first non-null value across the given columns.
    If none of the columns exist, returns an all-None Series of the right length.
    """
    if df.empty:
        return pd.Series([], dtype=object)
    s = _blank_series(df, None)
    for c in cols:
        if c in df.columns:
            s = s.combine_first(df[c])
    return s

def normalize_ebay(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Transform eBay raw/pre-bronze records into the pre-silver contract, then finalize into silver schema.
    Assumes eBay Browse-like fields (e.g., itemId, itemWebUrl, price/originalPrice if present).
    """
    if df_raw is None or df_raw.empty:
        return pd.DataFrame(columns=SILVER_COLS)

    r = df_raw.copy()

    # -----------------------
    # IDs / Keys
    # -----------------------
    r["_native_item_id"] = coalesce(r, "ebay_item_id", "itemId").astype(str)

    # -----------------------
    # UPC / GTIN
    # -----------------------
    if "upc" in r.columns:
        upc = r["upc"].astype(str).str.strip().replace({"": None})
    else:
        upc = _blank_series(r, None)
    r["_upc"] = upc

    # -----------------------
    # Descriptive
    # -----------------------
    r["_title_raw"]         = coalesce(r, "title", "title_raw")
    r["_brand_raw"]         = coalesce(r, "brand", "brand_raw")
    r["_model_raw"]         = coalesce(r, "mpn", "model_raw")
    r["_category_raw_path"] = coalesce(r, "category_raw_path")
    r["_product_url"]       = coalesce(r, "itemWebUrl", "product_url")

    # -----------------------
    # Currency & Prices
    # -----------------------
    # Currency: prefer column, default to USD, ensure a Series
    if "currency" in r.columns:
        curr_series = r["currency"].astype(str).replace({"": None}).fillna("USD")
    else:
        curr_series = _blank_series(r, "USD")
    r["_currency"] = curr_series

    # Current price: prefer "price_current", then "price"
    price_current_raw = coalesce(r, "price_current", "price")
    r["_price_current"] = price_current_raw.apply(_to_float) if price_current_raw is not None else _blank_series(r, None)

    # Explicit regular price if present (originalPrice or price_regular)
    if "originalPrice" in r.columns:
        explicit_regular = r["originalPrice"].apply(_to_float)
    elif "price_regular" in r.columns:
        explicit_regular = r["price_regular"].apply(_to_float)
    else:
        explicit_regular = _blank_series(r, None)
    r["_price_regular"] = explicit_regular

    # MSRP typically absent in eBay Browse
    r["_msrp"] = _blank_series(r, None)

    # Choose regular + source (based on explicit regular vs msrp; current price is not needed here)
    chosen_vals, chosen_srcs = [], []
    for reg, ms in zip(r["_price_regular"], r["_msrp"]):
        chosen, src = _choose_regular(reg, ms)
        chosen_vals.append(chosen)
        chosen_srcs.append(src)
    r["_price_regular_chosen"] = chosen_vals
    r["_regular_price_source"] = chosen_srcs

    # -----------------------
    # Shipping / Landed
    # -----------------------
    ship = r["shipping_cost"] if "shipping_cost" in r.columns else _blank_series(r, None)
    r["_shipping_cost"] = ship.apply(_to_float)
    r["_landed_price"]  = [_landed(c, s) for c, s in zip(r["_price_current"], r["_shipping_cost"])]

    # -----------------------
    # Promo flags & discount depth
    # -----------------------
    # "Explicit" promo if original (regular) exists and current < regular
    explicit_promo_series = []
    base_reg_series = r["_price_regular"] if r["_price_regular"] is not None else _blank_series(r, None)
    for cur, reg in zip(r["_price_current"], base_reg_series):
        explicit_promo_series.append(bool(cur is not None and reg is not None and cur < reg))

    promo_flags, promo_methods, disc_depths = [], [], []
    for cur, reg_chosen, exp in zip(r["_price_current"], r["_price_regular_chosen"], explicit_promo_series):
        pf, pm, dd = _promo_and_depth(cur, reg_chosen, bool(exp))
        promo_flags.append(pf)
        promo_methods.append(pm)
        # Optionally round for stability
        disc_depths.append(round(dd, 6) if isinstance(dd, float) else dd)

    r["_promo_flag"] = promo_flags
    r["_promo_detect_method"] = promo_methods
    r["_discount_depth"] = disc_depths

    # -----------------------
    # Availability (best-effort)
    # -----------------------
    avail = r["availabilityStatus"] if "availabilityStatus" in r.columns else _blank_series(r, None)
    in_stock, msg = [], []
    for a in avail:
        if isinstance(a, str) and a.upper() == "IN_STOCK":
            in_stock.append(True);  msg.append(a)
        elif isinstance(a, str) and a.upper() == "OUT_OF_STOCK":
            in_stock.append(False); msg.append(a)
        else:
            in_stock.append(None);  msg.append(a)
    r["_in_stock_flag"] = in_stock
    r["_availability_message"] = msg

    # -----------------------
    # Ratings (often not present in eBay Browse)
    # -----------------------
    r["_rating_avg"] = _blank_series(r, None)
    r["_rating_count"] = _blank_series(r, None)

    # -----------------------
    # Assemble pre-silver → finalize to silver
    # -----------------------
    ingest_run_id = str(uuid.uuid4())
    out = pd.DataFrame({
        "snapshot_date": _today_iso(),
        "captured_at": _now_utc_iso(),
        "retailer_id": "EBAY",
        "native_item_id": r["_native_item_id"],
        "upc": r["_upc"],
        "title_raw": r["_title_raw"],
        "brand_raw": r["_brand_raw"],
        "model_raw": r["_model_raw"],
        "category_raw_path": r["_category_raw_path"],
        "product_url": r["_product_url"],
        "currency": r["_currency"],
        "price_current": r["_price_current"],
        "price_regular": r["_price_regular"],
        "msrp": r["_msrp"],
        "price_regular_chosen": r["_price_regular_chosen"],
        "regular_price_source": r["_regular_price_source"],
        "shipping_cost": r["_shipping_cost"],
        "landed_price": r["_landed_price"],
        "promo_flag": r["_promo_flag"],
        "promo_detect_method": r["_promo_detect_method"],
        "discount_depth": r["_discount_depth"],
        "in_stock_flag": r["_in_stock_flag"],
        "availability_message": r["_availability_message"],
        "rating_avg": r["_rating_avg"],
        "rating_count": r["_rating_count"],
        "source_endpoint": "browse:item",
        "ingest_run_id": ingest_run_id,
        "ingest_status": "ok",
    })

    # _finalize should handle schema enforcement, dtype casting, keys, validation, etc.
    return _finalize(out, "EBAY", "browse:item", ingest_run_id)

