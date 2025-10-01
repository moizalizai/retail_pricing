import uuid
import pandas as pd
from silver_utils import (
    SILVER_COLS, _to_float, _to_int, _choose_regular, _promo_and_depth,
    _landed, _today_iso, _now_utc_iso, _finalize
)

def coalesce(df, *cols):
    s = pd.Series([None] * len(df), index=df.index)
    for c in cols:
        if c in df.columns:
            s = s.combine_first(df[c])
    return s

def normalize_walmart(df_raw: pd.DataFrame) -> pd.DataFrame:
    if df_raw is None or df_raw.empty:
        return pd.DataFrame(columns=SILVER_COLS)

    r = df_raw.copy()

    # IDs / UPC
    r["_native_item_id"] = coalesce(r, "walmart_item_id", "itemId").astype(str)
    upc = r["upc"].astype(str).str.strip().replace({"": None}) if "upc" in r.columns else pd.Series([None]*len(r), index=r.index)
    r["_upc"] = upc

    # Descriptive
    r["_title_raw"]         = coalesce(r, "title_raw", "name")
    r["_brand_raw"]         = coalesce(r, "brand_raw", "brandName")
    r["_model_raw"]         = coalesce(r, "model_raw", "modelNumber", "model")
    r["_category_raw_path"] = coalesce(r, "category_raw_path", "categoryPath")
    r["_product_url"]       = coalesce(r, "product_url", "productUrl", "productTrackingUrl")

    # Currency
    r["_currency"] = "USD"

    # Prices
    price_current = r["price_current"] if "price_current" in r.columns else coalesce(r, "salePrice", "price")
    r["_price_current"] = price_current.apply(_to_float)

    msrp = r["msrp"].apply(_to_float) if "msrp" in r.columns else pd.Series([None]*len(r), index=r.index)
    r["_msrp"] = msrp

    explicit_regular = r["price_regular"].apply(_to_float) if "price_regular" in r.columns else msrp
    r["_price_regular"] = explicit_regular

    # Choose regular + source
    chosen_vals, chosen_srcs = [], []
    for reg, ms in zip(r["_price_regular"], r["_msrp"]):
        chosen, src = _choose_regular(reg, ms)
        chosen_vals.append(chosen); chosen_srcs.append(src)
    r["_price_regular_chosen"] = chosen_vals
    r["_regular_price_source"] = chosen_srcs

    # Promo flags
    rollback   = r["rollback"].astype("boolean")  if "rollback"   in r.columns else pd.Series([False]*len(r), index=r.index)
    clearance  = r["clearance"].astype("boolean") if "clearance"  in r.columns else pd.Series([False]*len(r), index=r.index)
    is_on_sale = r["isOnSale"].astype("boolean")  if "isOnSale"   in r.columns else pd.Series([False]*len(r), index=r.index)
    explicit_promo_series = (rollback.fillna(False) | clearance.fillna(False) | is_on_sale.fillna(False))

    promo_flags, promo_methods, disc_depths = [], [], []
    for cur, reg, exp in zip(r["_price_current"], r["_price_regular_chosen"], explicit_promo_series):
        pf, pm, dd = _promo_and_depth(cur, reg, bool(exp))
        promo_flags.append(pf); promo_methods.append(pm); disc_depths.append(dd)
    r["_promo_flag"] = promo_flags
    r["_promo_detect_method"] = promo_methods
    r["_discount_depth"] = disc_depths

    # Availability
    stock_str     = r["stock"] if "stock" in r.columns else pd.Series([None]*len(r), index=r.index)
    available_onl = r["availableOnline"] if "availableOnline" in r.columns else pd.Series([None]*len(r), index=r.index)
    in_stock, msg = [], []
    for s, ao in zip(stock_str, available_onl):
        s_norm = str(s).strip().lower() if s is not None else ""
        if s_norm.startswith("avail"):
            in_stock.append(True);  msg.append(s)
        elif s_norm.startswith("out"):
            in_stock.append(False); msg.append(s)
        elif ao is not None:
            val = bool(ao)
            in_stock.append(val); msg.append("Available Online" if val else "Out of Stock")
        else:
            in_stock.append(None); msg.append(s)
    r["_in_stock_flag"] = in_stock
    r["_availability_message"] = msg

    # Ratings
    ra = r["rating_avg"] if "rating_avg" in r.columns else coalesce(r, "customerRating", "averageRating")
    r["_rating_avg"] = ra.apply(_to_float) if ra is not None else pd.Series([None]*len(r), index=r.index)

    rc = coalesce(r, "rating_count", "numReviews", "reviewCount", "customerRatingCount", "numberOfReviews")
    r["_rating_count"] = rc.apply(_to_int) if rc is not None else pd.Series([None]*len(r), index=r.index)

    # Shipping / Landed
    ship = r["shipping_cost"] if "shipping_cost" in r.columns else pd.Series([None]*len(r), index=r.index)
    r["_shipping_cost"] = ship.apply(_to_float)
    r["_landed_price"]  = [_landed(c, s) for c, s in zip(r["_price_current"], r["_shipping_cost"])]

    # Build Silver rows
    ingest_run_id = str(uuid.uuid4())
    out = pd.DataFrame({
        "snapshot_date": _today_iso(),
        "captured_at": _now_utc_iso(),
        "retailer_id": "WALMART",
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
        "source_endpoint": "items:responseGroup=full",
        "ingest_run_id": ingest_run_id,
        "ingest_status": "ok",
    })

    return _finalize(out, "WALMART", "items:responseGroup=full", ingest_run_id)

