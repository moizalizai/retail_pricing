import uuid, datetime as dt
import pandas as pd
from typing import Optional, Tuple

SILVER_COLS = [
    "snapshot_date","captured_at","retailer_id","native_item_id","upc",
    "title_raw","brand_raw","model_raw","category_raw_path","product_url","currency",
    "price_current","price_regular","msrp","price_regular_chosen","regular_price_source",
    "shipping_cost","landed_price",
    "promo_flag","promo_detect_method","discount_depth",
    "in_stock_flag","availability_message",
    "rating_avg","rating_count",
    "source_endpoint","ingest_run_id","ingest_status",
]

def _today_iso() -> str:
    return dt.date.today().isoformat()

def _now_utc_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def _to_float(x) -> Optional[float]:
    try:
        if x is None: return None
        s = str(x).strip().replace(",", "")
        return float(s)
    except Exception:
        return None

def _to_int(x) -> Optional[int]:
    try:
        if x is None: return None
        s = str(x).replace("+", "").replace(",", "").strip()
        return int(s)
    except Exception:
        return None

def _choose_regular(explicit_regular: Optional[float], msrp: Optional[float]) -> Tuple[Optional[float], str]:
    if explicit_regular is not None:
        return explicit_regular, "explicit_regular"
    if msrp is not None:
        return msrp, "msrp"
    return None, "unknown"

def _promo_and_depth(price_current: Optional[float], chosen_regular: Optional[float],
                     explicit_promo: bool) -> Tuple[bool, str, Optional[float]]:
    if price_current is None or chosen_regular is None or chosen_regular <= 0:
        return (explicit_promo, "explicit" if explicit_promo else "none", None)
    # explicit wins if present and a real discount
    if explicit_promo and price_current < chosen_regular:
        return True, "explicit", (chosen_regular - price_current)/chosen_regular
    # heuristic if 5%+ below chosen regular
    if price_current <= 0.95 * chosen_regular:
        return True, "heuristic_regular", (chosen_regular - price_current)/chosen_regular
    return False, "none", 0.0

def _landed(price_current: Optional[float], shipping_cost: Optional[float]) -> Optional[float]:
    if price_current is None: return None
    return price_current + (shipping_cost or 0.0)

def _finalize(df: pd.DataFrame, retailer_id: str, source_endpoint: str, ingest_run_id: str) -> pd.DataFrame:
    df = df.copy()
    df["snapshot_date"] = df.get("snapshot_date", pd.Series([_today_iso()]*len(df)))
    df["captured_at"]   = df.get("captured_at",   pd.Series([_now_utc_iso()]*len(df)))
    df["retailer_id"]   = retailer_id
    df["source_endpoint"] = source_endpoint
    df["ingest_run_id"] = ingest_run_id
    df["ingest_status"] = df.get("ingest_status", "ok")
    # Ensure all columns exist and order them
    for c in SILVER_COLS:
        if c not in df.columns:
            df[c] = None
    df = df[SILVER_COLS]
    return df
