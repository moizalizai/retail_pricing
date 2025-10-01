import uuid, datetime as dt
import io, os, json
from azure.storage.blob import BlobServiceClient
import pandas as pd
from typing import Optional, Tuple

RAW_CONTAINER = os.getenv("RAW_CONTAINER", "retail-data")  
RAW_PREFIX    = os.getenv("RAW_PREFIX", "raw") 

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

def _blob_service() -> BlobServiceClient:
    conn = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    return BlobServiceClient.from_connection_string(conn)

def _list_latest_blob(source: str):
    """
    Return (blob_name, kind) for the most recently modified blob under raw/{source}/.
    kind ∈ {'jsonl','json','csv'} (guessed from extension). None if none found.
    """
    svc = _blob_service()
    container = svc.get_container_client(RAW_CONTAINER)
    prefix = f"{RAW_PREFIX}/{source}/"
    latest = None  # (name, last_modified)
    for b in container.list_blobs(name_starts_with=prefix):
        if latest is None or b.last_modified > latest[1]:
            latest = (b.name, b.last_modified)
    if latest is None:
        return None
    name = latest[0]
    if name.endswith(".jsonl"):
        return name, "jsonl"
    if name.endswith(".json"):
        return name, "json"
    return name, "csv"

def _download_blob_text(blob_name: str) -> str:
    svc = _blob_service()
    container = svc.get_container_client(RAW_CONTAINER)
    downloader = container.download_blob(blob_name)
    return downloader.readall().decode("utf-8", errors="replace")

def _df_from_jsonl(s: str) -> pd.DataFrame:
    rows = [json.loads(line) for line in s.splitlines() if line.strip()]
    return pd.json_normalize(rows) if rows else pd.DataFrame()

def _df_from_json(s: str) -> pd.DataFrame:
    obj = json.loads(s)
    if isinstance(obj, list):
        return pd.json_normalize(obj)
    if isinstance(obj, dict):
        for k in ("items", "data", "results"):
            if k in obj and isinstance(obj[k], list):
                return pd.json_normalize(obj[k])
        return pd.json_normalize(obj)
    return pd.DataFrame()

def read_raw_latest(source: str) -> pd.DataFrame:
    """
    List blobs under raw/{source}/ in RAW_CONTAINER, pick the most recent,
    download, parse to DataFrame. Adds snapshot metadata if missing.
    """
    found = _list_latest_blob(source)
    if not found:
        return pd.DataFrame()
    name, kind = found
    text = _download_blob_text(name)
    if kind == "jsonl":
        df = _df_from_jsonl(text)
    elif kind == "json":
        df = _df_from_json(text)
    else:
        df = pd.read_csv(io.StringIO(text))
    if "snapshot_date" not in df.columns:
        df["snapshot_date"] = _today_iso()
    if "captured_at" not in df.columns:
        df["captured_at"] = _now_utc_iso()
    return df

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
