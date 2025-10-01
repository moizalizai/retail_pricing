# silver_utils.py
# ----------------
import uuid, datetime as dt
import io, os, json
from typing import Optional, Tuple, List, Dict

import pandas as pd
from azure.storage.blob import BlobServiceClient

# ----------------
# Config (env-overridable)
# ----------------
RAW_CONTAINER   = os.getenv("RAW_CONTAINER",   "retail-data")
RAW_PREFIX      = os.getenv("RAW_PREFIX",      "raw")

SILVER_CONTAINER = os.getenv("SILVER_CONTAINER", RAW_CONTAINER)  # default to same container
SILVER_PREFIX    = os.getenv("SILVER_PREFIX",    "silver")

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

# ----------------
# Time helpers
# ----------------
def _today_iso() -> str:
    return dt.date.today().isoformat()

def _now_utc_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

# ----------------
# Azure helpers
# ----------------
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

def _download_blob_text(container_name: str, blob_name: str) -> str:
    svc = _blob_service()
    container = svc.get_container_client(container_name)
    downloader = container.download_blob(blob_name)
    return downloader.readall().decode("utf-8", errors="replace")

def _upload_blob_text(container_name: str, blob_name: str, text: str, overwrite: bool = True):
    svc = _blob_service()
    container = svc.get_container_client(container_name)
    container.upload_blob(name=blob_name, data=text.encode("utf-8"), overwrite=overwrite)

def _blob_exists(container_name: str, blob_name: str) -> bool:
    svc = _blob_service()
    container = svc.get_container_client(container_name)
    try:
        container.get_blob_client(blob_name).get_blob_properties()
        return True
    except Exception:
        return False

# ----------------
# Raw readers
# ----------------
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
    text = _download_blob_text(RAW_CONTAINER, name)
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

# ----------------
# Coercers & pricing helpers
# ----------------
def _to_float(x) -> Optional[float]:
    try:
        if x is None: return None
        s = str(x).strip().replace(",", "").replace("$", "")
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

def _to_bool(x) -> Optional[bool]:
    if x is None:
        return None
    s = str(x).strip().lower()
    if s in {"1","true","t","y","yes"}:
        return True
    if s in {"0","false","f","n","no"}:
        return False
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

# ----------------
# Normalization & schema
# ----------------
def standardize_brand_title(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean brand/title text minimally and consistently.
    """
    if df is None or len(df) == 0:
        return pd.DataFrame(columns=SILVER_COLS)

    df = df.copy()
    if "brand_raw" in df.columns:
        df["brand_raw"] = df["brand_raw"].astype(str).str.strip()
        # keep original casing but collapse whitespace
        df["brand_raw"] = df["brand_raw"].str.replace(r"\s+", " ", regex=True)
    if "title_raw" in df.columns:
        df["title_raw"] = df["title_raw"].astype(str).str.strip()
        df["title_raw"] = df["title_raw"].str.replace(r"\s+", " ", regex=True)
    return df

def normalize_currency(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure currency codes are normalized to ISO-like (USD, EUR, GBP, CAD, etc.).
    Does NOT perform FX conversion.
    """
    if df is None or len(df) == 0:
        return pd.DataFrame(columns=SILVER_COLS)

    symbol_to_code = {
        "$": "USD", "US$": "USD",
        "£": "GBP",
        "€": "EUR",
        "C$": "CAD", "CA$": "CAD",
        "A$": "AUD", "AU$": "AUD",
        "¥": "JPY",
        "₹": "INR",
        "₩": "KRW",
        "₽": "RUB",
        "HK$": "HKD",
        "NT$": "TWD",
        "₫": "VND",
        "R$": "BRL",
        "₱": "PHP",
        "₦": "NGN",
        "CHF": "CHF",
        "MX$": "MXN", "MXN": "MXN",
    }

    df = df.copy()
    if "currency" in df.columns:
        cur = df["currency"].astype(str).str.strip()
        # map symbols when column has symbol-like entries
        df["currency"] = cur.map(lambda x: symbol_to_code.get(x, x)).str.upper()
    else:
        df["currency"] = None
    return df

def normalize_units(df: pd.DataFrame) -> pd.DataFrame:
    """
    Placeholder for unit normalization (weights, sizes, etc.).
    For now, ensures numeric price fields are clean floats and rating_count is int.
    """
    if df is None or len(df) == 0:
        return pd.DataFrame(columns=SILVER_COLS)

    df = df.copy()
    for c in ["price_current","price_regular","msrp","shipping_cost","rating_avg","discount_depth","landed_price"]:
        if c in df.columns:
            df[c] = df[c].map(_to_float)
    if "rating_count" in df.columns:
        df["rating_count"] = df["rating_count"].map(_to_int)
    return df

def map_categories(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize category path delimiters to ' > ' and trim.
    (Hook point for mapping to a controlled taxonomy if/when you add a map.)
    """
    if df is None or len(df) == 0:
        return pd.DataFrame(columns=SILVER_COLS)

    df = df.copy()
    if "category_raw_path" in df.columns:
        col = df["category_raw_path"].astype(str)
        col = col.str.replace(r"\s*[/|>]\s*", " > ", regex=True)
        col = col.str.replace(r"\s+", " ", regex=True).str.strip(" >")
        df["category_raw_path"] = col
    return df

def compute_effective_price(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute price_regular_chosen, regular_price_source, promo fields, landed_price.
    Respects explicit promo flags if present.
    """
    if df is None or len(df) == 0:
        return pd.DataFrame(columns=SILVER_COLS)

    df = df.copy()

    # Ensure inputs exist
    for c in ["price_current","price_regular","msrp","shipping_cost","promo_flag"]:
        if c not in df.columns:
            df[c] = None

    # Coerce numerics/bools needed for computation
    df["price_current"] = df["price_current"].map(_to_float)
    df["price_regular"] = df["price_regular"].map(_to_float)
    df["msrp"]          = df["msrp"].map(_to_float)
    df["shipping_cost"] = df["shipping_cost"].map(_to_float)
    df["promo_flag"]    = df["promo_flag"].map(_to_bool).fillna(False)

    # Choose regular
    chosen, src = [], []
    for r in df.itertuples(index=False):
        c, how = _choose_regular(getattr(r, "price_regular", None),
                                 getattr(r, "msrp", None))
        chosen.append(c); src.append(how)
    df["price_regular_chosen"] = chosen
    df["regular_price_source"] = src

    # Landed price
    df["landed_price"] = df.apply(lambda r: _landed(r.get("price_current"), r.get("shipping_cost")), axis=1)

    # Promo & depth
    promo, method, depth = [], [], []
    for r in df.itertuples(index=False):
        p, m, d = _promo_and_depth(
            getattr(r, "price_current", None),
            getattr(r, "price_regular_chosen", None),
            bool(getattr(r, "promo_flag", False)),
        )
        promo.append(p); method.append(m); depth.append(d)
    df["promo_flag"] = promo
    df["promo_detect_method"] = method
    df["discount_depth"] = depth

    return df

def validate_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enforce SILVER_COLS presence, types, and ordering. Non-destructive for unknown columns.
    """
    if df is None or len(df) == 0:
        return pd.DataFrame(columns=SILVER_COLS)

    df = df.copy()

    # Coerce identifiers / strings
    for c in ["native_item_id","upc","title_raw","brand_raw","model_raw",
              "category_raw_path","product_url","currency",
              "availability_message","source_endpoint","regular_price_source","ingest_status"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    # Currency normalization
    df = normalize_currency(df)

    # Units / numerics
    df = normalize_units(df)

    # Ratings/int coercion
    if "rating_count" in df.columns:
        df["rating_count"] = df["rating_count"].map(_to_int)

    # Dates
    if "snapshot_date" in df.columns:
        df["snapshot_date"] = pd.to_datetime(df["snapshot_date"], errors="coerce").dt.date.astype("string")
    else:
        df["snapshot_date"] = _today_iso()
    if "captured_at" in df.columns:
        df["captured_at"] = pd.to_datetime(df["captured_at"], errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%SZ").astype("string")
    else:
        df["captured_at"] = _now_utc_iso()

    # Ensure required columns exist
    for c in SILVER_COLS:
        if c not in df.columns:
            df[c] = None

    # Canonical order
    df = df[SILVER_COLS]
    return df

def make_silver_keys(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure `native_item_id` exists. If missing/blank, derive a stable UUID5 from product_url or title+brand.
    """
    if df is None or len(df) == 0:
        return pd.DataFrame(columns=SILVER_COLS)

    df = df.copy()
    if "native_item_id" not in df.columns:
        df["native_item_id"] = None

    mask_missing = df["native_item_id"].isna() | (df["native_item_id"].astype(str).str.strip() == "")
    if mask_missing.any():
        ns = uuid.NAMESPACE_URL
        def _derive(row):
            url = str(row.get("product_url") or "").strip()
            if url:
                return str(uuid.uuid5(ns, url))
            combo = (str(row.get("brand_raw") or "").strip() + "|" + str(row.get("title_raw") or "").strip())
            if combo.strip("|"):
                return str(uuid.uuid5(ns, combo))
            return str(uuid.uuid4())
        df.loc[mask_missing, "native_item_id"] = df[mask_missing].apply(_derive, axis=1)

    # Make sure as string
    df["native_item_id"] = df["native_item_id"].astype(str)
    return df

# ----------------
# Finalize & Upsert to Silver
# ----------------
def _finalize(df: pd.DataFrame, retailer_id: str, source_endpoint: str, ingest_run_id: str) -> pd.DataFrame:
    df = df.copy()
    df["snapshot_date"]   = df.get("snapshot_date", pd.Series([_today_iso()]*len(df)))
    df["captured_at"]     = df.get("captured_at",   pd.Series([_now_utc_iso()]*len(df)))
    df["retailer_id"]     = retailer_id
    df["source_endpoint"] = source_endpoint
    df["ingest_run_id"]   = ingest_run_id
    df["ingest_status"]   = df.get("ingest_status", "ok")
    # Ensure all columns exist and order them
    for c in SILVER_COLS:
        if c not in df.columns:
            df[c] = None
    df = df[SILVER_COLS]
    return df

def _merge_dedupe(existing: pd.DataFrame, incoming: pd.DataFrame, key_cols: List[str]) -> pd.DataFrame:
    """
    Simple upsert: keep latest by captured_at for each key.
    """
    if existing is None or len(existing) == 0:
        base = incoming.copy()
    else:
        base = pd.concat([existing, incoming], ignore_index=True)

    # Coerce captured_at to sortable timestamp
    if "captured_at" in base.columns:
        ts = pd.to_datetime(base["captured_at"], errors="coerce")
        base["_ts"] = ts
    else:
        base["_ts"] = pd.Timestamp.utcnow()

    # Sort and drop duplicates keeping the newest record per key
    base = base.sort_values(by=["_ts"], ascending=True)
    base = base.drop(columns=["_ts"])
    base = base.drop_duplicates(subset=key_cols, keep="last")

    # Return in canonical order
    cols = [c for c in SILVER_COLS if c in base.columns] + [c for c in base.columns if c not in SILVER_COLS]
    return base[cols]

def upsert_to_silver(
    df: pd.DataFrame,
    retailer_id: str,
    source_endpoint: str,
    ingest_run_id: str,
    key_cols: Optional[List[str]] = None,
) -> List[str]:
    """
    Upsert `df` into silver storage (Azure Blob) partitioned by `snapshot_date` and `retailer_id`.

    Writes:
      - silver/{retailer_id}/snapshot_date=YYYY-MM-DD/run_{ingest_run_id}.csv   (audit)
      - silver/{retailer_id}/snapshot_date=YYYY-MM-DD/current.csv              (merged)

    Returns a list of blob names written.
    """
    if key_cols is None:
        key_cols = ["retailer_id","native_item_id"]

    if df is None or len(df) == 0:
        return []

    # Basic pipeline: tidy → compute → validate → keys → finalize
    df = standardize_brand_title(df)
    df = map_categories(df)
    df = normalize_currency(df)
    df = normalize_units(df)
    df = compute_effective_price(df)
    df = validate_schema(df)
    df = make_silver_keys(df)
    df = _finalize(df, retailer_id=retailer_id, source_endpoint=source_endpoint, ingest_run_id=ingest_run_id)

    written: List[str] = []
    # May contain multiple snapshot_dates; upsert each partition separately
    for snap_date, part in df.groupby("snapshot_date", dropna=False):
        snap = str(snap_date) if pd.notna(snap_date) else _today_iso()
        base_path = f"{SILVER_PREFIX}/{retailer_id}/snapshot_date={snap}"
        run_blob   = f"{base_path}/run_{ingest_run_id}.csv"
        current_blob = f"{base_path}/current.csv"

        # Write audit/run file
        _upload_blob_text(SILVER_CONTAINER, run_blob, part.to_csv(index=False))
        written.append(run_blob)

        # Merge with existing current.csv if present
        if _blob_exists(SILVER_CONTAINER, current_blob):
            old_text = _download_blob_text(SILVER_CONTAINER, current_blob)
            existing = pd.read_csv(io.StringIO(old_text))
        else:
            existing = pd.DataFrame(columns=part.columns)

        merged = _merge_dedupe(existing, part, key_cols=key_cols)
        _upload_blob_text(SILVER_CONTAINER, current_blob, merged.to_csv(index=False))
        written.append(current_blob)

    return written

# ----------------
# Public exports
# ----------------
__all__ = [
    # readers
    "read_raw_latest",
    # normalizers
    "validate_schema",
    "normalize_currency",
    "normalize_units",
    "standardize_brand_title",
    "map_categories",
    "compute_effective_price",
    "make_silver_keys",
    # writer
    "upsert_to_silver",
    # constants/schema
    "SILVER_COLS",
]

