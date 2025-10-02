import os, sys, time, uuid, pathlib, datetime as dt
from typing import Dict, Any, List, Optional, Tuple
from azure.storage.blob import BlobServiceClient, ContentSettings
from io import BytesIO
from dotenv import load_dotenv

import requests
import pandas as pd

from signer import walmart_headers  # <-- your signer

# -----------------------
# Config
# -----------------------
BASE = "https://developer.api.walmart.com/api-proxy/service/affil/product/v2"
DETAILS_EP = f"{BASE}/items"   # supports batching: ids=comma-separated
BATCH_SIZE = 20                # safe batch size (tune as you like)
RATE_SLEEP = 0.35              # seconds between calls (be gentle)
OUT_DIR = "out/walmart/daily"
WRITE_FORMAT = "csv"           # 'csv' or 'parquet'
SNAPSHOT_DATE = dt.date.today().isoformat()
CONTAINER = "retail-data"
ENABLE_SILVER = False

load_dotenv()

_blob_service_client = None

def _get_blob_client(container: str, blob_path: str):
    global _blob_service_client
    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not conn_str:
        raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING not set.")
    if _blob_service_client is None:
        _blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    return _blob_service_client.get_blob_client(container=container, blob=blob_path)

def upload_bytes(container: str, blob_path: str, data: bytes, content_type: str):
    """Upload raw bytes directly to a blob path (folders auto-created)."""
    bc = _get_blob_client(container, blob_path)
    bc.upload_blob(data, overwrite=True, content_settings=ContentSettings(content_type=content_type))
    print(f"[OK] Uploaded → {container}/{blob_path}")

def upload_df_csv(df: pd.DataFrame, container: str, blob_path: str):
    """Stream a DataFrame as CSV to Azure Blob (no temp file)."""
    buf = BytesIO()
    df.to_csv(buf, index=False)
    upload_bytes(container, blob_path, buf.getvalue(), content_type="text/csv")

def upload_df_parquet(df: pd.DataFrame, container: str, blob_path: str):
    """Stream a DataFrame as Parquet (snappy) to Azure Blob."""
    buf = BytesIO()
    df.to_parquet(buf, index=False)  # requires pyarrow
    upload_bytes(container, blob_path, buf.getvalue(), content_type="application/octet-stream")


# -----------------------
# Helpers
# -----------------------
def chunked(seq: List[str], n: int) -> List[List[str]]:
    return [seq[i:i+n] for i in range(0, len(seq), n)]

def load_master(path: str = "master_skus.csv") -> pd.DataFrame:
    if not pathlib.Path(path).exists():
        print(f"[ERROR] {path} not found", file=sys.stderr)
        sys.exit(1)
    df = pd.read_csv(path)
    if "walmart_itemId" not in df.columns:
        print("[ERROR] master_skus.csv must include walmart_itemId", file=sys.stderr)
        sys.exit(1)
    if "upc" not in df.columns:
        df["upc"] = None
    # force as text
    df["walmart_itemId"] = df["walmart_itemId"].astype(str)
    df["upc"] = df["upc"].astype(str).where(df["upc"].notna(), None)
    # Deduplicate by itemId
    df = df.drop_duplicates(subset=["walmart_itemId"]).reset_index(drop=True)
    return df

def request_items(ids: list) -> list:
    """Return a flat list of item dicts; splits batches if needed."""
    if not ids:
        return []
    params = {"ids": ",".join(ids), "responseGroup": "full"}
    try:
        r = requests.get(DETAILS_EP, headers=walmart_headers(), params=params, timeout=30)
        if r.status_code == 200:
            payload = r.json() or {}
            return payload.get("items", []) or payload.get("Items", []) or []
        # If batch too large or other 4xx, split and try halves
        if r.status_code == 400 and len(ids) > 1:
            mid = len(ids) // 2
            return request_items(ids[:mid]) + request_items(ids[mid:])
        # 429/5xx: brief backoff then retry once as singles
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(1.2)
            out = []
            for i in ids:
                out.extend(request_items([i]))
            return out
        print(f"[WARN] {r.status_code} {r.url}\n{r.text[:300]}")
        return []
    except requests.RequestException as e:
        print(f"[ERROR] {e}")
        return []


def as_float(x) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        return float(str(x).replace(",", "").strip())
    except Exception:
        return None

def parse_item(it: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map Affiliate v2 item to normalized fields.
    We’re conservative: if a field isn't there, we leave None.
    """
    # Raw/basic
    item_id = it.get("itemId")
    upc = (it.get("upc") or "").strip() or None
    name = it.get("name")
    brand = it.get("brandName")
    model = it.get("modelNumber") or it.get("model")
    category_path = it.get("categoryPath")
    url = it.get("productUrl") or it.get("productTrackingUrl") or it.get("productUrlText")

    # Prices
    price_curr = as_float(it.get("salePrice") or it.get("price"))
    msrp = as_float(it.get("msrp"))
    # Some categories expose strike-through/was via other nodes; if you see one, map it here:
    price_regular_raw = msrp  # prefer msrp; adjust if you find a better "was"/"listPrice" in your payloads
    # Promo flags (explicit)
    rollback = bool(it.get("rollback"))
    clearance = bool(it.get("clearance"))
    is_on_sale = bool(it.get("isOnSale"))
    promo_explicit = rollback or clearance or is_on_sale

    # Availability
    # Common patterns: 'stock' ("Available"/"Out of stock") or 'availableOnline' (True/False)
    stock_str = (it.get("stock") or "").strip()
    available_online = it.get("availableOnline")
    if isinstance(available_online, str):
        available_online = available_online.lower() == "true"
    in_stock = None
    if stock_str:
        in_stock = stock_str.lower().startswith("avail")  # "Available"
    elif available_online is not None:
        in_stock = bool(available_online)

    # Reviews
    rating_avg = None
    ra = it.get("customerRating") or it.get("averageRating")
    if ra is not None:
        try:
            rating_avg = float(str(ra))
        except Exception:
            rating_avg = None
    rating_count = None
    for k in ("numReviews", "reviewCount", "customerRatingCount", "numberOfReviews"):
        if it.get(k) is not None:
            try:
                rating_count = int(str(it.get(k)).replace("+", "").replace(",", ""))
                break
            except Exception:
                pass

    # Decide regular price + promo method + discount depth
    reg_price, reg_src = infer_regular_price(price_curr, price_regular_raw, msrp)
    promo_flag, promo_method, disc_depth = detect_promo(price_curr, reg_price, promo_explicit)

    return {
        "snapshot_date": SNAPSHOT_DATE,
        "captured_at": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "retailer_id": "WALMART",
        "walmart_item_id": str(item_id) if item_id is not None else None,
        "upc": upc,

        "title_raw": name,
        "brand_raw": brand,
        "model_raw": model,
        "category_raw_path": category_path,
        "product_url": url,
        "currency": "USD",

        "price_current": price_curr,
        "price_regular": price_regular_raw,
        "msrp": msrp,
        "price_regular_chosen": reg_price,
        "regular_price_source": reg_src,

        "promo_flag": promo_flag,
        "promo_detect_method": promo_method,   # explicit | heuristic_regular | none
        "discount_depth": disc_depth,

        "in_stock_flag": in_stock,
        "availability_message": stock_str or ( "Available Online" if in_stock else ("Out of Stock" if in_stock is False else None) ),
        "shipping_cost": None,  # Affiliate v2 usually doesn't return it; leave None

        "rating_avg": rating_avg,
        "rating_count": rating_count,

        "source_endpoint": "items:responseGroup=full"
    }

def infer_regular_price(current: Optional[float], explicit_regular: Optional[float], msrp: Optional[float]) -> Tuple[Optional[float], str]:
    if explicit_regular is not None:
        return explicit_regular, "explicit_regular_or_msrp"
    if msrp is not None:
        return msrp, "msrp"
    return None, "unknown"

def detect_promo(current: Optional[float], regular: Optional[float], explicit_flag: bool) -> Tuple[bool, str, Optional[float]]:
    if explicit_flag and (regular and current):
        return True, "explicit", (regular - current) / regular if regular > 0 else None
    if (regular and current) and regular > 0 and current < 0.95 * regular:
        return True, "heuristic_regular", (regular - current) / regular
    return False, "none", None

import uuid, datetime as dt

def _now_utc_iso():
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def write_snapshot_walmart(df: pd.DataFrame, ingest_run_id: str | None = None) -> str:
    if df is None or df.empty:
        raise ValueError("write_snapshot_walmart: empty df")
    run_id = ingest_run_id or os.getenv("INGEST_RUN_ID") or os.getenv("GITHUB_RUN_ID") or uuid.uuid4().hex[:8]
    df = df.copy()
    df["snapshot_date"] = SNAPSHOT_DATE
    df["captured_at"]   = _now_utc_iso()
    df["retailer_id"]   = "walmart"
    if "walmart_item_id" not in df.columns and "itemId" in df.columns:
        df.rename(columns={"itemId": "walmart_item_id"}, inplace=True)
    blob = f"raw/walmart/daily/{SNAPSHOT_DATE}/run_id={run_id}/walmart_snapshot_{SNAPSHOT_DATE}_{run_id}.csv"
    upload_df_csv(df=df, container=CONTAINER, blob_path=blob)  # uses your existing helper
    print(f"[OK] Uploaded RAW Walmart → {blob}")
    return blob

def run_pull_and_write_raw(ingest_run_id: str | None = None) -> str:
    master = load_master("master_skus.csv")                     # your existing function
    ids = master["walmart_itemId"].dropna().astype(str).tolist()
    rows = []
    for chunk in chunked(ids, 20):
        items = request_items(chunk)                            # your existing Walmart API call
        for it in items:
            rows.append(parse_item(it))                         # your existing parser
    if not rows:
        raise RuntimeError("Walmart: no rows parsed")
    df = pd.DataFrame(rows)
    return write_snapshot_walmart(df, ingest_run_id=ingest_run_id)


    # Upload SILVER Parquet (optional, standardized schema)
    if ENABLE_SILVER:
        try:
            upload_df_parquet(
                df=df,
                container=CONTAINER,
                blob_path=f"silver/daily_prices/{SNAPSHOT_DATE}/part-0000.parquet",
            )
            print(f"[OK] Uploaded Walmart Silver → silver/daily_prices/{SNAPSHOT_DATE}/")
        except Exception as e:
            print(f"[WARN] Silver parquet upload failed (install pyarrow?): {e}")

def main():
    master = load_master("master_skus.csv")
    item_ids = master["walmart_itemId"].dropna().astype(str).tolist()
    if not item_ids:
        print("[WARN] No walmart_itemId values found in master_skus.csv", file=sys.stderr)
        sys.exit(0)

    # IMPORTANT: define this before the loop and inside main()
    all_rows = []

    # harvest in batches of 20
    for chunk in chunked(item_ids, BATCH_SIZE):   # BATCH_SIZE should be 20
        items = request_items(chunk)              # returns List[Dict[str, Any]]
        for it in items:
            try:
                row = parse_item(it)
                all_rows.append(row)
            except Exception as e:
                print(f"[WARN] parse error for item {it.get('itemId')}: {e}", file=sys.stderr)
        time.sleep(RATE_SLEEP)                    # e.g., 0.35s

    if not all_rows:
        print("[WARN] No rows parsed; check API response & auth", file=sys.stderr)
        sys.exit(0)

    df = pd.DataFrame(all_rows)

    # Ensure consistent column order (Silver-aligned)
    cols = [
        "snapshot_date","captured_at","retailer_id","walmart_item_id","upc",
        "title_raw","brand_raw","model_raw","category_raw_path","product_url","currency",
        "price_current","price_regular","msrp","price_regular_chosen","regular_price_source",
        "promo_flag","promo_detect_method","discount_depth",
        "in_stock_flag","availability_message","shipping_cost",
        "rating_avg","rating_count","source_endpoint"
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]

    write_snapshot(df)


if __name__ == "__main__":
    main()