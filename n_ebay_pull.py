# n_ebay_pull.py
import os, sys, time, uuid, base64, pathlib, datetime as dt
from typing import Iterable, List, Dict
import requests
import pandas as pd
from azure.storage.blob import BlobServiceClient

# -----------------------
# Config / env
# -----------------------
BATCH_SIZE   = int(os.getenv("EBAY_BATCH_SIZE", "20"))
RATE_SLEEP   = float(os.getenv("EBAY_RATE_SLEEP", "0.30"))
CONTAINER    = os.getenv("RAW_CONTAINER", "retail-data")
SNAPSHOT_DATE = dt.date.today().isoformat()

EBAY_CLIENT_ID      = os.getenv("EBAY_CLIENT_ID")
EBAY_CLIENT_SECRET  = os.getenv("EBAY_CLIENT_SECRET")
EBAY_OAUTH_ENV      = (os.getenv("EBAY_OAUTH_ENV", "PRODUCTION") or "PRODUCTION").upper()  # PRODUCTION or SANDBOX
EBAY_MARKETPLACE_ID = os.getenv("EBAY_MARKETPLACE_ID", "EBAY_US")

AZ_CONN = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
if not AZ_CONN:
    print("[ERROR] AZURE_STORAGE_CONNECTION_STRING is not set", file=sys.stderr)
    sys.exit(1)

# -----------------------
# Azure helpers
# -----------------------
def _blob_client():
    return BlobServiceClient.from_connection_string(AZ_CONN)

def upload_df_csv(df: pd.DataFrame, container: str, blob_path: str):
    svc = _blob_client()
    cc = svc.get_container_client(container)
    cc.upload_blob(name=blob_path, data=df.to_csv(index=False).encode("utf-8"), overwrite=True)

# -----------------------
# eBay OAuth + endpoints
# -----------------------
def _oauth_host() -> str:
    return "api.ebay.com" if EBAY_OAUTH_ENV == "PRODUCTION" else "api.sandbox.ebay.com"

def _browse_host() -> str:
    return "api.ebay.com" if EBAY_OAUTH_ENV == "PRODUCTION" else "api.sandbox.ebay.com"

EBAY_TOKEN_URL        = f"https://{_oauth_host()}/identity/v1/oauth2/token"
EBAY_DETAILS_PREFIX   = f"https://{_browse_host()}/buy/browse/v1/item"  # GET /{item_id}

_cached_token: str | None = None
_cached_expiry: float = 0.0

def _now_utc_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def _get_ebay_access_token() -> str:
    global _cached_token, _cached_expiry
    now = time.time()
    if _cached_token and now < _cached_expiry - 60:
        return _cached_token
    if not EBAY_CLIENT_ID or not EBAY_CLIENT_SECRET:
        print("[ERROR] EBAY_CLIENT_ID/EBAY_CLIENT_SECRET not set", file=sys.stderr)
        sys.exit(1)

    basic = base64.b64encode(f"{EBAY_CLIENT_ID}:{EBAY_CLIENT_SECRET}".encode()).decode()
    headers = {
        "Authorization": f"Basic {basic}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    # IMPORTANT: Application access token for Browse = base scope ONLY
    data = {
        "grant_type": "client_credentials",
        "scope": "https://api.ebay.com/oauth/api_scope",
    }
    r = requests.post(EBAY_TOKEN_URL, headers=headers, data=data, timeout=30)
    if r.status_code != 200:
        print(f"[ERROR] eBay token failed {r.status_code}: {r.text[:300]}", file=sys.stderr)
        sys.exit(1)
    tok = r.json()
    _cached_token = tok["access_token"]
    _cached_expiry = now + int(tok.get("expires_in", 7200))
    return _cached_token

def ebay_headers() -> dict:
    return {
        "Authorization": f"Bearer {_get_ebay_access_token()}",
        "Content-Type": "application/json",
        "X-EBAY-C-MARKETPLACE-ID": EBAY_MARKETPLACE_ID,  # e.g. EBAY_US
    }

# Search endpoint (prod/sandbox aware)
EBAY_SEARCH_URL = f"https://{_browse_host()}/buy/browse/v1/item_summary/search"

def search_by_gtin(upcs, per_upc_limit=10):
    """
    Look up eBay items by GTIN (UPC/EAN). Returns a list of dicts with ebay_item_id, url, etc.
    Requires only the base scope: https://api.ebay.com/oauth/api_scope
    """
    out = []
    hdrs = ebay_headers()  # uses your token + X-EBAY-C-MARKETPLACE-ID
    for upc in upcs:
        params = {"gtin": upc, "limit": per_upc_limit}
        try:
            r = requests.get(EBAY_SEARCH_URL, headers=hdrs, params=params, timeout=20)
            if r.status_code == 200:
                js = r.json() or {}
                for it in js.get("itemSummaries", []):
                    out.append({
                        "upc": upc,
                        "ebay_item_id": it.get("itemId"),
                        "viewItemURL": it.get("itemWebUrl") or it.get("itemHref"),
                        "title": it.get("title"),
                        "price": (it.get("price") or {}).get("value"),
                        "currency": (it.get("price") or {}).get("currency"),
                    })
            elif r.status_code in (429, 500, 502, 503, 504):
                time.sleep(1.0)
                r2 = requests.get(EBAY_SEARCH_URL, headers=hdrs, params=params, timeout=20)
                if r2.status_code == 200:
                    js = r2.json() or {}
                    for it in js.get("itemSummaries", []):
                        out.append({
                            "upc": upc,
                            "ebay_item_id": it.get("itemId"),
                            "viewItemURL": it.get("itemWebUrl") or it.get("itemHref"),
                            "title": it.get("title"),
                            "price": (it.get("price") or {}).get("value"),
                            "currency": (it.get("price") or {}).get("currency"),
                        })
            else:
                print(f"[WARN] search UPC {upc} -> {r.status_code}: {r.text[:160]}")
        except requests.RequestException as e:
            print(f"[ERROR] GTIN search error for {upc}: {e}")
        time.sleep(RATE_SLEEP)

    # de-dupe by ebay_item_id
    dedup = {}
    for row in out:
        eid = row.get("ebay_item_id")
        if eid and eid not in dedup:
            dedup[eid] = row
    return list(dedup.values())


# -----------------------
# Load seed IDs (from your mapping)
# -----------------------
def load_ebay_matches(path: str = "ebay_matches.csv") -> pd.DataFrame:
    p = pathlib.Path(path)
    if not p.exists():
        print(f"[ERROR] {path} not found", file=sys.stderr)
        sys.exit(1)
    df = pd.read_csv(p)
    if "ebay_item_id" not in df.columns and "itemId" not in df.columns:
        print("[ERROR] ebay_matches.csv needs ebay_item_id or itemId column", file=sys.stderr)
        sys.exit(1)
    if "ebay_item_id" not in df.columns and "itemId" in df.columns:
        df = df.rename(columns={"itemId": "ebay_item_id"})
    df["ebay_item_id"] = df["ebay_item_id"].astype(str)
    df = df.drop_duplicates(subset=["ebay_item_id"]).reset_index(drop=True)
    return df

# -----------------------
# Call eBay API
# -----------------------
def request_items_by_id(ids: Iterable[str]) -> List[Dict]:
    out: List[Dict] = []
    hdrs = ebay_headers()
    for item_id in ids:
        url = f"{EBAY_DETAILS_PREFIX}/{item_id}"
        try:
            resp = requests.get(url, headers=hdrs, timeout=20)
            if resp.status_code == 200:
                out.append(resp.json())
            elif resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(1.0)
                resp2 = requests.get(url, headers=hdrs, timeout=20)
                if resp2.status_code == 200:
                    out.append(resp2.json())
            elif resp.status_code != 404:
                print(f"[WARN] eBay {resp.status_code} for {item_id}: {resp.text[:160]}", file=sys.stderr)
        except requests.RequestException as e:
            print(f"[ERROR] eBay request error for {item_id}: {e}", file=sys.stderr)
        time.sleep(RATE_SLEEP)
    return out

# -----------------------
# Parse eBay item (fields your normalizer will coalesce)
# -----------------------
def _g(d: dict, *path):
    cur = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return None
        cur = cur[p]
    return cur

def parse_ebay_item(it: dict) -> dict:
    ebay_id  = it.get("itemId") or it.get("legacyItemId")
    url      = it.get("itemWebUrl") or it.get("viewItemURL") or it.get("itemWebURL")
    cur_val  = _g(it, "price", "value") or _g(it, "currentPrice", "value") or it.get("currentPrice")
    cur_ccy  = _g(it, "price", "currency") or _g(it, "currentPrice", "currency") or it.get("currency")
    orig_val = _g(it, "marketingPrice", "originalPrice", "value") or it.get("originalPrice")
    ship     = None
    opts = it.get("shippingOptions") or []
    if isinstance(opts, list) and opts:
        ship = _g(opts[0], "shippingCost", "value") or opts[0].get("shippingServiceCost")

    return {
        "ebay_item_id": ebay_id,
        "itemId": ebay_id,
        "itemWebUrl": url,
        "viewItemURL": url,
        "title": it.get("title") or it.get("shortDescription"),
        "brand": it.get("brand") or it.get("itemBrand"),
        "mpn": it.get("mpn") or it.get("model"),
        "currentPrice": cur_val,
        "price": cur_val,
        "originalPrice": orig_val,
        "currency": cur_ccy,
        "shippingServiceCost": ship,
        "availabilityStatus": it.get("availabilityStatus"),
        "upc": it.get("upc") or it.get("gtin"),
        "ean": it.get("ean"),
        "gtin": it.get("gtin"),
    }

# -----------------------
# Write RAW snapshot
# -----------------------
def write_snapshot_ebay(df: pd.DataFrame, ingest_run_id: str | None = None) -> str:
    if df is None or df.empty:
        raise ValueError("write_snapshot_ebay: received empty dataframe")

    run_id = ingest_run_id or os.getenv("INGEST_RUN_ID") or os.getenv("GITHUB_RUN_ID") or uuid.uuid4().hex[:8]

    df = df.copy()
    df["snapshot_date"] = SNAPSHOT_DATE
    df["captured_at"]   = _now_utc_iso()
    df["retailer_id"]   = "ebay"  # critical

    if "ebay_item_id" not in df.columns and "itemId" in df.columns:
        df.rename(columns={"itemId": "ebay_item_id"}, inplace=True)

    blob_path = f"raw/ebay/daily/{SNAPSHOT_DATE}/run_id={run_id}/ebay_snapshot_{SNAPSHOT_DATE}_{run_id}.csv"
    upload_df_csv(df=df, container=CONTAINER, blob_path=blob_path)
    print(f"[OK] Uploaded RAW eBay → {blob_path}")
    return blob_path

def run_pull_and_write_raw(ingest_run_id: str | None = None) -> str:
    # Prefer explicit matches file
    ids = []
    try:
        matches = load_ebay_matches("ebay_matches.csv")
        ids = matches["ebay_item_id"].dropna().astype(str).unique().tolist()
    except SystemExit:
        pass  # no file; we’ll fallback
    except Exception as e:
        print(f"[WARN] could not load ebay_matches.csv: {e}")

    # Fallback to GTIN search from master_skus.csv
    if not ids:
        if not pathlib.Path("master_skus.csv").exists():
            raise RuntimeError("No ebay_matches.csv and no master_skus.csv to search by GTIN")
        master = pd.read_csv("master_skus.csv")
        upcs = master.get("upc", pd.Series([], dtype=str)).dropna().astype(str).unique().tolist()
        if not upcs:
            raise RuntimeError("No UPCs found to search on")
        print(f"[INFO] Searching eBay by GTIN for {len(upcs)} UPCs...")
        found = search_by_gtin(upcs, per_upc_limit=5)
        if not found:
            raise RuntimeError("GTIN search returned no items")
        # Optional: persist a fresh mapping for next runs
        pd.DataFrame(found)[["upc","ebay_item_id","viewItemURL"]].to_csv("ebay_matches.csv", index=False)
        ids = [r["ebay_item_id"] for r in found if r.get("ebay_item_id")]

    # Pull full item details by id
    rows = []
    for chunk in chunked(ids, BATCH_SIZE):
        items = request_items_by_id(chunk)
        for it in items:
            try:
                rows.append(parse_ebay_item(it))
            except Exception as e:
                print(f"[WARN] parse error: {e}")

    if not rows:
        raise RuntimeError("eBay: no rows parsed from details API")
    df = pd.DataFrame(rows)
    return write_snapshot_ebay(df, ingest_run_id=ingest_run_id)



# -----------------------
# Main
# -----------------------
def chunked(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

def main():
    matches = load_ebay_matches("ebay_matches.csv")
    ids = matches["ebay_item_id"].dropna().astype(str).unique().tolist()
    if not ids:
        print("[WARN] No eBay item IDs found in ebay_matches.csv", file=sys.stderr)
        sys.exit(0)

    rows: List[Dict] = []
    for part in chunked(ids, BATCH_SIZE):
        items = request_items_by_id(part)
        for it in items:
            try:
                rows.append(parse_ebay_item(it))
            except Exception as e:
                print(f"[WARN] parse error: {e}", file=sys.stderr)

    if not rows:
        print("[WARN] No rows parsed from eBay; check credentials/env/endpoints", file=sys.stderr)
        sys.exit(0)

    df = pd.DataFrame(rows)
    write_snapshot_ebay(df)

if __name__ == "__main__":
    main()
