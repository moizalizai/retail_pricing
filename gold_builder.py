# gold_builder.py
import io
import os
import pandas as pd
from typing import List
from azure.storage.blob import BlobServiceClient

# ========= Config =========
GOLD_CONTAINER = os.getenv("GOLD_CONTAINER") or os.getenv("SILVER_CONTAINER") or os.getenv("RAW_CONTAINER", "retail-data")
CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
SILVER_PREFIX = os.getenv("SILVER_PREFIX", "silver")
SILVER_TABLE = os.getenv("SILVER_TABLE", "products_v1")   # where your normalized runs are written
GOLD_ROOT = os.getenv("GOLD_PREFIX", "gold") + "/prices/"

# ========= Azure helpers =========
def _svc() -> BlobServiceClient:
    if not CONN_STR:
        raise RuntimeError("Missing AZURE_STORAGE_CONNECTION_STRING")
    return BlobServiceClient.from_connection_string(CONN_STR)

def _container():
    return _svc().get_container_client(GOLD_CONTAINER)

def list_run_csvs() -> List[str]:
    """
    List all per-run normalized CSVs: silver/products_v1/snapshot_date=.../run_*.csv
    """
    prefix = f"{SILVER_PREFIX}/{SILVER_TABLE}/"
    paths = []
    for b in _container().list_blobs(name_starts_with=prefix):
        name = b.name
        if name.endswith(".csv") and "/snapshot_date=" in name and "/run_" in name:
            paths.append(name)
    return sorted(paths)

def read_csv_blob(path: str) -> pd.DataFrame:
    data = _container().download_blob(path).readall()
    return pd.read_csv(io.BytesIO(data))

def write_parquet_blob(df: pd.DataFrame, path: str):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    _container().upload_blob(name=path, data=buf.getvalue(), overwrite=True)

# ========= Gold logic =========
def load_all_runs() -> pd.DataFrame:
    paths = list_run_csvs()
    if not paths:
        return pd.DataFrame()
    dfs = []
    for p in paths:
        df = read_csv_blob(p)
        # normalize essential fields
        if "captured_at" in df.columns:
            df["captured_at"] = pd.to_datetime(df["captured_at"], errors="coerce", utc=True)
        if "snapshot_date" in df.columns:
            df["snapshot_date"] = pd.to_datetime(df["snapshot_date"], errors="coerce").dt.date
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)

def latest_per_sku(df: pd.DataFrame) -> pd.DataFrame:
    need = {"retailer_id", "native_item_id", "captured_at"}
    if not need.issubset(df.columns):
        missing = need - set(df.columns)
        raise RuntimeError(f"Missing columns for latest snapshot: {missing}")
    df = df.sort_values(["retailer_id","native_item_id","captured_at"])
    return df.groupby(["retailer_id","native_item_id"], as_index=False).tail(1)

def latest_per_sku_per_day(df: pd.DataFrame) -> pd.DataFrame:
    # reduce to one row per SKU per day (stable daily series)
    df = df.copy()
    df["snap_day"] = df["captured_at"].dt.date
    df = df.sort_values(["retailer_id","native_item_id","snap_day","captured_at"])
    out = df.groupby(["retailer_id","native_item_id","snap_day"], as_index=False).tail(1)
    return out.drop(columns=["snap_day"])

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.sort_values(["retailer_id","native_item_id","captured_at"])
    def _fe(g: pd.DataFrame) -> pd.DataFrame:
        g = g.copy()
        # make sure price_current exists
        if "price_current" not in g.columns:
            g["price_current"] = pd.NA
        g["price_prev"] = g["price_current"].shift(1)
        g["price_change"] = g["price_current"] - g["price_prev"]
        g["price_change_pct"] = (g["price_current"] / g["price_prev"] - 1).where(g["price_prev"] > 0)
        # 30-snapshot rolling stats (adjust later if you change frequency)
        g["roll_min_30"] = g["price_current"].rolling(30, min_periods=3).min()
        g["roll_max_30"] = g["price_current"].rolling(30, min_periods=3).max()
        g["roll_vol_30"] = g["price_current"].pct_change().rolling(30, min_periods=5).std()
        # stockout rate rolling
        if "in_stock_flag" in g.columns:
            s = g["in_stock_flag"].map({False:1, True:0})
            g["stockout_rate_30"] = s.rolling(30, min_periods=5).mean()
        else:
            g["stockout_rate_30"] = pd.NA
        # price vs msrp
        if "msrp" in g.columns:
            g["price_vs_msrp_pct"] = (g["price_current"] / g["msrp"]).where(g["msrp"] > 0)
        else:
            g["price_vs_msrp_pct"] = pd.NA
        return g
    return df.groupby(["retailer_id","native_item_id"], group_keys=False).apply(_fe)

def write_features_partitioned(features: pd.DataFrame):
    if features.empty: return
    features = features.copy()
    features["snapshot_date"] = features["captured_at"].dt.date
    for day, chunk in features.groupby("snapshot_date"):
        path = f"{GOLD_ROOT}features_snapshot_date={day}/part-000.parquet"
        write_parquet_blob(chunk.drop(columns=["snapshot_date"]), path)

def write_latest_snapshot(latest_df: pd.DataFrame):
    path = f"{GOLD_ROOT}latest/latest.parquet"
    write_parquet_blob(latest_df, path)

def write_daily_summary(features: pd.DataFrame):
    """Optional aggregated metrics per day & retailer to power fast dashboards."""
    if features.empty: return
    feats = features.copy()
    feats["snapshot_date"] = feats["captured_at"].dt.date
    group = feats.groupby(["snapshot_date","retailer_id"], dropna=False)
    summary = group.agg(
        avg_price=("price_current","mean"),
        promo_rate=("promo_flag", lambda s: float(pd.Series(s).fillna(False).mean())),
        stockout_rate=("in_stock_flag", lambda s: float((~pd.Series(s).fillna(True)).mean())),
        avg_discount_depth=("discount_depth","mean"),
        sku_count=("native_item_id","nunique"),
    ).reset_index()
    path = f"{GOLD_ROOT}metrics/summary_by_day.parquet"
    write_parquet_blob(summary, path)

def main():
    all_runs = load_all_runs()
    if all_runs.empty:
        print("[WARN] No silver run files found under silver/products_v1/")
        return

    # 1) Latest snapshot (one row per SKU)
    latest = latest_per_sku(all_runs)
    write_latest_snapshot(latest)
    print(f"[OK] latest snapshot → {len(latest):,} rows")

    # 2) Daily-dedup → features → partitioned gold
    daily = latest_per_sku_per_day(all_runs)
    feats = engineer_features(daily)
    write_features_partitioned(feats)
    print(f"[OK] features written for {feats['captured_at'].dt.date.nunique()} days")

    # 3) Optional summary table for dashboards
    write_daily_summary(feats)
    print("[OK] daily summary metrics written")

if __name__ == "__main__":
    main()
