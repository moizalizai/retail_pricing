import base64, time, uuid
import os
from pathlib import Path
from typing import Optional
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

CONSUMER_ID   = os.getenv("CONSUMER_ID")
KEY_VERSION   = os.getenv("KEY_VERSION")         # e.g., "1"
# WALMART_SECRET: PEM string (with BEGIN/END) or base64 of the PEM
# WALMART_SECRET_PATH: optional path to a PEM file written by the workflow

def _load_walmart_private_key_bytes() -> bytes:
    """
    Load private key bytes from either:
      - WALMART_SECRET_PATH -> a PEM file path, or
      - WALMART_SECRET      -> PEM content (BEGIN/END) or base64-encoded PEM.
    """
    p: Optional[str] = os.getenv("WALMART_SECRET_PATH")
    if p:
        fp = Path(p)
        if fp.exists():
            return fp.read_bytes()

    val = os.getenv("WALMART_SECRET", "").strip()
    if not val:
        raise FileNotFoundError(
            "Private key not provided. Set WALMART_SECRET_PATH to a PEM file, "
            "or WALMART_SECRET to PEM/base64 content."
        )

    if val.startswith("-----BEGIN"):
        return val.encode("utf-8")

    # fallback: base64 string
    try:
        return base64.b64decode(val)
    except Exception as e:
        raise ValueError("WALMART_SECRET is neither PEM nor valid base64.") from e

def walmart_headers() -> dict:
    if not CONSUMER_ID:
        raise ValueError("Missing CONSUMER_ID")
    if not KEY_VERSION:
        raise ValueError("Missing KEY_VERSION")

    ts = str(int(time.time() * 1000))
    # Walmart signing string:
    to_sign = f"{CONSUMER_ID}\n{ts}\n{KEY_VERSION}\n"

    key_bytes = _load_walmart_private_key_bytes()
    key = serialization.load_pem_private_key(key_bytes, password=None)

    sig = key.sign(
        to_sign.encode("utf-8"),
        padding.PKCS1v15(),
        hashes.SHA256(),
    )
    sig_b64 = base64.b64encode(sig).decode("ascii")

    return {
        "WM_CONSUMER.ID": CONSUMER_ID,
        "WM_SEC.KEY_VERSION": KEY_VERSION,
        "WM_CONSUMER.INTIMESTAMP": ts,
        "WM_SEC.AUTH_SIGNATURE": sig_b64,
        "WM_QOS.CORRELATION_ID": str(uuid.uuid4()),
    }
