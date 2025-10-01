import base64, time, uuid
import os
from pathlib import Path
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

CONSUMER_ID = os.getenv("CONSUMER_ID")
WALMART_SECRET = os.getenv("PRIVATE_KEY_PEM")
KEY_VERSION = os.getenv("KEY_VERSION")

def walmart_headers():
    ts = str(int(time.time()*1000))
    to_sign = f"{CONSUMER_ID}\n{ts}\n{KEY_VERSION}\n"
    key = serialization.load_pem_private_key(Path(WALMART_SECRET).read_bytes(), password=None)
    sig = key.sign(
        to_sign.encode("utf-8"),
        padding.PKCS1v15(),
        hashes.SHA256()
    )
    sig_b64 = base64.b64encode(sig).decode("ascii")
    return {
        "WM_CONSUMER.ID": CONSUMER_ID,
        "WM_SEC.KEY_VERSION": KEY_VERSION,
        "WM_CONSUMER.INTIMESTAMP": ts,
        "WM_SEC.AUTH_SIGNATURE": sig_b64,
        "WM_QOS.CORRELATION_ID": str(uuid.uuid4())
    }

