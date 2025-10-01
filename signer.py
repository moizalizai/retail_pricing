import base64, time, uuid
from pathlib import Path
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

CONSUMER_ID = "ef7c1fec-1f3c-424e-843e-c25f0ee4289f"
KEY_VERSION = "1"
PRIVATE_KEY_PEM = "/Users/muhammadmoiz/Documents/UoP/MSBA 286/retail_pricing/WM_IO_private_key.pem"

def walmart_headers():
    ts = str(int(time.time()*1000))
    to_sign = f"{CONSUMER_ID}\n{ts}\n{KEY_VERSION}\n"
    key = serialization.load_pem_private_key(Path(PRIVATE_KEY_PEM).read_bytes(), password=None)
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

