import requests, json
from signer import walmart_headers

BASE = "https://developer.api.walmart.com/api-proxy/service/affil/product/v2"

resp = requests.get(f"{BASE}/taxonomy", headers=walmart_headers(), timeout=30)
print(resp.status_code)
data = resp.json()
print(list(data.keys())) 

with open("taxonomy.json", "w") as f:
    json.dump(data, f, indent=2)
print("Saved taxonomy.json")