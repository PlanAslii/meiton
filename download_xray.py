#!/usr/bin/env python3
import json, os, sys, zipfile, urllib.request, stat

API = "https://api.github.com/repos/XTLS/Xray-core/releases/latest"
print("Fetching Xray-core release info...")
req = urllib.request.Request(API, headers={"User-Agent": "phoenix-bot"})
with urllib.request.urlopen(req, timeout=60) as r:
    data = json.loads(r.read().decode())

url = None
for asset in data.get("assets", []):
    name = asset.get("name", "")
    if name == "Xray-linux-64.zip" or (name.startswith("Xray-linux-64") and name.endswith(".zip")):
        url = asset.get("browser_download_url")
        break

if not url:
    print("Could not find Xray-linux-64.zip")
    sys.exit(1)

print("Downloading:", url)
zip_path = "/tmp/xray_dl.zip"
urllib.request.urlretrieve(url, zip_path)

print("Extracting...")
with zipfile.ZipFile(zip_path, "r") as z:
    member = next((n for n in z.namelist() if n == "xray" or n.endswith("/xray")), None)
    if not member:
        print("xray not in zip")
        sys.exit(1)
    with z.open(member) as src, open("xray", "wb") as dst:
        dst.write(src.read())

os.chmod("xray", 0o755)
os.remove(zip_path)
print("OK ->", os.path.abspath("xray"), f"({os.path.getsize('xray')/1024/1024:.1f} MB)")
