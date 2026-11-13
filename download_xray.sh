#!/bin/bash
# Download Xray-core Linux amd64 using Python (no curl/wget required)
set -e
cd "$(dirname "$0")"

python3 << 'PY'
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
    print("Could not find Xray-linux-64.zip in latest release")
    sys.exit(1)

print(f"Downloading: {url}")
zip_path = "/tmp/xray_dl.zip"
urllib.request.urlretrieve(url, zip_path)

print("Extracting xray binary...")
with zipfile.ZipFile(zip_path, "r") as z:
    member = None
    for n in z.namelist():
        if n == "xray" or n.endswith("/xray"):
            member = n
            break
    if not member:
        print("xray binary not found inside zip:", z.namelist()[:10])
        sys.exit(1)
    with z.open(member) as src, open("xray", "wb") as dst:
        dst.write(src.read())

os.chmod("xray", os.stat("xray").st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
os.remove(zip_path)

print("Done. Binary at:", os.path.abspath("xray"))
print("Size: {:.1f} MB".format(os.path.getsize("xray") / 1024 / 1024))
PY

./xray version 2>/dev/null || true
echo "If permission denied, run: chmod +x xray"
