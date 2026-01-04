#!/usr/bin/env python3
import requests, os
from datetime import datetime, timedelta

# version 1.0.4

RETENTION_HOURS = 48

NAMENODE = "http://192.168.0.131:9870"
HDFS_DIR = "/data/daily"
USER = "hadoop"

LOCAL_DIR = "/var/www/html/data/hourly"
os.makedirs(LOCAL_DIR, exist_ok=True)

# STEP 1: LIST files in HDFS
params = {
    "op": "LISTSTATUS",
    "user.name": USER
}

r = requests.get(
    f"{NAMENODE}/webhdfs/v1{HDFS_DIR}",
    params=params
)
r.raise_for_status()

files = r.json()["FileStatuses"]["FileStatus"]

# STEP 2: Download each file
for f in files:
    if f["type"] != "FILE":
        continue

    name = f["pathSuffix"]
    hdfs_path = f"{HDFS_DIR}/{name}"
    local_path = f"{LOCAL_DIR}/{name}"

    # Ask NameNode where to read
    r = requests.get(
        f"{NAMENODE}/webhdfs/v1{hdfs_path}",
        params={"op": "OPEN", "user.name": USER},
        allow_redirects=False
    )

    if "Location" not in r.headers:
        print("Skipping (no redirect):", name)
        continue

    redirect_url = r.headers["Location"]

    # Download from DataNode
    with requests.get(redirect_url, stream=True) as r2:
        r2.raise_for_status()
        with open(local_path, "wb") as out:
            for chunk in r2.iter_content(8192):
                if chunk:
                    out.write(chunk)

    print("Downloaded:", name)

# STEP 3: Local retention cleanup
now = datetime.now()

for f in os.listdir(LOCAL_DIR):
    if not f.endswith(".json"):
        continue

    try:
        # Extract timestamp part after hostname
        ts = f.rsplit("-", 4)[-4:]          # YYYY MM DD HH
        ts = "-".join(ts).replace(".json", "")

        dt = datetime.strptime(ts, "%Y-%m-%d-%H")
        if dt < now - timedelta(hours=RETENTION_HOURS):
            os.remove(f"{LOCAL_DIR}/{f}")
            print("Deleted old archive:", f)
    except:
        pass
