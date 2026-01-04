#!/usr/bin/env python3
import json, os, time, glob, shutil, socket
from datetime import datetime, timedelta, UTC

# version 1.3

RAW_DIR = "/var/data/data"
OUT_DIR = "/var/www/html/data/hourly"
os.makedirs(OUT_DIR, exist_ok=True)

# Local retention cleanup
RETENTION_HOURS = 48
LOCAL_DIR = OUT_DIR

hostname = socket.gethostname()

# Get all raw data log files
files = sorted(glob.glob(f"{RAW_DIR}/*.json"))

if not files:
    print("No data to aggregate.")
    exit(0)

# Load all entries
entries = []
for f in files:
    try:
        with open(f, "r") as fp:
            entries.extend(json.load(fp))
    except:
        continue

entries.sort(key=lambda e: e["timestamp"])

# Calculate per-sample network throughput (MB/s) from cumulative counters
rate_entries = []

prev = None
for e in entries:
    if prev is not None:
        dt = e["timestamp"] - prev["timestamp"]
        if dt > 0:
            rate_entries.append({
                "vm": e["vm"],
                "timestamp": e["timestamp"],
                "cpu_load": e["cpu_load"],
                "ram": e["ram"],
                "netout": (e["netout"] - prev["netout"]) / dt / (1024 * 1024),
                "netin":  (e["netin"]  - prev["netin"])  / dt / (1024 * 1024),
            })
    prev = e

# Determine output filename based on the past hour
now = datetime.now()
hour_bucket = now.replace(minute=0, second=0, microsecond=0)
out_name = f"{hostname}-{hour_bucket.strftime('%Y-%m-%d-%H')}.json"
out_path = os.path.join(OUT_DIR, out_name)

# Save aggregated data
with open(out_path, "w") as fp:
    json.dump(rate_entries, fp, indent=2)

print(f"Saved hourly aggregate → {out_path}")

# Delete raw files after aggregation
for f in files:
    os.remove(f)

print("Raw data logs cleared.")

# Adding data to hdfs
import requests
import json

# Base URL of the HDFS NameNode Web UI / WebHDFS endpoint
NAMENODE = "http://192.168.0.131:9870"
# Target path in HDFS where the file will be stored
HDFS_PATH = f"/data/daily/{out_name}"
# HDFS user to act as (must have write permission to the path)
USER = "hadoop"
# Parameters for the WebHDFS CREATE operation
# CREATE does NOT upload data yet — it only asks the NameNode
# where to send the data (it will respond with a redirect)
params = {
    "op": "CREATE",
    "overwrite": "true",
    "user.name": USER
}
# STEP 1: Send CREATE request to the NameNode
r = requests.put(
    f"{NAMENODE}/webhdfs/v1{HDFS_PATH}",
    params=params,
    allow_redirects=False
)
# If the NameNode does not return a redirect,
if "Location" not in r.headers:
    raise RuntimeError("No redirect from NameNode")
# Extract the DataNode upload URL from the redirect
redirect_url = r.headers["Location"]
# STEP 2: Upload the actual file contents to the DataNode
with open(out_path, "rb") as f:
    r2 = requests.put(redirect_url, data=f)

# WebHDFS returns HTTP 201 Created on success
if r2.status_code != 201:
    raise RuntimeError("Upload failed")

print("Uploaded to HDFS:", HDFS_PATH)

# Delete old aggregated files

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

