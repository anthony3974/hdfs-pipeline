#!/usr/bin/env python3
import requests, re
from datetime import datetime, timedelta

# version 1.0.1

RETENTION_HOURS = 48

NAMENODE = "http://namenode:9870"
USER = "hadoop"

DAILY_DIR = "/data/daily"
WEEKLY_DIR = "/data/weekly"

pattern = re.compile(r"(.*)-(\d{4}-\d{2}-\d{2})-(\d{2})\.json")
cutoff = datetime.now() - timedelta(hours=RETENTION_HOURS)

# Ensure weekly dir exists
requests.put(
    f"{NAMENODE}/webhdfs/v1{WEEKLY_DIR}",
    params={"op": "MKDIRS", "user.name": USER}
)

# STEP 1: List files in /data/daily
r = requests.get(
    f"{NAMENODE}/webhdfs/v1{DAILY_DIR}",
    params={"op": "LISTSTATUS", "user.name": USER}
)
r.raise_for_status()

files = r.json()["FileStatuses"]["FileStatus"]

# STEP 2: Move old files
for f in files:
    if f["type"] != "FILE":
        continue

    name = f["pathSuffix"]
    m = pattern.match(name)
    if not m:
        continue

    device, date_str, hour = m.groups()
    file_time = datetime.strptime(
        f"{date_str} {hour}", "%Y-%m-%d %H"
    )

    if file_time >= cutoff:
        continue

    src = f"{DAILY_DIR}/{name}"
    dst = f"{WEEKLY_DIR}/{name}"

    # WebHDFS rename (move)
    r = requests.put(
        f"{NAMENODE}/webhdfs/v1{src}",
        params={
            "op": "RENAME",
            "destination": dst,
            "user.name": USER
        }
    )

    if r.status_code == 200:
        print(f"→ weekly: {name}")
    else:
        print(f"⚠ failed to move {name}: {r.text}")
