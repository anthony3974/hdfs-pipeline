#!/usr/bin/env python3
import requests, os, re, json, gzip
from datetime import datetime
from collections import defaultdict

# version 1.0.0

NAMENODE = "http://namenode:9870"
USER = "hadoop"

WEEKLY_DIR = "/data/weekly"
YEARLY_DIR = "/data/yearly"
TMP_DIR = "/tmp/weekly_to_yearly"

os.makedirs(TMP_DIR, exist_ok=True)

pattern = re.compile(r"(.*)-(\d{4}-\d{2}-\d{2})-(\d{2})\.json")

# Ensure yearly dir exists
requests.put(
    f"{NAMENODE}/webhdfs/v1{YEARLY_DIR}",
    params={"op": "MKDIRS", "user.name": USER}
)

# STEP 1: List weekly files
r = requests.get(
    f"{NAMENODE}/webhdfs/v1{WEEKLY_DIR}",
    params={"op": "LISTSTATUS", "user.name": USER}
)
r.raise_for_status()

files = r.json()["FileStatuses"]["FileStatus"]

groups = defaultdict(list)

# STEP 2: Group by (device, ISO week)
for f in files:
    if f["type"] != "FILE":
        continue

    name = f["pathSuffix"]
    m = pattern.match(name)
    if not m:
        continue

    device, date_str, hour = m.groups()
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    year, week, _ = dt.isocalendar()

    key = (device, year, week)
    groups[key].append(name)

# STEP 3: Process each group
for (device, year, week), filenames in groups.items():
    entries = []

    print(f"▶ Processing {device} {year}-W{week:02d} ({len(filenames)} files)")

    # Download + merge
    for name in filenames:
        hdfs_path = f"{WEEKLY_DIR}/{name}"

        r = requests.get(
            f"{NAMENODE}/webhdfs/v1{hdfs_path}",
            params={"op": "OPEN", "user.name": USER},
            allow_redirects=False
        )
        if "Location" not in r.headers:
            print("Skipping:", name)
            continue

        with requests.get(r.headers["Location"]) as r2:
            r2.raise_for_status()
            data = json.loads(r2.text)
            entries.extend(data)

    # Write + gzip locally
    out_name = f"{device}-{year}-W{week:02d}.json.gz"
    local_out = os.path.join(TMP_DIR, out_name)

    with gzip.open(local_out, "wt") as gz:
        json.dump(entries, gz)

    # Upload to HDFS
    with open(local_out, "rb") as f:
        requests.put(
            f"{NAMENODE}/webhdfs/v1{YEARLY_DIR}/{out_name}",
            params={"op": "CREATE", "overwrite": "true", "user.name": USER},
            data=f
        )

    print(f"✓ Uploaded {out_name}")

# Cleanup weekly files
for f in files:
    if f["type"] == "FILE":
        requests.delete(
            f"{NAMENODE}/webhdfs/v1{WEEKLY_DIR}/{f['pathSuffix']}",
            params={"op": "DELETE", "recursive": "false", "user.name": USER}
        )
