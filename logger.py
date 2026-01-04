#!/usr/bin/env python3
import json, time, os, psutil, socket
from datetime import datetime

# version 1.0

OUT_DIR = "/var/data/data"
os.makedirs(OUT_DIR, exist_ok=True)

hostname = socket.gethostname()

data = []  # store all readings here

for _ in range(10):
    timestamp = int(time.time())
    cpu_load = psutil.cpu_percent(interval=1)
    ram = psutil.virtual_memory()
    net = psutil.net_io_counters()
    netout = net.bytes_sent
    netin  = net.bytes_recv

    data.append({
        "vm": hostname,
        "timestamp": timestamp,
        "cpu_load": cpu_load,
	"ram": ram.percent,
        "netout": netout,
        "netin": netin
    })

    time.sleep(60)

# final file name uses the start timestamp
#start_ts = data[0]["timestamp"]
out_path = f"{OUT_DIR}/{hostname}_{timestamp}.json"

with open(out_path, "w") as f:
    json.dump(data, f, indent=2)

print("Saved:", out_path)



