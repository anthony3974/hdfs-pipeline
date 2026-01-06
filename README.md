This system collects, aggregates, and visualizes performance metrics (CPU, RAM, and Network I/O) from multiple virtual machines across an HDFS cluster.

## System Architecture

1. **Metric Collection**: A Python logger runs on each VM to collect hardware statistics.


2. **Aggregation**: Scripts process raw data into hourly, weekly, and yearly buckets to manage storage efficiency.


3. **HDFS Storage**: Aggregated data is automatically uploaded to a Hadoop Distributed File System for long-term retention.


4. **Visualization**: A web-based dashboard provides real-time performance charts for all monitored VMs.



---

## Components

### Data Collection & Movement

* **`logger.py`**: Captures CPU, RAM, and Network data every minute and saves local JSON files.
* **`hourly-aggregate-to-hdfs.py`**: Merges local logs into hourly summaries, calculates network throughput (MB/s), and uploads them to HDFS `/data/daily`.


* **`daily-to-weekly-hdfs.py`**: Moves HDFS files older than 48 hours from `/daily` to `/weekly`.
* **`weekly-to-yearly-hdfs.py`**: Merges weekly files into gzipped yearly archives to save space.

### Monitoring & UI

* **`index.html`**: A Chart.js dashboard that visualizes VM metrics directly from the hourly data.


* **`sync-with-hdfs.py`**: Downloads aggregated files from HDFS to a local web server directory for visualization.

### Infrastructure

* **`hdfs-cluster-nodes`**: Inventory of the cluster, including NameNodes and DataNodes.


* **`install.sh`**: Automates script deployment and configures cron jobs (e.g., logger every 10 minutes, aggregator every hour).

---

## Installation

1. **Run Installer**: Execute `install.sh` on target VMs to download scripts and set up cron jobs.
2. **HDFS Setup**: Ensure the NameNode is accessible at `192.168.0.131` (as defined in the storage scripts).


3. **Dashboard**: Serve `index.html` via a web server (e.g., Nginx) and ensure `/data/hourly/` contains the JSON performance files.



---

## Metric Logic

* **CPU/RAM**: Reported as percentages (0-100%).


* **Network**: Derived from cumulative byte counters and converted into MB/s for throughput visualization.


* **Retention**: Local raw data is cleared after aggregation; hourly local archives are kept for 48 hours.

