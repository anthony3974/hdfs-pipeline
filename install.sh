#!/usr/bin/env bash

# version 1.4

set -e

INSTALL_DIR="/usr/local/bin"
BASE_URL="https://tonycad.com/linux/python"

# Single place to name files
FILES=(
  "logger.py"
  "hourly-aggregate-to-hdfs.py"
)

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT
cd "$TMP_DIR"

echo "▶ Downloading scripts..."

for file in "${FILES[@]}"; do
  wget "$BASE_URL/$file"
done

echo "▶ Installing..."

for file in "${FILES[@]}"; do
  sudo install -m 755 "$file" "$INSTALL_DIR/$file"
done

echo "▶ Adding cron jobs..."

crontab -l 2>/dev/null | {
  cat
  echo "*/10 * * * * /usr/bin/python3 /usr/local/bin/logger.py"
  echo "0 * * * * /usr/bin/python3 /usr/local/bin/hourly-aggregate-to-hdfs.py"
} | crontab -

echo "✅ Done"
