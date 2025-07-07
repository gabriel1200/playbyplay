#!/bin/bash

set -euo pipefail

# Create the destination directory structure
mkdir -p ../../web_app/data/pbp/regular_season
mkdir -p ../../web_app/data/pbp/playoffs

echo "==> Copying parquet files from regular_season (only files with '2026' in the name)..."
find ~/basketball/playbyplay/organized_data/regular_season -type f -name "*2026*.parquet" | while read -r file; do
    filename=$(basename "$file")
    echo "Copying $filename"
    cp "$file" ../../web_app/data/pbp/regular_season/
done

echo "==> Copying parquet files from playoffs (only files with '2026' in the name)..."
find ~/basketball/playbyplay/organized_data/playoffs -type f -name "*2026*.parquet" | while read -r file; do
    filename=$(basename "$file")
    echo "Copying $filename"
    cp "$file" ../../web_app/data/pbp/playoffs/
done

echo "==> All 2026 parquet files copied successfully!"

# Verify the copy worked
echo ""
echo "==> Verifying files copied:"
echo -n "Files in regular_season: "
find ../../web_app/data/pbp/regular_season -type f -name "*2026*.parquet" | wc -l
echo -n "Files in playoffs: "
find ../../web_app/data/pbp/playoffs -type f -name "*2026*.parquet" | wc -l

echo ""
echo "If everything looks good, you can change 'cp' to 'mv' in the script to move the files instead of copying."
