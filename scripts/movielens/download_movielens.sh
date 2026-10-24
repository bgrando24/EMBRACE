#!/usr/bin/env bash
# Downloads MovieLens 32M (GroupLens, University of Minnesota) into data/movielens/ml-32m and verifies its checksum.
# Free for non-commercial use - see data/movielens/ml-32m/README.txt for the licence. Run from the project root.
# The ML code only reads links.csv and ratings.csv, and caches what it needs in data/cache/movielens_likers.parquet.
set -euo pipefail

DEST="data/movielens"
URL="https://files.grouplens.org/datasets/movielens/ml-32m.zip"

mkdir -p "$DEST"
curl -fsSL -o "$DEST/ml-32m.zip" "$URL"
expected=$(curl -fsSL "$URL.md5" | awk '{print $1}')
actual=$( (md5sum "$DEST/ml-32m.zip" 2>/dev/null || md5 -r "$DEST/ml-32m.zip") | awk '{print $1}')
if [ "$expected" != "$actual" ]; then
    echo "Checksum mismatch: expected $expected, got $actual" >&2
    exit 1
fi
unzip -q -o "$DEST/ml-32m.zip" ml-32m/ratings.csv ml-32m/links.csv ml-32m/movies.csv ml-32m/README.txt -d "$DEST"
rm "$DEST/ml-32m.zip"
rm -f data/cache/movielens_likers.parquet    # rebuilt from the new files on next use
echo "MovieLens 32M ready in $DEST/ml-32m"
