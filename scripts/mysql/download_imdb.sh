#!/usr/bin/env bash
# Downloads the IMDB non-commercial dataset files the loader uses into IMDB_DATA_DIR, decompresses them
# (LOAD DATA INFILE can't read .gz) and writes MANIFEST.tsv with each file's download time, size and sha256,
# so an import can be traced back to the snapshot it came from. IMDB refreshes these files daily.
#
# usage: IMDB_DATA_DIR=/path/to/dir scripts/mysql/download_imdb.sh     (or set IMDB_DATA_DIR in scripts/mysql/.env)
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
if [[ -z "${IMDB_DATA_DIR:-}" && -f "${SCRIPT_DIR}/.env" ]]; then
	IMDB_DATA_DIR="$(grep -E '^IMDB_DATA_DIR=' "${SCRIPT_DIR}/.env" | cut -d= -f2- | sed -e 's/[[:space:]]*#.*$//' -e 's/^"//' -e 's/"$//')"
fi
: "${IMDB_DATA_DIR:?set IMDB_DATA_DIR}"
mkdir -p "${IMDB_DATA_DIR}"
cd "${IMDB_DATA_DIR}"

FILES=(title.basics title.crew title.episode title.principals title.ratings name.basics)
MANIFEST="MANIFEST.tsv"
printf "file\tdownloaded_utc\tbytes\tsha256\n" > "${MANIFEST}.tmp"

for f in "${FILES[@]}"; do
	echo "Downloading ${f}..."
	curl -sSfL --retry 3 -o "${f}.tsv.gz.part" "https://datasets.imdbws.com/${f}.tsv.gz"
	gunzip -c "${f}.tsv.gz.part" > "${f}.tsv.part"
	mv "${f}.tsv.part" "${f}.tsv"
	rm "${f}.tsv.gz.part"
	printf "%s\t%s\t%s\t%s\n" "${f}.tsv" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
		"$(wc -c < "${f}.tsv" | tr -d ' ')" "$(sha256sum "${f}.tsv" 2>/dev/null | cut -d' ' -f1 || shasum -a 256 "${f}.tsv" | cut -d' ' -f1)" \
		>> "${MANIFEST}.tmp"
done

mv "${MANIFEST}.tmp" "${MANIFEST}"
echo "Done:"; cat "${MANIFEST}"
