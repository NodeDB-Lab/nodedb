#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Derive the compiled-in EPSG tables of nodedb-crs from nodedb-crs/data/epsg.tsv:
#
# - epsg_core.tsv: WGS 84 (4326), Web Mercator (3857), and the WGS 84 UTM and
#   UPS grids (32601 to 32661, 32701 to 32761). Every build embeds it.
# - epsg.tsv.zst: the full table as one zstd frame, level 19. The `epsg`
#   feature embeds it.
#
# Run after scripts/gen_epsg.py rewrites epsg.tsv. Needs zstd and awk.
set -euo pipefail

data="$(cd "$(dirname "${BASH_SOURCE[0]}")/../nodedb-crs/data" && pwd)"

if ! command -v zstd >/dev/null 2>&1; then
    echo "pack_epsg.sh: zstd is not on PATH; install zstd, then rerun" >&2
    exit 1
fi

# Keep the comment and header lines before the first data row, then the core rows.
awk -F'\t' '
    !/^[0-9]/ { if (!data) print; next }
    { data = 1 }
    $1 == 4326 || $1 == 3857 || ($1 >= 32601 && $1 <= 32661) || ($1 >= 32701 && $1 <= 32761)
' "$data/epsg.tsv" > "$data/epsg_core.tsv"

rows="$(grep -c '^[0-9]' "$data/epsg_core.tsv")"
if [ "$rows" -ne 124 ]; then
    echo "pack_epsg.sh: epsg_core.tsv has $rows rows, expected 124; check epsg.tsv" >&2
    exit 1
fi

zstd -19 --quiet --force "$data/epsg.tsv" -o "$data/epsg.tsv.zst"

if ! zstd --quiet --decompress --stdout "$data/epsg.tsv.zst" | cmp --silent - "$data/epsg.tsv"; then
    echo "pack_epsg.sh: epsg.tsv.zst does not decode to epsg.tsv" >&2
    exit 1
fi

wc -c "$data/epsg.tsv" "$data/epsg.tsv.zst" "$data/epsg_core.tsv"
