#!/usr/bin/env bash
# Warm-tier storage gate.
#
# The warm-tier code paths (snapshot writer, snapshot executor) use
# `Arc<dyn object_store::ObjectStore>`. A direct `std::fs::` or
# `tokio::fs::` call in those modules bypasses the object store.
#
# Every match in these files must carry a `// no-objectstore:
# <reason>` marker on the preceding line. The marker documents an
# intentional local-only operation, such as a redb engine-state landing
# path or the LocalFileSystem ObjectStore root bootstrap.
#
# Excluded: bodies of inline `#[cfg(test)] mod tests` blocks.

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
files=(
    nodedb/src/storage/snapshot_executor.rs
    nodedb/src/storage/snapshot_writer.rs
)

violations=()
for rel in "${files[@]}"; do
    file="$ROOT/$rel"
    [ -f "$file" ] || { echo "WARN: $rel not found"; continue; }
    while IFS= read -r match; do
        lineno="${match%%:*}"
        last_cfg=$(awk -v n="$lineno" 'NR<=n && /^#\[cfg\(test\)\]/ {x=NR} END{print x+0}' "$file")
        if [ "$last_cfg" -gt 0 ]; then
            close_after=$(awk -v a="$last_cfg" -v b="$lineno" 'NR>a && NR<b && /^}/ {x=NR} END{print x+0}' "$file")
            [ "$close_after" -eq 0 ] && continue
        fi
        # marker check: 3-line window above
        start=$((lineno > 3 ? lineno - 3 : 1))
        window=$(sed -n "${start},${lineno}p" "$file")
        echo "$window" | grep -q 'no-objectstore:' && continue
        violations+=("$rel:$lineno")
    done < <(grep -En '(std|tokio)::fs::' "$file" 2>/dev/null \
             | grep -v ':[[:space:]]*//' \
             | grep -v ':[[:space:]]*\*' \
             || true)
done

if [ ${#violations[@]} -gt 0 ]; then
    echo "FAIL: ${#violations[@]} unmarked filesystem call(s) in warm-tier files:"
    printf '  %s\n' "${violations[@]}"
    echo
    echo "Either route the operation through the ObjectStore handle, or add a"
    echo "'// no-objectstore: <reason>' marker on the preceding line."
    exit 1
fi
echo "OK: warm-storage gate clean across ${#files[@]} files."
