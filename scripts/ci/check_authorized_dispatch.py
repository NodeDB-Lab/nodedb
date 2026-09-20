#!/usr/bin/env python3
"""Reject trusted-internal dispatch APIs from external transport modules."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "nodedb" / "src"

EXTERNAL_PREFIXES = (
    "control/array_sync/",
    "control/otel/",
    "control/server/http/",
    "control/server/native/",
    "control/server/pgwire/",
    "control/server/resp/",
    "control/server/session/",
    "control/server/sync/",
    "control/server/shared/ddl/neutral/collection/dml/",
)
EXTERNAL_FILES = {
    "control/server/ilp_batch.rs",
    "control/server/ilp_listener.rs",
    "control/server/ilp_auth.rs",
}

# These APIs intentionally accept raw plans because their authority comes from
# replay, consensus, maintenance, or an already-admitted parent operation.
# External transports must use the capability-bearing sibling API instead.
FORBIDDEN = (
    "dispatch_to_data_plane",
    "dispatch_to_data_plane_with_source",
    "dispatch_to_data_plane_with_txn",
    "dispatch_autocommit_write",
    "dispatch_write_to_data_plane",
    "dispatch_trusted_internal_write_to_data_plane",
    "dispatch_tasks_to_calvin",
    "dispatch_strict_atomic_tasks_to_calvin",
    "dispatch_crdt_apply_admitted",
    "dispatch_crdt_apply_admitted_outcome",
    "dispatch_sync_response",
    "dispatch_trusted_internal_sync_response",
    "execute_internal",
    "execute_internal_with_watermarks",
    "execute_stream_internal",
    "run_insert_select",
    "run_merge",
    "run_update_from_join",
    "into_physical_task",
    "gather_all_cores_stream",
    "dispatch_dependent_edge_recon",
    "dispatch_single_task_raw",
    "into_scope",
    "dispatch_crdt_restore_admitted",
    "dispatch_calvin_or_fast",
    "propose_sync_write",
    "propose_replicated_entry",
    "dispatch_route",
    "dispatch_route_stream",
    "gather_all_cores",
    "gather_all_vshards",
    "execute_plan_all_local_cores",
)

# One mixed sync helper module owns a trusted-internal function declaration.
# Only its declaration is exempt; imports and calls remain forbidden.
ALLOWED_DEFINITIONS = {
    (
        "control/server/sync/raft_dispatch/response.rs",
        "dispatch_trusted_internal_sync_response",
    ),
    ("control/server/sync/raft_dispatch/propose.rs", "propose_sync_write"),
}

# Narrow implementation seams that sit in mixed transport/helper modules. The
# referenced functions are private or are reached only after a consumed
# capability; no other occurrence in these files is exempt.
ALLOWED_REFERENCES = {
    (
        "control/server/sync/raft_dispatch/response.rs",
        "dispatch_trusted_internal_write_to_data_plane",
    ),
    (
        "control/server/native/dispatch/transaction.rs",
        "dispatch_trusted_internal_write_to_data_plane",
    ),
    ("control/server/sync/raft_dispatch/response.rs", "into_physical_task"),
    ("control/server/sync/raft_dispatch/write.rs", "into_physical_task"),
    (
        "control/server/sync/raft_dispatch/mod.rs",
        "dispatch_trusted_internal_sync_response",
    ),
    ("control/server/pgwire/handler/dispatch.rs", "into_physical_task"),
    ("control/server/pgwire/handler/submit.rs", "into_physical_task"),
    ("control/server/shared/cluster_array_dispatch.rs", "into_physical_task"),
    ("control/array_sync/inbound.rs", "into_scope"),
    ("control/array_sync/inbound_propose.rs", "into_scope"),
    ("control/array_sync/snapshot_assembly.rs", "into_scope"),
    ("control/server/native/dispatch/sql_dispatch_task.rs", "into_physical_task"),
    ("control/server/sync/raft_dispatch/response.rs", "propose_sync_write"),
    ("control/server/sync/raft_dispatch/write.rs", "propose_sync_write"),
    (
        "control/server/pgwire/handler/dispatch.rs",
        "propose_replicated_entry",
    ),
}


def mask_rust(source: str) -> str:
    """Mask comments and string/character literals while preserving newlines."""
    out = list(source)
    i = 0
    n = len(source)
    block_depth = 0
    while i < n:
        if block_depth:
            if source.startswith("/*", i):
                out[i : i + 2] = "  "
                block_depth += 1
                i += 2
            elif source.startswith("*/", i):
                out[i : i + 2] = "  "
                block_depth -= 1
                i += 2
            else:
                if source[i] != "\n":
                    out[i] = " "
                i += 1
            continue
        if source.startswith("//", i):
            end = source.find("\n", i)
            end = n if end < 0 else end
            out[i:end] = " " * (end - i)
            i = end
            continue
        if source.startswith("/*", i):
            out[i : i + 2] = "  "
            block_depth = 1
            i += 2
            continue
        # Rust raw strings: r"...", r#"..."#, br##"..."##.
        raw = re.match(r"(?:b)?r(#{0,255})\"", source[i:])
        if raw:
            hashes = raw.group(1)
            start_len = raw.end()
            end_marker = '"' + hashes
            end = source.find(end_marker, i + start_len)
            end = n if end < 0 else end + len(end_marker)
            for j in range(i, end):
                if source[j] != "\n":
                    out[j] = " "
            i = end
            continue
        prefix_len = 2 if source.startswith('b"', i) else 1
        if source[i] == '"' or source.startswith('b"', i):
            j = i + prefix_len
            escaped = False
            while j < n:
                ch = source[j]
                if ch == '"' and not escaped:
                    j += 1
                    break
                escaped = ch == "\\" and not escaped
                if ch != "\\":
                    escaped = False
                j += 1
            for k in range(i, j):
                if source[k] != "\n":
                    out[k] = " "
            i = j
            continue
        # Character/byte literals, but not lifetimes.
        char_match = re.match(r"(?:b)?'(?:\\.|[^'\\\n])'", source[i:])
        if char_match:
            end = i + char_match.end()
            out[i:end] = " " * (end - i)
            i = end
            continue
        i += 1
    return "".join(out)


def is_external(rel: str) -> bool:
    return rel in EXTERNAL_FILES or rel.startswith(EXTERNAL_PREFIXES)


def violations(rel: str, source: str) -> list[tuple[int, str]]:
    masked = mask_rust(source)
    found: list[tuple[int, str]] = []
    if rel.startswith("control/server/session/"):
        for match in re.finditer(r"(?<![A-Za-z0-9_])Request\s*\{", masked):
            found.append(
                (masked.count("\n", 0, match.start()) + 1, "direct Request construction")
            )

    for name in FORBIDDEN:
        pattern = re.compile(rf"\b{re.escape(name)}\b")
        for match in pattern.finditer(masked):
            line_start = masked.rfind("\n", 0, match.start()) + 1
            line_end = masked.find("\n", match.end())
            line_end = len(masked) if line_end < 0 else line_end
            line = masked[line_start:line_end]
            definition = re.search(rf"\bfn\s+{re.escape(name)}\b", line)
            if definition and (rel, name) in ALLOWED_DEFINITIONS:
                continue
            if (rel, name) in ALLOWED_REFERENCES:
                continue
            found.append((masked.count("\n", 0, match.start()) + 1, name))
    return found


def self_test() -> None:
    rel = "control/server/http/example.rs"
    assert not violations(rel, '// dispatch_to_data_plane(x)\nlet s = "run_merge(x)";')
    assert not violations(rel, "dispatch_authorized_to_data_plane(state, task, trace).await;")
    assert violations(rel, "dispatch_to_data_plane(state, tenant, db, shard, plan, trace).await;")
    assert violations(rel, "use crate::x::dispatch_to_data_plane as send;")
    assert violations(rel, "gateway.execute_internal(&ctx, plan).await;")
    assert violations(rel, "gather_all_cores_stream(state, tenant, db, plan, trace, None);")
    assert is_external("control/otel/receiver.rs")
    dml_rel = "control/server/shared/ddl/neutral/collection/dml/parse/dispatch.rs"
    assert is_external(dml_rel)
    assert violations(dml_rel, "dispatch_autocommit_write(state, write).await;")
    session_rel = "control/server/session/example.rs"
    assert violations(session_rel, "let request = Request { request_id }; ")
    assert public_raw_api_violations(
        "control/example.rs",
        "pub async fn dispatch_to_data_plane() {}",
    )
    assert not public_raw_api_violations(
        "control/example.rs",
        "pub(crate) async fn dispatch_to_data_plane() {}",
    )
    allowed = "control/server/sync/raft_dispatch/response.rs"
    assert not violations(
        allowed,
        "pub async fn dispatch_trusted_internal_sync_response() {}",
    )
    assert violations(allowed, "dispatch_sync_response(state, plan).await;")
    print("OK: authorized-dispatch gate self-tests passed.")


PUBLIC_RAW_EXACT = {
    "control/cluster/array_cluster_exec/executor.rs": ("execute",),
}


def public_raw_api_violations(rel: str, source: str) -> list[tuple[int, str]]:
    masked = mask_rust(source)
    found: list[tuple[int, str]] = []
    names = FORBIDDEN + PUBLIC_RAW_EXACT.get(rel, ())
    for name in names:
        pattern = re.compile(
            rf"\bpub\s+(?:async\s+)?fn\s+{re.escape(name)}\b"
        )
        for match in pattern.finditer(masked):
            found.append((masked.count("\n", 0, match.start()) + 1, name))
    return found


def stale_exemptions() -> list[str]:
    """Report exemptions that no longer name a live occurrence.

    An exemption is keyed on (path, symbol). Move the file or rename the
    symbol and the entry stops matching anything — it does not fail, it
    silently stops being an exemption, and the next occurrence added to
    that path is waved through instead of being caught. Both halves of
    this gate have already been disarmed that way by an ordinary file
    move, so a dead entry is treated as a gate failure, not as tidiness.

    The symbol is looked for in masked source, so an entry kept alive only
    by a comment or a string literal still reports as dead.
    """
    stale: list[str] = []
    for label, exemptions in (
        ("ALLOWED_DEFINITIONS", ALLOWED_DEFINITIONS),
        ("ALLOWED_REFERENCES", ALLOWED_REFERENCES),
    ):
        for rel, name in sorted(exemptions):
            path = SRC / rel
            if not path.is_file():
                stale.append(f"{label}: ({rel!r}, {name!r}) names a file that does not exist")
                continue
            masked = mask_rust(path.read_text(encoding="utf-8"))
            if not re.search(rf"\b{re.escape(name)}\b", masked):
                stale.append(f"{label}: ({rel!r}, {name!r}) matches no occurrence in that file")
    return stale


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return 0

    stale = stale_exemptions()
    if stale:
        print("ERROR: stale exemptions — the gate is disarmed where they point:", file=sys.stderr)
        for entry in stale:
            print(f"  {entry}", file=sys.stderr)
        print(
            "Repoint each entry at the code's current location, or drop it if the seam is gone.",
            file=sys.stderr,
        )
        return 1

    errors: list[str] = []
    for path in SRC.rglob("*.rs"):
        rel = path.relative_to(SRC).as_posix()
        source = path.read_text(encoding="utf-8")
        if is_external(rel):
            for line, name in violations(rel, source):
                errors.append(f"{path.relative_to(ROOT)}:{line}: external transport references trusted-internal API `{name}`")
        for line, name in public_raw_api_violations(rel, source):
            errors.append(f"{path.relative_to(ROOT)}:{line}: trusted-internal API `{name}` must not be public")
    if errors:
        print("ERROR: external transports must consume authorization capabilities:", file=sys.stderr)
        for error in errors:
            print(f"  {error}", file=sys.stderr)
        return 1
    print("OK: external transport dispatches are capability-gated.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
