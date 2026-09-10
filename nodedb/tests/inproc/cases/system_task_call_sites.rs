// SPDX-License-Identifier: BUSL-1.1

//! The system dispatch door stays closed to client-reachable code.
//!
//! `SystemTask` exists so a caller that reaches the Data Plane without a user
//! identity has to say why. That only holds while the set of callers is work
//! the server genuinely originates itself — retention, backup, cluster
//! snapshot, DDL apply, catalog maintenance, tenant lifecycle, Event Plane
//! rules, and legs of an already-admitted request.
//!
//! Rust's module privacy cannot express "only these unrelated modules may
//! construct this": `pub(in path)` requires an ancestor module, and every
//! caller here lives outside `sync_dispatch`'s ancestry. So the boundary is
//! enforced here instead — a new construction site has to be added to the
//! allowlist deliberately, in a file whose whole purpose is to make someone
//! think about whether a client can reach it.
//!
//! If this test fails, do not simply extend the list. Ask first whether the new
//! caller has an identity available. If it does, it belongs on the authorized
//! path (`user_dispatch::dispatch_for_identity`), not here.

use std::path::{Path, PathBuf};

/// Files permitted to construct a `SystemTask`, relative to `nodedb/src`.
///
/// Every entry is work with no user behind it. Transports (`resp`, `pgwire`,
/// `native`, `http`, `sync`, `ilp`) are deliberately absent.
const ALLOWED: &[&str] = &[
    // Retention and temporal enforcement, on their own timers.
    "engine/timeseries/retention_policy/autowire.rs",
    "engine/timeseries/retention_policy/enforcement.rs",
    "engine/bitemporal/enforcement.rs",
    // Backup capture and restore reissue.
    "control/backup/orchestrator.rs",
    "control/backup/restore/orchestrate/restore.rs",
    "control/backup/restore/columnar_reissue.rs",
    "control/backup/restore/timeseries_reissue.rs",
    "control/backup/restore/vector_reissue.rs",
    // Cluster snapshot transfer.
    "control/cluster/snapshot_builder.rs",
    "control/cluster/snapshot_applier.rs",
    // Committed DDL applied to engine state, and catalog maintenance.
    "control/server/shared/ddl/engine_apply.rs",
    "control/server/shared/ddl/neutral/convert/driver.rs",
    "control/server/shared/ddl/neutral/continuous_agg/create.rs",
    "control/server/shared/ddl/neutral/continuous_agg/drop.rs",
    "control/server/shared/ddl/neutral/continuous_agg/register.rs",
    "control/server/shared/ddl/neutral/continuous_agg/show.rs",
    // `synonym_group/create.rs` and `synonym_group/drop.rs` are absent:
    // a synonym group replicates as a catalog entry, and each node's
    // post-apply lane installs it in that node's FTS backend.
    // `at_version.rs` and `diff.rs` are deliberately absent: they serve user
    // reads, so they dispatch through the authorized path rather than the
    // system door, which performs no authorization or RLS injection.
    // `compact.rs` is absent too: COMPACT HISTORY replicates as a catalog
    // entry, and each node's post-apply lane dispatches the compaction.
    "control/server/shared/ddl/neutral/version_history/checkpoint.rs",
    // Tenant lifecycle.
    "control/server/shared/ddl/neutral/tenant/purge.rs",
    "control/server/shared/ddl/neutral/tenant/move_tenant/cutover.rs",
    "control/server/shared/ddl/neutral/tenant/move_tenant/snapshot.rs",
    // Event Plane rules dispatched back through the Control Plane.
    "event/alert/executor.rs",
    // Legs of a request whose capability was consumed at the entry point.
    "control/crdt_admission.rs",
    "control/server/sync/raft_dispatch/write.rs",
];

fn src_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("src")
}

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read {}: {e}", dir.display()));
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// Every `SystemTask::new` in the tree is in a file that named itself as
/// system-initiated work.
#[test]
fn system_task_is_constructed_only_by_system_initiated_code() {
    let root = src_root();
    let mut files = Vec::new();
    rust_files(&root, &mut files);
    assert!(
        !files.is_empty(),
        "found no sources under {}",
        root.display()
    );

    let mut unexpected = Vec::new();
    for file in &files {
        let body = std::fs::read_to_string(file)
            .unwrap_or_else(|e| panic!("read {}: {e}", file.display()));
        if !body.contains("SystemTask::new") {
            continue;
        }
        let relative = file
            .strip_prefix(&root)
            .unwrap_or(file)
            .to_string_lossy()
            .replace('\\', "/");
        if !ALLOWED.contains(&relative.as_str()) {
            unexpected.push(relative);
        }
    }

    assert!(
        unexpected.is_empty(),
        "these files construct a SystemTask but are not listed as system-initiated work: {unexpected:#?}\n\
         A SystemTask asserts that no user identity exists for the dispatch. If the caller \
         has an identity, route it through user_dispatch::dispatch_for_identity instead."
    );
}

/// The allowlist does not outlive its entries: a stale path hides the fact that
/// a caller moved, and a moved caller is exactly what needs re-examining.
#[test]
fn every_allowlisted_file_still_constructs_a_system_task() {
    let root = src_root();
    let mut stale = Vec::new();
    for entry in ALLOWED {
        let path = root.join(entry);
        match std::fs::read_to_string(&path) {
            Ok(body) if body.contains("SystemTask::new") => {}
            Ok(_) => stale.push(format!("{entry} (no longer constructs one)")),
            Err(_) => stale.push(format!("{entry} (file is gone)")),
        }
    }

    assert!(
        stale.is_empty(),
        "the SystemTask allowlist has stale entries; remove them: {stale:#?}"
    );
}
