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
    // `retention_policy/autowire.rs` is absent: a policy's downsample tiers
    // register on every core of the node, not through the system door.
    "engine/timeseries/retention_policy/enforcement.rs",
    // `backup/restore/durable.rs` is absent: every restore re-issue proposes
    // a replicated entry, and each replica applies it. Backup capture, the
    // cluster snapshot builder, PURGE TENANT and the MOVE TENANT snapshot
    // fan out to every core through the all-cores exchange, not a SystemTask.
    // `cluster/snapshot_applier.rs` is absent: a cluster snapshot install
    // builds its restore plans itself and sends them to each core with the
    // dispatcher, not through the system door.
    // Committed DDL applied to engine state, and catalog maintenance.
    "control/server/shared/ddl/engine_apply.rs",
    // A committed CONVERT entry on each replica. The statement's identity is
    // checked on the proposing node, and the log entry carries no user.
    "control/distributed_applier/apply_loop/collection_convert.rs",
    // `continuous_agg/create.rs`, `drop.rs` and `register.rs` are absent: a
    // continuous aggregate replicates as a catalog entry, and each node's
    // post-apply lane registers or removes it on every core. `show.rs` is
    // absent too: SHOW CONTINUOUS AGGREGATES fans out to every core through
    // the all-cores exchange.
    // `synonym_group/create.rs` and `synonym_group/drop.rs` are absent:
    // a synonym group replicates as a catalog entry, and each node's
    // post-apply lane installs it in that node's FTS backend.
    // `at_version.rs` and `diff.rs` are deliberately absent: they serve user
    // reads, so they dispatch through the authorized path rather than the
    // system door, which performs no authorization or RLS injection.
    // `compact.rs` is absent too: COMPACT HISTORY replicates as a catalog
    // entry, and each node's post-apply lane dispatches the compaction.
    "control/server/shared/ddl/neutral/version_history/checkpoint.rs",
    // `tenant/move_tenant/cutover.rs` is absent: MOVE TENANT re-issues its
    // rows as durable writes and moves the namespace through one metadata
    // commit, whose post-apply lane reclaims the source storage.
    // Event Plane rules dispatched back through the Control Plane.
    "event/alert/executor.rs",
    // Legs of a request whose capability was consumed at the entry point.
    "control/crdt_admission/legs.rs",
    // `sync/raft_dispatch/write.rs` is absent: a sync write proposes its
    // plan through Raft, and the entry's apply dispatches it on each replica.
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

/// The file that declares the module `file` holds, and that module's name.
///
/// `a/b.rs` and `a/b/mod.rs` are both module `b`, declared in `a/mod.rs`,
/// `a.rs`, or the crate root (`lib.rs`, `main.rs`) when `a` is `src`.
fn declaring_file(root: &Path, file: &Path) -> Option<(PathBuf, String)> {
    let stem = file.file_stem()?.to_str()?;
    let (name, dir) = if stem == "mod" {
        let dir = file.parent()?;
        (dir.file_name()?.to_str()?.to_owned(), dir.parent()?)
    } else {
        (stem.to_owned(), file.parent()?)
    };
    if dir == root {
        return ["lib.rs", "main.rs"]
            .iter()
            .map(|f| root.join(f))
            .find(|p| std::fs::read_to_string(p).is_ok_and(|b| declared_attrs(&b, &name).is_some()))
            .map(|p| (p, name));
    }
    [dir.join("mod.rs"), dir.with_extension("rs")]
        .into_iter()
        .find(|p| p.is_file())
        .map(|p| (p, name))
}

/// The attribute lines directly above `mod <name>;` in `body`, or `None`
/// when `body` does not declare that module.
fn declared_attrs(body: &str, name: &str) -> Option<Vec<String>> {
    let lines: Vec<&str> = body.lines().collect();
    let target = format!("mod {name};");
    let index = lines.iter().position(|line| {
        let line = line.trim();
        line == target || (line.starts_with("pub") && line.ends_with(&format!(" {target}")))
    })?;
    let attrs = lines[..index]
        .iter()
        .rev()
        .map(|line| line.trim())
        .take_while(|line| line.starts_with("#[") || line.starts_with("//"))
        .filter(|line| line.starts_with("#["))
        .map(str::to_owned)
        .collect();
    Some(attrs)
}

/// Whether `file` compiles only under `cfg(test)`: the file opens with
/// `#![cfg(test)]`, its `mod` declaration carries `#[cfg(test)]`, or its
/// parent module does. Such a file never reaches a production build, so a
/// `SystemTask` it builds is a test fixture, not a dispatch door.
fn is_test_only(root: &Path, file: &Path) -> bool {
    let Ok(body) = std::fs::read_to_string(file) else {
        return false;
    };
    if body.lines().any(|line| line.trim() == "#![cfg(test)]") {
        return true;
    }
    let Some((parent, name)) = declaring_file(root, file) else {
        return false;
    };
    let Ok(parent_body) = std::fs::read_to_string(&parent) else {
        return false;
    };
    match declared_attrs(&parent_body, &name) {
        Some(attrs) if attrs.iter().any(|a| a == "#[cfg(test)]") => true,
        Some(_) => {
            parent.as_path() != file && !is_crate_root(root, &parent) && is_test_only(root, &parent)
        }
        None => false,
    }
}

fn is_crate_root(root: &Path, file: &Path) -> bool {
    file == root.join("lib.rs") || file == root.join("main.rs")
}

/// Every `SystemTask::new` in a production build is in a file that named
/// itself as system-initiated work. Test-only files are skipped: they never
/// compile into the server, and a fixture that drives the system door must
/// not need a production allowlist entry.
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
        if !body.contains("SystemTask::new") || is_test_only(&root, file) {
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

/// The test-only skip can never hide a production caller: no allowlisted
/// file reads as test-only, and a file declared under `#[cfg(test)]` does.
#[test]
fn test_only_detection_matches_the_module_tree() {
    let root = src_root();
    for entry in ALLOWED {
        assert!(
            !is_test_only(&root, &root.join(entry)),
            "{entry} is production code but reads as test-only"
        );
    }
    let fixture = root.join("control/gateway/error_map/test_fixtures.rs");
    assert!(
        is_test_only(&root, &fixture),
        "a `#[cfg(test)] mod` file must read as test-only"
    );
}
