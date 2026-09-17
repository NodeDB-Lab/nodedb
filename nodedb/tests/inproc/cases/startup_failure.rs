// SPDX-License-Identifier: BUSL-1.1

//! Integration test: nodedb binary exits non-zero when startup fails.
//!
//! The test spawns the real `nodedb` binary (built in the test profile) with
//! a corrupted WAL segment in the data directory. The binary must detect the
//! corruption and exit with a non-zero status within 5 seconds.
//!
//! WAL segment naming: `wal-{lsn:020}.seg` under `<data_dir>/wal/`.

use std::fs;
use std::time::Duration;

use nodedb::control::security::catalog::rls::StoredRlsPolicy;
use nodedb::control::security::credential::store::CredentialStore;

/// Spawn the `nodedb` binary against `data_dir`, wait up to `timeout` for
/// it to exit, and return its exit status (killing it on timeout).
fn spawn_and_wait(
    data_dir: &std::path::Path,
    timeout: Duration,
    ctx: &str,
) -> std::process::ExitStatus {
    let bin = env!("CARGO_BIN_EXE_nodedb");
    let mut cmd = std::process::Command::new(bin);
    // Direct I/O stays on wherever the data directory supports it, so these
    // cases fail-stop against the same WAL a deployment opens. The opt-out is
    // set only when the probe proves the filesystem cannot do it — otherwise
    // every case here would exit non-zero for the wrong reason.
    super::support::direct_io::apply_wal_direct_io(&mut cmd, data_dir);
    let mut child = cmd
        .env("NODEDB_DATA_DIR", data_dir)
        .env("RUST_LOG", "error")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("failed to spawn nodedb binary");
    let deadline = std::time::Instant::now() + timeout;
    loop {
        match child.try_wait().expect("try_wait failed") {
            Some(s) => break s,
            None => {
                if std::time::Instant::now() >= deadline {
                    child.kill().ok();
                    panic!("nodedb did not exit within {timeout:?} ({ctx})");
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

/// The WAL segment filename for LSN 0 (the first segment a fresh node writes).
const SEGMENT_NAME: &str = "wal-00000000000000000000.seg";

/// Corrupt WAL content that looks like a valid page header but has a bad CRC.
/// The WAL reader validates CRC32C on every page, so this should cause an error.
const CORRUPT_CONTENT: &[u8] = b"NDBS\x00\x01\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00JUNK_CORRUPT_WAL_PAYLOAD_TO_FORCE_FAILURE";

#[test]
fn nodedb_exits_nonzero_on_corrupted_wal() {
    // Build a temporary data directory with a corrupt WAL segment.
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().to_path_buf();
    let wal_dir = data_dir.join("wal");
    fs::create_dir_all(&wal_dir).expect("create wal dir");
    fs::write(wal_dir.join(SEGMENT_NAME), CORRUPT_CONTENT).expect("write corrupt segment");

    let status = spawn_and_wait(&data_dir, Duration::from_secs(5), "corrupt WAL");
    assert!(
        !status.success(),
        "nodedb exited with success (0) despite corrupted WAL — expected non-zero exit"
    );
}

/// A catalog whose cross-table integrity check fails must abort startup —
/// the binary must exit non-zero, not come up answering `/healthz` with a
/// catalog it could not certify.
///
/// The fail-stop pieces exist individually (`verify_redb_integrity`
/// reports the divergence; `CatalogSanityReport::is_acceptable` returns
/// false on a non-empty divergence list; `await_cluster_ready` returns
/// `Err` when the report is not acceptable), but nothing pins that the
/// *process* actually refuses to start when they line up — which is the
/// exact wiring whose failure leaves "healthy /healthz, every DDL and
/// query fails, no client-visible signal that the boot went wrong". We
/// seed the smallest such divergence: an RLS policy that references a
/// collection that does not exist (one of the referential invariants the
/// integrity walker enforces). The catalog is otherwise a normal,
/// freshly-bootstrapped `system.redb`.
#[test]
fn nodedb_exits_nonzero_on_catalog_integrity_violation() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().to_path_buf();
    fs::create_dir_all(&data_dir).expect("create data dir");

    // Open the catalog the way the server does, then plant a dangling
    // reference: an RLS policy on a collection that was never created.
    {
        let creds =
            CredentialStore::open(&data_dir.join("system.redb")).expect("open credential store");
        let catalog = creds.catalog();
        catalog
            .put_rls_policy(&StoredRlsPolicy {
                tenant_id: 1,
                database_id: 0,
                collection: "collection_that_was_never_created".to_string(),
                display_collection: "collection_that_was_never_created".to_string(),
                name: "dangling_policy".to_string(),
                policy_type_tag: 0,
                predicate_text: String::new(),
                mode_tag: 0,
                on_deny_json: r#""Silent""#.to_string(),
                enabled: true,
                created_by: "admin".to_string(),
                created_at: 0,
            })
            .expect("write dangling rls policy");
        // `creds` (and its redb handle) drop here so the binary can open
        // the same file.
    }

    // Generous timeout: the catalog sanity check runs after the
    // raft/schema readiness gates, so the binary needs longer than the
    // corrupt-WAL case (which fails during WAL replay, much earlier).
    let status = spawn_and_wait(
        &data_dir,
        Duration::from_secs(20),
        "catalog integrity violation",
    );
    assert!(
        !status.success(),
        "nodedb exited 0 despite a catalog integrity violation (an RLS \
         policy referencing a non-existent collection) — the boot sanity \
         check must fail-stop the process, not log the divergence and serve \
         a half-loaded catalog"
    );
}
