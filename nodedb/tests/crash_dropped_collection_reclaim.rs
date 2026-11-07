// SPDX-License-Identifier: BUSL-1.1

//! Pins whether `DROP COLLECTION` actually unlinks a vector/spatial
//! collection's index checkpoint files, or only appears to. Checkpoints
//! shard per core as `{data_dir}/{vector,spatial}-ckpt/core-{id}/gen-{n}/`,
//! and a reclaim that reads the wrong level finds nothing to delete. Uses a
//! short checkpoint interval, unlike most crash tests in this family, since
//! a file must actually exist on disk before the drop for reclaim to prove
//! anything.

mod crash_harness;

use std::time::{Duration, Instant};

use crash_harness::CrashHarness;
use crash_harness::log_fields::{boot_section, log_field};

const CHECKPOINT_WAIT: Duration = Duration::from_secs(20);

/// The boot that follows the test's single `kill_9` and `reopen`.
const RESTART_BOOT: u32 = 2;

/// The line each Data Plane core logs when it finishes a checkpoint.
const CORE_CHECKPOINT_DONE: &str = "core checkpoint complete";

/// Server env for a harness that must actually produce a checkpoint file
/// within the test's runtime. A 1-second interval guarantees at least one
/// flush fires well inside the `CHECKPOINT_WAIT` poll budget below. The log
/// filter keeps the per-core checkpoint line, which the restart wait reads.
fn checkpoint_every_second() -> CrashHarness {
    CrashHarness::new()
        .with_env("NODEDB_CHECKPOINT_INTERVAL_SECS", "1")
        .with_env("RUST_LOG", "error,nodedb=info")
}

/// Wait until a full checkpoint cycle has run after the restart, bounded by
/// `CHECKPOINT_WAIT`. A core that logs the completion line twice proves the
/// first cycle finished on every core, since a cycle waits for all of them.
/// A restarted core that still holds the dropped collection's index writes
/// its files in that cycle, so a file check before it proves nothing.
async fn wait_for_post_restart_checkpoint_cycle(h: &CrashHarness) {
    let deadline = Instant::now() + CHECKPOINT_WAIT;
    loop {
        let log = boot_section(&h.server_log(), RESTART_BOOT);
        let cores = log_field(&log, CORE_CHECKPOINT_DONE, "core");
        let max_per_core = cores
            .iter()
            .map(|core| cores.iter().filter(|c| *c == core).count())
            .max()
            .unwrap_or(0);
        if max_per_core >= 2 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "no full checkpoint cycle ran after the restart within {CHECKPOINT_WAIT:?}; a file \
             check now would pass whether or not the dropped collection's index was reloaded.{}",
            h.keep_data_dir_note()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Recursively collect every file path under `root` (root need not exist —
/// an absent directory just yields an empty list, matching a collection
/// whose engine never flushed anything).
fn walk_files(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.filter_map(|e| e.ok()) {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else {
                out.push(path);
            }
        }
    }
    out
}

/// Poll `root` recursively until at least one `*.ckpt` file exists, bounded
/// by `CHECKPOINT_WAIT`. A timeout means the test can prove nothing about
/// reclaim, since there would be no file on disk either way.
fn wait_for_any_ckpt_file(root: &std::path::Path, engine: &str) -> std::path::PathBuf {
    let deadline = Instant::now() + CHECKPOINT_WAIT;
    loop {
        if let Some(f) = walk_files(root)
            .into_iter()
            .find(|p| p.extension().and_then(|e| e.to_str()) == Some("ckpt"))
        {
            return f;
        }
        assert!(
            Instant::now() < deadline,
            "no {engine} checkpoint (*.ckpt) file appeared under {} within {CHECKPOINT_WAIT:?} \
             even with a 1-second checkpoint interval; the test's premise — that a real \
             checkpoint file exists on disk before the drop — never held, so this run cannot \
             say anything about whether DROP COLLECTION reclaims it",
            root.display()
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// True if any file anywhere under `root` has `needle` in its file name.
/// Filenames encode the collection name directly, so a substring match on a
/// distinctive name is unambiguous without knowing the database/tenant id.
fn any_file_name_contains(root: &std::path::Path, needle: &str) -> Option<std::path::PathBuf> {
    walk_files(root).into_iter().find(|p| {
        p.file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.contains(needle))
    })
}

/// Connect fresh and assert `sql` fails with a "collection not found" style
/// error — the direct proof that the collection itself did not resurrect,
/// independent of whatever its index checkpoint files still say on disk.
async fn assert_collection_missing(h: &CrashHarness, sql: &str, collection: &str) {
    let (client, connection) = tokio_postgres::connect(&h.pgwire_conn_str(), tokio_postgres::NoTls)
        .await
        .expect("connect to post-restart server");
    let conn_handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    let result = client.simple_query(sql).await;
    drop(client);
    let _ = conn_handle.await;
    let err = result.err().unwrap_or_else(|| {
        panic!(
            "'{sql}' succeeded after restart; the dropped collection '{collection}' \
             resurrected instead of staying gone"
        )
    });
    let message = err
        .as_db_error()
        .map(|db| db.message().to_string())
        .unwrap_or_else(|| err.to_string());
    // Accept either phrasing the server uses for an absent collection; the assertion
    // that matters is that the query FAILED naming this collection, not the wording.
    let lowered = message.to_lowercase();
    assert!(
        message.contains(collection)
            && (lowered.contains("not found") || lowered.contains("does not exist")),
        "expected a 'collection not found' style error naming '{collection}' after restart, \
         got: {message:?}"
    );
}

/// The claim under test, for the vector engine: a checkpoint file for the
/// dropped collection must be gone after `DROP COLLECTION` + restart, and
/// the collection itself must not be queryable.
#[tokio::test(flavor = "multi_thread")]
async fn dropped_vector_collection_does_not_resurrect_after_restart() {
    const COLLECTION: &str = "crashdropvec";

    let mut h = checkpoint_every_second();
    h.spawn();
    h.wait_ready();

    h.exec(&format!("CREATE COLLECTION {COLLECTION} TYPE document"))
        .await;
    h.exec(&format!(
        "CREATE VECTOR INDEX idx_{COLLECTION} ON {COLLECTION} (embedding) METRIC cosine DIM 4"
    ))
    .await;
    h.exec(&format!(
        "INSERT INTO {COLLECTION} (id, embedding) VALUES ('r1', ARRAY[1.0,0.0,0.0,0.0])"
    ))
    .await;
    h.exec(&format!(
        "INSERT INTO {COLLECTION} (id, embedding) VALUES ('r2', ARRAY[0.0,1.0,0.0,0.0])"
    ))
    .await;

    // Vector checkpoint filenames hex-encode the collection key, so no
    // collection name can escape the generation directory. Match on the same
    // encoding the writer uses.
    let needle: String = COLLECTION.bytes().map(|b| format!("{b:02x}")).collect();

    let ckpt_root = h.data_dir().join("vector-ckpt");
    let first_ckpt = wait_for_any_ckpt_file(&ckpt_root, "vector");
    assert!(
        any_file_name_contains(&ckpt_root, &needle).is_some(),
        "a vector checkpoint file exists ({}), but none of them name '{COLLECTION}' — test \
         setup is not exercising the collection under test",
        first_ckpt.display()
    );

    h.exec(&format!("DROP COLLECTION {COLLECTION} PURGE")).await;

    // A hard kill gives the reclaim path every chance to have already run —
    // it executes synchronously as part of DROP, before the kill.
    h.kill_9();
    h.reopen();

    assert_collection_missing(&h, &format!("SELECT id FROM {COLLECTION}"), COLLECTION).await;
    // Same rationale as the spatial test below.
    wait_for_post_restart_checkpoint_cycle(&h).await;

    let leaked = any_file_name_contains(&ckpt_root, &needle);
    assert!(
        leaked.is_none(),
        "vector checkpoint file for dropped collection '{COLLECTION}' is still on disk after \
         DROP COLLECTION + restart: {leaked:?} (all files under {}: {:?})",
        ckpt_root.display(),
        walk_files(&ckpt_root)
    );
}

/// Same claim, spatial engine: mirrors
/// `crash_recovery::spatial_index_survives_kill_9`'s DDL/insert shape.
#[tokio::test(flavor = "multi_thread")]
async fn dropped_spatial_collection_does_not_resurrect_after_restart() {
    const COLLECTION: &str = "crashdropspatial";

    let mut h = checkpoint_every_second();
    h.spawn();
    h.wait_ready();

    h.exec(&format!(
        "CREATE COLLECTION {COLLECTION} (id TEXT, location GEOMETRY SPATIAL_INDEX, name TEXT) \
         WITH (engine='spatial')"
    ))
    .await;
    h.exec(&format!(
        "INSERT INTO {COLLECTION} (id, location, name) \
         VALUES ('p1', ST_Point(-73.9857, 40.7580), 'Times Square')"
    ))
    .await;
    h.exec(&format!(
        "INSERT INTO {COLLECTION} (id, location, name) \
         VALUES ('p2', ST_Point(2.3522, 48.8566), 'Paris')"
    ))
    .await;

    let ckpt_root = h.data_dir().join("spatial-ckpt");
    let first_ckpt = wait_for_any_ckpt_file(&ckpt_root, "spatial");
    assert!(
        any_file_name_contains(&ckpt_root, COLLECTION).is_some(),
        "a spatial checkpoint file exists ({}), but none of them name '{COLLECTION}' — test \
         setup is not exercising the collection under test",
        first_ckpt.display()
    );

    h.exec(&format!("DROP COLLECTION {COLLECTION} PURGE")).await;

    // Same rationale as the vector test above.
    h.kill_9();
    h.reopen();

    assert_collection_missing(&h, &format!("SELECT id FROM {COLLECTION}"), COLLECTION).await;
    // A restarted core that still holds the dropped collection's index writes
    // its files again on the first cycle, so a check before any cycle proves
    // nothing.
    wait_for_post_restart_checkpoint_cycle(&h).await;

    let leaked = any_file_name_contains(&ckpt_root, COLLECTION);
    assert!(
        leaked.is_none(),
        "spatial checkpoint file for dropped collection '{COLLECTION}' is still on disk after \
         DROP COLLECTION + restart: {leaked:?} (all files under {}: {:?})",
        ckpt_root.display(),
        walk_files(&ckpt_root)
    );
}

/// The columnar checkpoint filename's collection part: the name's bytes as
/// lowercase hex, as the write path encodes it.
fn hex_name(collection: &str) -> String {
    collection.bytes().map(|b| format!("{b:02x}")).collect()
}

/// Same claim, plain columnar engine. The columnar checkpoint is what every
/// columnar-family collection restores from, `spatial` included, so a file
/// left behind resurrects the dropped rows and a same-name CREATE inherits
/// them.
#[tokio::test(flavor = "multi_thread")]
async fn dropped_columnar_collection_does_not_resurrect_after_restart() {
    const COLLECTION: &str = "crashdropcolumnar";

    let mut h = checkpoint_every_second();
    h.spawn();
    h.wait_ready();

    h.exec(&format!(
        "CREATE COLLECTION {COLLECTION} (id TEXT, n INT) WITH (engine='columnar')"
    ))
    .await;
    for id in 0..3 {
        h.exec(&format!(
            "INSERT INTO {COLLECTION} (id, n) VALUES ('r{id}', {id})"
        ))
        .await;
    }

    let ckpt_root = h.data_dir().join("columnar-ckpt");
    let needle = format!("-coll-{}.ckpt", hex_name(COLLECTION));
    let deadline = Instant::now() + CHECKPOINT_WAIT;
    while any_file_name_contains(&ckpt_root, &needle).is_none() {
        assert!(
            Instant::now() < deadline,
            "no columnar checkpoint file named '{COLLECTION}' within {CHECKPOINT_WAIT:?}, so the \
             test can prove nothing about reclaim (files: {:?})",
            walk_files(&ckpt_root)
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    h.exec(&format!("DROP COLLECTION {COLLECTION} PURGE")).await;
    h.kill_9();
    h.reopen();

    assert_collection_missing(&h, &format!("SELECT id FROM {COLLECTION}"), COLLECTION).await;
    // Same rationale as the spatial test above.
    wait_for_post_restart_checkpoint_cycle(&h).await;
    let leaked = any_file_name_contains(&ckpt_root, &needle);
    assert!(
        leaked.is_none(),
        "columnar checkpoint file for dropped collection '{COLLECTION}' is still on disk after \
         DROP COLLECTION + restart: {leaked:?} (all files under {}: {:?})",
        ckpt_root.display(),
        walk_files(&ckpt_root)
    );

    h.exec(&format!(
        "CREATE COLLECTION {COLLECTION} (id TEXT, n INT) WITH (engine='columnar')"
    ))
    .await;
    let count = h
        .query_col_idx(&format!("SELECT COUNT(*) FROM {COLLECTION}"), 0)
        .await;
    assert_eq!(
        count,
        vec!["0".to_string()],
        "a same-name CREATE after the drop inherited the dropped collection's rows"
    );
}
