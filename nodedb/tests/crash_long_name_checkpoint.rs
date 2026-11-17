// SPDX-License-Identifier: BUSL-1.1

//! Collections named with the full 255-byte limit checkpoint and restore
//! across a `kill -9`. Checkpoint file names are fixed-length hashes of the
//! key, so a long collection or field name cannot push a file name past the
//! filesystem limit. A checkpoint write that failed on the name would leave no
//! file holding the collection's key, and the wait below would time out.

mod crash_harness;

use std::time::{Duration, Instant};

use crash_harness::CrashHarness;

const CHECKPOINT_WAIT: Duration = Duration::from_secs(30);

/// Every file path under `root`. An absent directory yields nothing.
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

/// Whether a `*.ckpt` file under `root` stores a key holding `collection`.
fn has_ckpt_holding(root: &std::path::Path, collection: &str) -> bool {
    walk_files(root).into_iter().any(|p| {
        p.extension().and_then(|e| e.to_str()) == Some("ckpt")
            && nodedb_wal::segment::read_checkpoint_key(&p).is_ok_and(|key| {
                key.windows(collection.len())
                    .any(|w| w == collection.as_bytes())
            })
    })
}

/// Wait until `engine`'s checkpoint root holds a file for `collection`.
async fn wait_for_ckpt(h: &CrashHarness, engine_dir: &str, collection: &str) {
    let root = h.data_dir().join(engine_dir);
    let deadline = Instant::now() + CHECKPOINT_WAIT;
    while !has_ckpt_holding(&root, collection) {
        assert!(
            Instant::now() < deadline,
            "no {engine_dir} checkpoint for the {}-byte collection within {CHECKPOINT_WAIT:?} \
             (files: {:?}){}",
            collection.len(),
            walk_files(&root),
            h.keep_data_dir_note()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// A 255-byte collection name, distinct per engine by its leading byte.
fn long_name(lead: char) -> String {
    let mut name = String::with_capacity(255);
    name.push(lead);
    name.push_str(&"n".repeat(254));
    name
}

#[tokio::test(flavor = "multi_thread")]
async fn long_named_collections_checkpoint_and_restore_after_kill_9() {
    let kv = long_name('k');
    let vec_coll = long_name('v');
    let columnar = long_name('c');
    let field = "e".repeat(200);

    let mut h = CrashHarness::new()
        .with_env("NODEDB_CHECKPOINT_INTERVAL_SECS", "1")
        .with_env("RUST_LOG", "error,nodedb=info");
    h.spawn();
    h.wait_ready();

    h.exec(&format!(
        "CREATE COLLECTION {kv} (k STRING PRIMARY KEY, v STRING) WITH (engine='kv')"
    ))
    .await;
    h.exec(&format!("INSERT INTO {kv} (k, v) VALUES ('key1', 'val1')"))
        .await;

    h.exec(&format!("CREATE COLLECTION {vec_coll} TYPE document"))
        .await;
    h.exec(&format!(
        "CREATE VECTOR INDEX idx_long_field ON {vec_coll} ({field}) METRIC cosine DIM 4"
    ))
    .await;
    h.exec(&format!(
        "INSERT INTO {vec_coll} (id, {field}) VALUES ('r1', ARRAY[1.0,0.0,0.0,0.0])"
    ))
    .await;
    h.exec(&format!(
        "INSERT INTO {vec_coll} (id, {field}) VALUES ('r2', ARRAY[0.0,1.0,0.0,0.0])"
    ))
    .await;

    h.exec(&format!(
        "CREATE COLLECTION {columnar} (id TEXT, n INT) WITH (engine='columnar')"
    ))
    .await;
    h.exec(&format!("INSERT INTO {columnar} (id, n) VALUES ('r1', 1)"))
        .await;

    wait_for_ckpt(&h, "kv-ckpt", &kv).await;
    wait_for_ckpt(&h, "vector-ckpt", &format!("{vec_coll}:{field}")).await;
    wait_for_ckpt(&h, "columnar-ckpt", &columnar).await;

    h.kill_9();
    h.reopen();

    let kv_rows = h
        .query_col(&format!("SELECT v FROM {kv} WHERE k = 'key1'"), "v")
        .await;
    assert_eq!(
        kv_rows,
        vec!["val1".to_string()],
        "KV row lost: {kv_rows:?}"
    );

    for (probe, want) in [
        ("ARRAY[1.0,0.0,0.0,0.0]", "r1"),
        ("ARRAY[0.0,1.0,0.0,0.0]", "r2"),
    ] {
        let nn = h
            .query_col(
                &format!(
                    "SELECT id FROM {vec_coll} ORDER BY vector_distance({field}, {probe}) LIMIT 1"
                ),
                "id",
            )
            .await;
        assert_eq!(
            nn,
            vec![want.to_string()],
            "vector index on a 200-byte field did not restore: nearest to {probe} was {nn:?}"
        );
    }

    let columnar_rows = h
        .query_col(&format!("SELECT id FROM {columnar}"), "id")
        .await;
    assert_eq!(
        columnar_rows,
        vec!["r1".to_string()],
        "columnar row lost: {columnar_rows:?}"
    );
}
