//! Bitemporal document storage — end-to-end from `DocumentEngine` to
//! the versioned redb table.
//!
//! The SQL surface is exercised via the `FOR SYSTEM_TIME AS OF` tests in
//! `scripts/test.sql` (runs against a live server); this Rust test covers
//! the engine layer: the `CollectionConfig.bitemporal` flag must route
//! every write through the versioned path, every current-state read via
//! Ceiling-at-head, and arbitrary system-time cutoffs via
//! `versioned_get_as_of`.

use nodedb::engine::document::store::{CollectionConfig, DocumentEngine};
use nodedb::engine::sparse::btree::SparseEngine;

fn open() -> (SparseEngine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let e = SparseEngine::open(&dir.path().join("bi.redb")).unwrap();
    (e, dir)
}

#[test]
fn bitemporal_put_and_current_get_roundtrip() {
    let (sparse, _d) = open();
    let mut engine = DocumentEngine::new(&sparse, 1);
    engine.register_collection(CollectionConfig::new("users").with_bitemporal(true));

    engine
        .put("users", "u1", &serde_json::json!({"name": "Alice"}))
        .unwrap();

    let got = engine.get("users", "u1").unwrap().unwrap();
    assert_eq!(got["name"], "Alice");
}

#[test]
fn bitemporal_delete_appends_tombstone_so_current_get_is_none() {
    let (sparse, _d) = open();
    let mut engine = DocumentEngine::new(&sparse, 1);
    engine.register_collection(CollectionConfig::new("c").with_bitemporal(true));

    engine.put("c", "a", &serde_json::json!({"v": 1})).unwrap();
    assert!(engine.get("c", "a").unwrap().is_some());
    let removed = engine.delete("c", "a").unwrap();
    assert!(removed, "delete should report row was live");
    assert!(
        engine.get("c", "a").unwrap().is_none(),
        "after tombstone current-state get is None"
    );
}

#[test]
fn bitemporal_multiple_puts_retain_history_via_versioned_get_as_of() {
    let (sparse, _d) = open();
    let mut engine = DocumentEngine::new(&sparse, 1);
    engine.register_collection(CollectionConfig::new("c").with_bitemporal(true));

    // Three puts create three versions. The versioned API is the way
    // callers query history — engine.get() returns current state only.
    engine.put("c", "k", &serde_json::json!({"v": 1})).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(5));
    let t_mid = wall_ms();
    std::thread::sleep(std::time::Duration::from_millis(5));
    engine.put("c", "k", &serde_json::json!({"v": 2})).unwrap();

    // A cutoff before the second write should surface v=1.
    let body = sparse
        .versioned_get_as_of(1, "c", "k", Some(t_mid), None)
        .unwrap()
        .expect("version at cutoff");
    let val: serde_json::Value = {
        let rmpv_val = rmpv::decode::read_value(&mut body.as_slice()).unwrap();
        rmpv_to_json(&rmpv_val)
    };
    assert_eq!(val["v"], 1);

    // Current state reflects the latest write.
    let now = engine.get("c", "k").unwrap().unwrap();
    assert_eq!(now["v"], 2);
}

#[test]
fn non_bitemporal_collection_uses_legacy_storage() {
    let (sparse, _d) = open();
    let mut engine = DocumentEngine::new(&sparse, 1);
    engine.register_collection(CollectionConfig::new("c"));
    engine.put("c", "k", &serde_json::json!({"v": 1})).unwrap();

    // The versioned table must be empty for this collection because
    // writes went to the legacy path.
    let bitemporal_rows = sparse
        .versioned_scan_as_of(1, "c", None, None, 100)
        .unwrap();
    assert!(
        bitemporal_rows.is_empty(),
        "non-bitemporal collection should not populate the versioned table"
    );
    // And the legacy read still works.
    let got = engine.get("c", "k").unwrap().unwrap();
    assert_eq!(got["v"], 1);
}

fn wall_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn rmpv_to_json(v: &rmpv::Value) -> serde_json::Value {
    match v {
        rmpv::Value::Nil => serde_json::Value::Null,
        rmpv::Value::Boolean(b) => serde_json::Value::Bool(*b),
        rmpv::Value::Integer(i) => {
            if let Some(n) = i.as_i64() {
                serde_json::Value::Number(n.into())
            } else if let Some(n) = i.as_u64() {
                serde_json::Value::Number(n.into())
            } else {
                serde_json::Value::Null
            }
        }
        rmpv::Value::F64(f) => serde_json::Number::from_f64(*f)
            .map_or(serde_json::Value::Null, serde_json::Value::Number),
        rmpv::Value::F32(f) => serde_json::Number::from_f64(*f as f64)
            .map_or(serde_json::Value::Null, serde_json::Value::Number),
        rmpv::Value::String(s) => serde_json::Value::String(s.as_str().unwrap_or("").into()),
        rmpv::Value::Array(arr) => serde_json::Value::Array(arr.iter().map(rmpv_to_json).collect()),
        rmpv::Value::Map(m) => {
            let mut out = serde_json::Map::new();
            for (k, v) in m {
                if let Some(key) = k.as_str() {
                    out.insert(key.to_string(), rmpv_to_json(v));
                }
            }
            serde_json::Value::Object(out)
        }
        rmpv::Value::Binary(b) => {
            serde_json::Value::Array(b.iter().map(|x| serde_json::Value::from(*x)).collect())
        }
        rmpv::Value::Ext(_, _) => serde_json::Value::Null,
    }
}
