//! Bitemporal DML correctness: UPDATE/DELETE/UPSERT on a bitemporal
//! collection must append versions (copy-on-write), never overwrite.
//! Every past version must remain accessible via `versioned_get_as_of`.
//!
//! These tests exercise the engine layer directly because the handler
//! path under `CoreLoop` requires a full io_uring + memory-engine
//! harness. The engine invariants — "put/delete append; history is
//! preserved; current-state views match as-of(now); tombstones mask
//! prior versions from current-state reads only" — are the correctness
//! contract the handlers rely on.

use nodedb::engine::document::store::{CollectionConfig, DocumentEngine};
use nodedb::engine::sparse::btree::SparseEngine;

fn open() -> (SparseEngine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let e = SparseEngine::open(&dir.path().join("bi.redb")).unwrap();
    (e, dir)
}

fn register(sparse: &SparseEngine) -> DocumentEngine<'_> {
    let mut e = DocumentEngine::new(sparse, 1);
    e.register_collection(CollectionConfig::new("c").with_bitemporal(true));
    e
}

fn wall_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[test]
fn update_via_put_creates_new_version_and_preserves_old() {
    let (sparse, _d) = open();
    let engine = register(&sparse);

    engine.put("c", "k", &serde_json::json!({"v": 1})).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(5));
    let t_mid = wall_ms();
    std::thread::sleep(std::time::Duration::from_millis(5));
    engine.put("c", "k", &serde_json::json!({"v": 2})).unwrap();

    assert_eq!(engine.get("c", "k").unwrap().unwrap()["v"], 2);

    // History at t_mid still sees v=1.
    let body = sparse
        .versioned_get_as_of(1, "c", "k", Some(t_mid), None)
        .unwrap()
        .expect("historical version");
    let rmpv_val = rmpv::decode::read_value(&mut body.as_slice()).unwrap();
    let as_json: serde_json::Value = rmpv_to_json(&rmpv_val);
    assert_eq!(as_json["v"], 1);
}

#[test]
fn delete_appends_tombstone_but_prior_version_still_visible_as_of() {
    let (sparse, _d) = open();
    let engine = register(&sparse);

    engine
        .put("c", "k", &serde_json::json!({"name": "Alice"}))
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(5));
    let t_before_delete = wall_ms();
    std::thread::sleep(std::time::Duration::from_millis(5));
    assert!(engine.delete("c", "k").unwrap());

    // Current-state read: None.
    assert!(engine.get("c", "k").unwrap().is_none());

    // Historical read at t_before_delete: still Alice.
    let body = sparse
        .versioned_get_as_of(1, "c", "k", Some(t_before_delete), None)
        .unwrap()
        .expect("pre-delete version still reachable");
    let rmpv_val = rmpv::decode::read_value(&mut body.as_slice()).unwrap();
    assert_eq!(rmpv_to_json(&rmpv_val)["name"], "Alice");
}

#[test]
fn ten_sequential_updates_produce_ten_reachable_versions() {
    let (sparse, _d) = open();
    let engine = register(&sparse);
    let mut cutoffs: Vec<(i64, i64)> = Vec::new(); // (cutoff_ms, expected_v)
    for i in 1..=10 {
        engine.put("c", "k", &serde_json::json!({"v": i})).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(3));
        cutoffs.push((wall_ms(), i));
        std::thread::sleep(std::time::Duration::from_millis(3));
    }
    for (cutoff, expected) in &cutoffs {
        let body = sparse
            .versioned_get_as_of(1, "c", "k", Some(*cutoff), None)
            .unwrap()
            .unwrap_or_else(|| panic!("missing version at cutoff {cutoff}"));
        let rmpv_val = rmpv::decode::read_value(&mut body.as_slice()).unwrap();
        let v = rmpv_to_json(&rmpv_val)["v"].as_i64().unwrap();
        assert_eq!(v, *expected, "at cutoff {cutoff}");
    }
}

#[test]
fn secondary_index_reflects_each_version_independently() {
    let (sparse, _d) = open();
    let mut engine = DocumentEngine::new(&sparse, 1);
    engine.register_collection(
        CollectionConfig::new("c")
            .with_bitemporal(true)
            .with_index("$.email"),
    );

    engine
        .put("c", "u1", &serde_json::json!({"email": "a@x.com"}))
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(5));
    let t_mid = wall_ms();
    std::thread::sleep(std::time::Duration::from_millis(5));
    engine
        .put("c", "u1", &serde_json::json!({"email": "b@x.com"}))
        .unwrap();

    // Past: "a@x.com" → u1 at t_mid.
    let ids_a_mid = sparse
        .versioned_index_lookup_as_of(1, "c", "$.email", "a@x.com", Some(t_mid))
        .unwrap();
    assert_eq!(ids_a_mid, vec!["u1"]);

    // Current: "b@x.com" → u1.
    let ids_b_now = sparse
        .versioned_index_lookup_as_of(1, "c", "$.email", "b@x.com", None)
        .unwrap();
    assert_eq!(ids_b_now, vec!["u1"]);

    // After delete → no current entry for b@x.com either.
    engine.delete("c", "u1").unwrap();
    let ids_b_after = sparse
        .versioned_index_lookup_as_of(1, "c", "$.email", "b@x.com", None)
        .unwrap();
    assert!(ids_b_after.is_empty(), "tombstone hides current lookup");
    // But historical lookup at t_mid still surfaces a@x.com → u1.
    let ids_a_still = sparse
        .versioned_index_lookup_as_of(1, "c", "$.email", "a@x.com", Some(t_mid))
        .unwrap();
    assert_eq!(ids_a_still, vec!["u1"]);
}

#[test]
fn re_put_after_tombstone_is_a_live_resurrection() {
    let (sparse, _d) = open();
    let engine = register(&sparse);
    engine.put("c", "k", &serde_json::json!({"v": 1})).unwrap();
    engine.delete("c", "k").unwrap();
    assert!(engine.get("c", "k").unwrap().is_none());
    engine.put("c", "k", &serde_json::json!({"v": 2})).unwrap();
    let now = engine.get("c", "k").unwrap().unwrap();
    assert_eq!(now["v"], 2);
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
