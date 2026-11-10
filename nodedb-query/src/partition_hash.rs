// SPDX-License-Identifier: Apache-2.0

//! Stable, fixed-seed partition hash over a MessagePack row's join-key values.
//!
//! This is the ONE routing hash shared by the node-local grace-hash join (Data
//! Plane) and the cross-node shuffle producer (Control Plane). Both planes MUST
//! hash identically: a build-side row and a probe-side row with equal key values
//! have to land in the same partition (and therefore on the same consumer node)
//! or the distributed join silently loses matches. Keeping the hash in one
//! shared, pure function — rather than duplicating it per plane — makes that
//! invariant impossible to drift.
//!
//! Hashing rule (must match the join's hash-index key identity exactly):
//! hash the canonical identity [`join_key_identity`] gives the row's key
//! values. Numbers equal by exact value (`5`, `5.0`, `5.00`, `-0.0` and
//! `0.0`) share it, and so do a plain and a native row with equivalent key
//! values, so they route to the same partition. A missing field reads as
//! NULL, so a NULL-key row routes deterministically; the join then matches
//! it with no row.
//!
//! Only the key VALUES are hashed (never the field names), in `keys` order.
//! Uses [`std::hash::DefaultHasher`] (deterministic, fixed keys) rather than
//! `RandomState`, so the same `(keys, values)` always produces the same hash
//! across processes and nodes.

use std::hash::Hasher;

use crate::expr::EvalError;
use crate::msgpack_scan::join_key_identity;

/// Fixed-seed partition hash over `doc`'s `keys` values (seed `0`).
///
/// `keys` is generic over `AsRef<str>` so callers pass `&[&str]` or `&[String]`
/// without an intermediate allocation. This is the TOP-LEVEL partition routing
/// hash; the seed-`0` behaviour is fixed and must not change. A row that is
/// not a decodable object is an error.
pub fn partition_hash<S: AsRef<str>>(doc: &[u8], keys: &[S]) -> Result<u64, EvalError> {
    partition_hash_seeded(doc, keys, 0)
}

/// Seeded variant of [`partition_hash`] for RECURSIVE re-partitioning of a
/// skewed partition. The seed is mixed in BEFORE the key identity so the same
/// `(seed, keys)` pair on build and probe sides produces matching routing while
/// a fresh seed redistributes distinct keys across sub-buckets.
pub fn partition_hash_seeded<S: AsRef<str>>(
    doc: &[u8],
    keys: &[S],
    seed: u64,
) -> Result<u64, EvalError> {
    let identity = join_key_identity(doc, keys)?;
    let mut hasher = std::hash::DefaultHasher::new();
    hasher.write_u64(seed);
    hasher.write(identity.as_bytes());
    Ok(hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(fields: &[(&str, serde_json::Value)]) -> Vec<u8> {
        let mut map = serde_json::Map::new();
        for (k, v) in fields {
            map.insert((*k).to_string(), v.clone());
        }
        nodedb_types::json_to_msgpack(&serde_json::Value::Object(map)).expect("encode row")
    }

    #[test]
    fn equal_key_bytes_hash_identically() {
        let a = row(&[("k", serde_json::json!(7)), ("v", serde_json::json!("a"))]);
        let b = row(&[("k", serde_json::json!(7)), ("v", serde_json::json!("b"))]);
        assert_eq!(
            partition_hash(&a, &["k"]).unwrap(),
            partition_hash(&b, &["k"]).unwrap(),
            "equal key bytes must co-locate regardless of other columns"
        );
    }

    #[test]
    fn distinct_keys_usually_differ() {
        let a = row(&[("k", serde_json::json!(1))]);
        let b = row(&[("k", serde_json::json!(2))]);
        assert_ne!(
            partition_hash(&a, &["k"]).unwrap(),
            partition_hash(&b, &["k"]).unwrap()
        );
    }

    #[test]
    fn missing_field_uses_nil_sentinel() {
        let present = row(&[("other", serde_json::json!(1))]);
        // Hashing a missing "k" must be deterministic (it reads NULL), so two rows
        // both missing "k" co-locate.
        let other = row(&[("zzz", serde_json::json!(9))]);
        assert_eq!(
            partition_hash(&present, &["k"]).unwrap(),
            partition_hash(&other, &["k"]).unwrap()
        );
    }

    #[test]
    fn dotted_key_falls_back_to_trailing_segment() {
        let r = row(&[("id", serde_json::json!(5))]);
        assert_eq!(
            partition_hash(&r, &["t.id"]).unwrap(),
            partition_hash(&r, &["id"]).unwrap(),
            "qualified key resolves to the unqualified column"
        );
    }

    #[test]
    fn seed_changes_routing() {
        let r = row(&[("k", serde_json::json!(3))]);
        assert_ne!(
            partition_hash_seeded(&r, &["k"], 0).unwrap(),
            partition_hash_seeded(&r, &["k"], 1).unwrap(),
            "a different seed must redistribute the same key"
        );
    }

    #[test]
    fn plain_and_native_rows_route_to_the_same_partition() {
        use nodedb_types::Value;
        for value in [
            Value::Integer(42),
            Value::Float(0.5),
            Value::String("k".into()),
            Value::Array(vec![Value::Integer(1), Value::String("x".into())]),
        ] {
            let doc = Value::Object([("k".to_owned(), value.clone())].into_iter().collect());
            let native = zerompk::to_msgpack_vec(&doc).expect("native row");
            let plain = nodedb_types::value_to_msgpack(&doc).expect("plain row");
            for seed in [0, 7] {
                assert_eq!(
                    partition_hash_seeded(&native, &["k"], seed).unwrap(),
                    partition_hash_seeded(&plain, &["k"], seed).unwrap(),
                    "{value:?} seed {seed}"
                );
            }
        }
    }

    #[test]
    fn equal_numbers_of_every_type_route_together() {
        use nodedb_types::Value;
        let route = |value: Value| {
            let doc = Value::Object([("k".to_owned(), value)].into_iter().collect());
            partition_hash(&zerompk::to_msgpack_vec(&doc).unwrap(), &["k"]).unwrap()
        };
        let five = route(Value::Integer(5));
        assert_eq!(route(Value::Float(5.0)), five);
        assert_eq!(route(Value::Decimal("5.00".parse().unwrap())), five);
        assert_eq!(route(Value::Float(-0.0)), route(Value::Integer(0)));
        assert_eq!(route(Value::Null), route(Value::Null));
    }

    #[test]
    fn malformed_row_is_an_error() {
        assert!(partition_hash(&[0x92, 7, 0x81, 0xa1, b'k'], &["k"]).is_err());
    }
}
