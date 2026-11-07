// SPDX-License-Identifier: BUSL-1.1

//! The sum of two write answers.
//!
//! A Calvin slice answers one reply for all its plans, and a transaction folds
//! one answer from its slices. Each adds the affected counts of what it holds.

use nodedb_query::msgpack_scan::{extract_field, read_integer, read_str};

use super::staging_predicates::{
    extract_affected_count, extract_kv_conflict_op, extract_rejected_count,
};

/// The fields of a staged write's MessagePack answer that add up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AffectedFields<'a> {
    pub affected: u64,
    pub rejected: u64,
    pub collection: Option<&'a str>,
    pub op: Option<&'a str>,
}

/// Read `payload`'s count fields in place, with no decoded tree. `None` when it
/// carries no count. The key order matches `extract_affected_count`.
pub fn read_affected_fields(payload: &[u8]) -> Option<AffectedFields<'_>> {
    let integer = |key: &str| {
        extract_field(payload, 0, key)
            .and_then(|(start, _)| read_integer(payload, start))
            .and_then(|value| u64::try_from(value).ok())
    };
    let text =
        |key: &str| extract_field(payload, 0, key).and_then(|(start, _)| read_str(payload, start));
    let affected = ["affected", "truncated", "inserted", "accepted", "deleted"]
        .into_iter()
        .find_map(integer)?;
    Some(AffectedFields {
        affected,
        rejected: integer("rejected").unwrap_or(0),
        collection: text("collection"),
        op: text("op"),
    })
}

/// `first` with its affected and rejected counts raised by `second`'s, `None`
/// when either carries no affected count. Fields only `second` carries join it.
/// A `collection` beside two `rejected` counts stays `first`'s: only a staged
/// ingest's answer carries `rejected`, and it names no collection. An `op` of `insert`
/// on either side answers `insert`, as an `ON CONFLICT DO UPDATE` of mixed rows
/// tags `INSERT`. The encoding has a fixed key order, so one input pair encodes
/// to one output on every node.
pub fn merge_affected_payloads(first: &[u8], second: &[u8]) -> Option<Vec<u8>> {
    let affected = extract_affected_count(first)?.saturating_add(extract_affected_count(second)?);
    let rejected = extract_rejected_count(first).saturating_add(extract_rejected_count(second));
    let op = merged_op(
        extract_kv_conflict_op(first),
        extract_kv_conflict_op(second),
    );
    let serde_json::Value::Object(mut merged) = nodedb_types::json_from_msgpack(first).ok()? else {
        return None;
    };
    if let Ok(serde_json::Value::Object(incoming)) = nodedb_types::json_from_msgpack(second) {
        for (key, value) in incoming {
            merged.entry(key).or_insert(value);
        }
    }
    merged.insert("affected".to_owned(), serde_json::Value::from(affected));
    if rejected > 0 {
        merged.insert("rejected".to_owned(), serde_json::Value::from(rejected));
    }
    if let Some(op) = op {
        merged.insert("op".to_owned(), serde_json::Value::from(op));
    }
    nodedb_types::json_to_msgpack(&serde_json::Value::Object(merged)).ok()
}

fn merged_op(first: Option<String>, second: Option<String>) -> Option<String> {
    match (first, second) {
        (Some(first), Some(second)) if first == "insert" || second == "insert" => {
            Some("insert".to_owned())
        }
        (Some(op), _) | (_, Some(op)) => Some(op),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn count(affected: u64) -> Vec<u8> {
        nodedb_types::json_to_msgpack(&serde_json::json!({ "affected": affected })).expect("encode")
    }

    fn with_op(affected: u64, op: &str) -> Vec<u8> {
        nodedb_types::json_to_msgpack(&serde_json::json!({ "affected": affected, "op": op }))
            .expect("encode")
    }

    #[test]
    fn two_rejected_counts_add() {
        let first = nodedb_types::json_to_msgpack(&serde_json::json!({
            "affected": 2, "rejected": 1
        }))
        .expect("encode");
        let second = nodedb_types::json_to_msgpack(&serde_json::json!({
            "affected": 3, "rejected": 4
        }))
        .expect("encode");
        let merged = merge_affected_payloads(&first, &second).expect("merged");
        assert_eq!(extract_affected_count(&merged), Some(5));
        assert_eq!(extract_rejected_count(&merged), 5);
    }

    #[test]
    fn the_field_reader_reads_what_the_tree_reader_reads() {
        let payload = with_op(4, "update");
        let fields = read_affected_fields(&payload).expect("a count");
        assert_eq!(fields.affected, 4);
        assert_eq!(fields.op, Some("update"));
        assert_eq!(fields.rejected, 0);
        assert_eq!(read_affected_fields(&[]), None);
        assert_eq!(read_affected_fields(&[0x80]), None);
    }

    #[test]
    fn two_counts_add() {
        let merged = merge_affected_payloads(&count(2), &count(3)).expect("merged");
        assert_eq!(extract_affected_count(&merged), Some(5));
    }

    #[test]
    fn a_no_op_adds_nothing() {
        let merged = merge_affected_payloads(&count(1), &count(0)).expect("merged");
        assert_eq!(extract_affected_count(&merged), Some(1));
    }

    #[test]
    fn a_payload_without_a_count_does_not_merge() {
        assert!(merge_affected_payloads(&count(1), &[]).is_none());
        assert!(merge_affected_payloads(&[], &count(1)).is_none());
    }

    #[test]
    fn an_insert_op_wins_and_an_update_op_stays() {
        let mixed =
            merge_affected_payloads(&with_op(1, "update"), &with_op(1, "insert")).expect("merged");
        assert_eq!(extract_kv_conflict_op(&mixed).as_deref(), Some("insert"));
        assert_eq!(extract_affected_count(&mixed), Some(2));

        let update = merge_affected_payloads(&count(1), &with_op(1, "update")).expect("merged");
        assert_eq!(extract_kv_conflict_op(&update).as_deref(), Some("update"));
    }

    #[test]
    fn one_pair_encodes_to_one_output() {
        let first = merge_affected_payloads(&with_op(1, "insert"), &count(1)).expect("merged");
        let second = merge_affected_payloads(&with_op(1, "insert"), &count(1)).expect("merged");
        assert_eq!(first, second);
    }
}
