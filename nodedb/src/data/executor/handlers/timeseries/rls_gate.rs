// SPDX-License-Identifier: BUSL-1.1

//! Row-level-security WRITE enforcement for timeseries ingest. A row only
//! exists once the line-protocol parser produces it, so the Control Plane
//! ships the compiled predicate and rows are decided here — every ingest
//! format normalizes into parsed ILP lines first, so this covers all of
//! them. The whole batch is decided before the first row is appended.

use std::collections::HashMap;

use nodedb_types::{RlsWriteCheck, Value, WriteGateDecision};

use super::{msgpack_decode, normalize};
use crate::data::executor::handlers::rls_write_gate::admit_value_row;
use crate::engine::timeseries::ilp::{self, FieldValue, IlpLine};

/// Nanoseconds per millisecond — line protocol timestamps are nanoseconds, the
/// stored time column is milliseconds.
const NANOS_PER_MILLI: i64 = 1_000_000;

/// Decide every parsed line against the compiled write policy. `time_key` is
/// `None` only for a measurement with no DDL, where no column can be bound
/// to the timestamp. Empty `rls_write_check` admits everything.
pub(in crate::data::executor) fn admit_ilp_lines(
    rls_write_check: &RlsWriteCheck,
    lines: &[IlpLine<'_>],
    time_key: Option<&str>,
    default_timestamp_ms: i64,
    tid: u64,
    collection: &str,
) -> crate::Result<()> {
    if matches!(rls_write_check.decision(), WriteGateDecision::AdmitAll) {
        return Ok(());
    }
    for line in lines {
        let image = line_image(line, time_key, default_timestamp_ms);
        admit_value_row(rls_write_check, &image, tid, collection)?;
    }
    Ok(())
}

/// Decide a structured MessagePack row batch against the compiled write
/// policy, normalized into line protocol first — same as the ingest handler
/// — so the policy sees STORED values, not submitted ones. A payload that
/// fails to decode or normalize is refused rather than admitted.
pub(in crate::data::executor) fn admit_msgpack_rows(
    rls_write_check: &RlsWriteCheck,
    payload: &[u8],
    measurement: &str,
    time_key: Option<&str>,
    default_timestamp_ms: i64,
    tid: u64,
    collection: &str,
) -> crate::Result<()> {
    if matches!(rls_write_check.decision(), WriteGateDecision::AdmitAll) {
        return Ok(());
    }
    let undecodable = || crate::Error::RejectedAuthz {
        tenant_id: crate::types::TenantId::new(tid),
        resource: format!(
            "RLS write policy on '{collection}': the row batch did not decode, so the policy \
             could not be evaluated against it"
        ),
    };
    let rows = msgpack_decode::decode_msgpack_rows(payload).map_err(|_| undecodable())?;
    let batch =
        normalize::msgpack_rows_to_ilp(&rows, measurement, time_key).map_err(|_| undecodable())?;
    let parsed = ilp::parse_batch(&batch).map_err(|_| undecodable())?;
    admit_ilp_lines(
        rls_write_check,
        parsed.lines(),
        time_key,
        default_timestamp_ms,
        tid,
        collection,
    )
}

/// Build the row image a line will be stored as: tags, fields, and timestamp
/// bound to the declared time column. The timestamp overwrites any field of
/// the same name unconditionally, matching what the ingest path stores.
fn line_image(line: &IlpLine<'_>, time_key: Option<&str>, default_timestamp_ms: i64) -> Value {
    let mut map: HashMap<String, Value> =
        HashMap::with_capacity(line.tags.len() + line.fields.len() + 1);
    for (key, value) in &line.tags {
        map.insert(key.to_string(), Value::String(value.to_string()));
    }
    for (key, value) in &line.fields {
        map.insert(key.to_string(), field_value(value));
    }
    if let Some(key) = time_key {
        let ts_ms = line
            .timestamp_ns
            .map(|ns| ns / NANOS_PER_MILLI)
            .unwrap_or(default_timestamp_ms);
        map.insert(key.to_string(), Value::Integer(ts_ms));
    }
    Value::Object(map)
}

/// An unsigned field beyond `i64::MAX` widens to a float rather than
/// wrapping negative, which would compare against the policy bound wrong.
fn field_value(value: &FieldValue<'_>) -> Value {
    match value {
        FieldValue::Float(f) => Value::Float(*f),
        FieldValue::Int(i) => Value::Integer(*i),
        FieldValue::UInt(u) => i64::try_from(*u)
            .map(Value::Integer)
            .unwrap_or_else(|_| Value::Float(*u as f64)),
        FieldValue::Str(s) => Value::String(s.to_string()),
        FieldValue::Bool(b) => Value::Bool(*b),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::scan_filter::{FilterOp, ScanFilter};

    fn owner_policy(owner: &str) -> Vec<u8> {
        let filter = ScanFilter {
            field: "owner".into(),
            op: FilterOp::Eq,
            value: Value::String(owner.into()),
            clauses: Vec::new(),
            expr: None,
        };
        zerompk::to_msgpack_vec(&vec![filter]).expect("encode policy filter")
    }

    fn lines(batch: &str) -> Vec<ilp::IlpLine<'_>> {
        ilp::parse_batch(batch).expect("parse ILP").into_lines()
    }

    #[test]
    fn an_empty_check_admits_every_line() {
        let batch = "cpu,owner=mallory value=1i\n";
        assert!(
            admit_ilp_lines(
                &RlsWriteCheck::NoPolicyApplies,
                &lines(batch),
                Some("ts"),
                0,
                1,
                "cpu"
            )
            .is_ok()
        );
    }

    #[test]
    fn a_conforming_line_is_admitted() {
        let batch = "cpu,owner=alice value=1i\n";
        assert!(
            admit_ilp_lines(
                &RlsWriteCheck::Predicate(owner_policy("alice")),
                &lines(batch),
                Some("ts"),
                0,
                1,
                "cpu"
            )
            .is_ok()
        );
    }

    /// One violating line fails the whole batch — the lines ahead of it must
    /// not become durable.
    #[test]
    fn one_violating_line_rejects_the_batch() {
        let batch = "cpu,owner=alice value=1i\ncpu,owner=mallory value=2i\n";
        assert!(matches!(
            admit_ilp_lines(
                &RlsWriteCheck::Predicate(owner_policy("alice")),
                &lines(batch),
                Some("ts"),
                0,
                1,
                "cpu"
            ),
            Err(crate::Error::RejectedAuthz { .. })
        ));
    }

    /// A line with no value for the governed column cannot satisfy the
    /// predicate, so it is rejected rather than admitted by omission.
    #[test]
    fn a_line_without_the_governed_column_is_rejected() {
        let batch = "cpu value=1i\n";
        assert!(
            admit_ilp_lines(
                &RlsWriteCheck::Predicate(owner_policy("alice")),
                &lines(batch),
                Some("ts"),
                0,
                1,
                "cpu"
            )
            .is_err()
        );
    }

    /// A filter payload that does not deserialize denies rather than passing
    /// the batch through unchecked.
    #[test]
    fn a_corrupt_check_denies() {
        let batch = "cpu,owner=alice value=1i\n";
        assert!(
            admit_ilp_lines(
                &RlsWriteCheck::Predicate(vec![0xFF, 0xFE]),
                &lines(batch),
                Some("ts"),
                0,
                1,
                "cpu"
            )
            .is_err()
        );
    }

    /// A plan that reached the gate without RLS injection is denied, not
    /// admitted.
    #[test]
    fn pending_injection_denies_the_batch() {
        let batch = "cpu,owner=alice value=1i\n";
        assert!(matches!(
            admit_ilp_lines(
                &RlsWriteCheck::PendingInjection,
                &lines(batch),
                Some("ts"),
                0,
                1,
                "cpu"
            ),
            Err(crate::Error::Internal { .. })
        ));
    }

    /// Decided against STORED values, not submitted ones — the SQL parser
    /// hands a decimal literal over as a string, normalized to a number
    /// before storage, so the policy must evaluate after conversion.
    #[test]
    fn a_msgpack_row_is_decided_on_its_normalized_values() {
        let filter = ScanFilter {
            field: "reading".into(),
            op: FilterOp::Eq,
            value: Value::Float(1.5),
            clauses: Vec::new(),
            expr: None,
        };
        let policy = RlsWriteCheck::Predicate(
            zerompk::to_msgpack_vec(&vec![filter]).expect("encode policy filter"),
        );

        // What the planner emits for `VALUES (…, 1.5)`: the decimal arrives as
        // a string, and only the ILP conversion recovers its numeric type.
        let payload = nodedb_types::json_to_msgpack_or_empty(&serde_json::json!([{
            "reading": "1.5",
        }]));

        assert!(
            admit_msgpack_rows(&policy, &payload, "cpu", Some("ts"), 0, 1, "cpu").is_ok(),
            "a row whose stored value satisfies the policy must be admitted"
        );
    }

    /// …and a row whose stored value does not satisfy it is still refused.
    #[test]
    fn a_violating_msgpack_row_is_still_refused_after_normalization() {
        let policy = RlsWriteCheck::Predicate(owner_policy("alice"));
        let payload = nodedb_types::json_to_msgpack_or_empty(&serde_json::json!([{
            "owner": "mallory",
            "value": 1,
        }]));
        assert!(
            admit_msgpack_rows(&policy, &payload, "cpu", Some("ts"), 0, 1, "cpu").is_err(),
            "normalization must not turn a violating row into an admitted one"
        );
    }

    /// A payload that does not decode is refused rather than admitted by
    /// omission.
    #[test]
    fn an_undecodable_msgpack_payload_is_refused() {
        assert!(
            admit_msgpack_rows(
                &RlsWriteCheck::Predicate(owner_policy("alice")),
                &[0xC1],
                "cpu",
                Some("ts"),
                0,
                1,
                "cpu"
            )
            .is_err()
        );
    }

    /// The line's own timestamp is bound to the declared time column, in the
    /// milliseconds the row is stored with.
    #[test]
    fn the_time_column_carries_the_line_timestamp_in_milliseconds() {
        let batch = "cpu,owner=alice value=1i 1700000000000000000\n";
        let parsed = lines(batch);
        let image = line_image(&parsed[0], Some("ts"), 7);
        let Value::Object(map) = image else {
            panic!("row image must be an object");
        };
        assert_eq!(map.get("ts"), Some(&Value::Integer(1_700_000_000_000)));
    }

    /// A line with no timestamp is stored with the batch's default, so that is
    /// what the policy decides.
    #[test]
    fn a_line_without_a_timestamp_carries_the_batch_default() {
        let parsed = lines("cpu,owner=alice value=1i\n");
        let image = line_image(&parsed[0], Some("ts"), 7);
        let Value::Object(map) = image else {
            panic!("row image must be an object");
        };
        assert_eq!(map.get("ts"), Some(&Value::Integer(7)));
    }
}
