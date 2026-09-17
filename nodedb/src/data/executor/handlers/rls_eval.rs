// SPDX-License-Identifier: BUSL-1.1

//! Data Plane RLS filter evaluation for post-fetch / post-candidate operations.
//!
//! The Control Plane injects serialized `ScanFilter` bytes into physical plan
//! `rls_filters` fields. This module deserializes and evaluates those filters
//! against documents fetched by the Data Plane.
//!
//! **Security contract**: If `rls_filters` is non-empty and the document does
//! not pass all filters, the handler MUST return `NOT_FOUND` (no info leak).
//! Empty `rls_filters` means no RLS policies apply — allow unconditionally.

use nodedb_types::Value;

use crate::bridge::scan_filter::ScanFilter;

/// Evaluate RLS filters against a JSON document.
///
/// Returns `true` if the document passes all RLS filters (or if no filters).
/// Returns `false` if any filter rejects the document (caller must deny).
///
/// Used by the write gate over the JSON post-images the update paths build.
pub fn rls_check_document(rls_filters: &[u8], doc: &serde_json::Value) -> bool {
    if rls_filters.is_empty() {
        return true;
    }
    check_encoded(rls_filters, &nodedb_types::json_to_msgpack_or_empty(doc))
}

/// Evaluate RLS filters against a typed `Value` document.
///
/// Same contract as [`rls_check_document`]. A document that does not encode
/// is denied: a row the policy could not be evaluated against is not a row
/// the policy admitted.
pub fn rls_check_value(rls_filters: &[u8], doc: &Value) -> bool {
    if rls_filters.is_empty() {
        return true;
    }
    match nodedb_types::value_to_msgpack(doc) {
        Ok(msgpack) => check_encoded(rls_filters, &msgpack),
        Err(e) => {
            tracing::warn!(error = %e, "RLS document encode failed — denying access");
            false
        }
    }
}

/// Evaluate RLS filters against raw MessagePack document bytes.
///
/// Evaluates filters directly on msgpack bytes — no decode to serde_json::Value.
/// Returns `true` if passes. Returns `false` if any filter rejects.
pub fn rls_check_msgpack_bytes(rls_filters: &[u8], doc_bytes: &[u8]) -> bool {
    if rls_filters.is_empty() {
        return true;
    }
    // Ensure bytes are standard msgpack for matches_binary.
    let mp = super::super::doc_format::json_to_msgpack(doc_bytes);
    check_encoded(rls_filters, &mp)
}

/// Decode the compiled filters and match them against one standard msgpack
/// document. RLS is a security boundary, so it fails closed: an undecodable
/// filter payload or a division/modulo-by-zero inside a filter denies rather
/// than propagating a query error, so a malformed or adversarial predicate
/// can never distinguish "row exists but errors" from "row doesn't exist".
fn check_encoded(rls_filters: &[u8], msgpack: &[u8]) -> bool {
    let filters: Vec<ScanFilter> = match zerompk::from_msgpack(rls_filters) {
        Ok(f) => f,
        Err(_) => {
            tracing::warn!("RLS filter deserialization failed — denying access");
            return false;
        }
    };
    match ScanFilter::all_match_binary(&filters, msgpack) {
        Ok(pass) => pass,
        Err(e) => {
            tracing::warn!(error = %e, "RLS filter evaluation failed — denying access");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::scan_filter::FilterOp;
    use serde_json::json;

    fn make_rls_bytes(field: &str, op: FilterOp, value: nodedb_types::Value) -> Vec<u8> {
        let filter = ScanFilter {
            field: field.into(),
            op,
            value,
            clauses: Vec::new(),
            expr: None,
        };
        zerompk::to_msgpack_vec(&vec![filter]).unwrap()
    }

    #[test]
    fn empty_filters_allow() {
        let doc = json!({"anything": "goes"});
        assert!(rls_check_document(&[], &doc));
    }

    #[test]
    fn matching_filter_allows() {
        let rls = make_rls_bytes(
            "user_id",
            FilterOp::Eq,
            nodedb_types::Value::String("42".into()),
        );
        let doc = json!({"user_id": "42", "name": "alice"});
        assert!(rls_check_document(&rls, &doc));
    }

    #[test]
    fn non_matching_filter_denies() {
        let rls = make_rls_bytes(
            "user_id",
            FilterOp::Eq,
            nodedb_types::Value::String("42".into()),
        );
        let doc = json!({"user_id": "99", "name": "bob"});
        assert!(!rls_check_document(&rls, &doc));
    }

    #[test]
    fn missing_field_denies() {
        let rls = make_rls_bytes(
            "user_id",
            FilterOp::Eq,
            nodedb_types::Value::String("42".into()),
        );
        let doc = json!({"name": "alice"});
        assert!(!rls_check_document(&rls, &doc));
    }

    #[test]
    fn typed_documents_evaluate_the_same_filters() {
        let rls = make_rls_bytes(
            "user_id",
            FilterOp::Eq,
            nodedb_types::Value::String("42".into()),
        );
        let ok = Value::from(json!({"user_id": "42"}));
        let bad = Value::from(json!({"user_id": "99"}));
        assert!(rls_check_value(&rls, &ok));
        assert!(!rls_check_value(&rls, &bad));
        assert!(rls_check_value(&[], &bad));
    }

    #[test]
    fn corrupt_filters_deny() {
        let corrupt = vec![0xFF, 0xFE, 0xFD];
        let doc = json!({"user_id": "42"});
        assert!(!rls_check_document(&corrupt, &doc));
    }

    #[test]
    fn multiple_filters_all_must_pass() {
        let filters = vec![
            ScanFilter {
                field: "user_id".into(),
                op: crate::bridge::scan_filter::FilterOp::Eq,
                value: nodedb_types::Value::String("42".into()),
                clauses: Vec::new(),
                expr: None,
            },
            ScanFilter {
                field: "status".into(),
                op: crate::bridge::scan_filter::FilterOp::Eq,
                value: nodedb_types::Value::String("active".into()),
                clauses: Vec::new(),
                expr: None,
            },
        ];
        let rls = zerompk::to_msgpack_vec(&filters).unwrap();

        let doc_ok = json!({"user_id": "42", "status": "active"});
        assert!(rls_check_document(&rls, &doc_ok));

        let doc_bad = json!({"user_id": "42", "status": "banned"});
        assert!(!rls_check_document(&rls, &doc_bad));
    }
}
