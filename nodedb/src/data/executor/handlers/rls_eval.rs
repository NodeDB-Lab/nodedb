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
pub fn rls_check_document(
    rls_filters: &[u8],
    doc: &serde_json::Value,
    crs: &nodedb_crs::CrsContext<'_>,
) -> bool {
    if rls_filters.is_empty() {
        return true;
    }
    check_value(rls_filters, &Value::from(doc.clone()), crs)
}

/// Evaluate RLS filters against a typed `Value` document.
///
/// Same contract as [`rls_check_document`]. The policy reads native fields directly.
pub fn rls_check_value(rls_filters: &[u8], doc: &Value, crs: &nodedb_crs::CrsContext<'_>) -> bool {
    if rls_filters.is_empty() {
        return true;
    }
    check_value(rls_filters, doc, crs)
}

/// Evaluate RLS filters against raw MessagePack document bytes.
///
/// Decode the native document once, then evaluate its filters.
/// Return `false` when decoding or evaluation fails.
pub fn rls_check_msgpack_bytes(
    rls_filters: &[u8],
    doc_bytes: &[u8],
    crs: &nodedb_crs::CrsContext<'_>,
) -> bool {
    if rls_filters.is_empty() {
        return true;
    }
    match crate::data::executor::doc_format::decode_document_value(doc_bytes) {
        Ok(doc) => check_value(rls_filters, &doc, crs),
        Err(error) => {
            tracing::warn!(%error, "RLS document decode failed — denying access");
            false
        }
    }
}

/// Decode the compiled filters and match them against one native
/// document. RLS is a security boundary, so it fails closed: an undecodable
/// filter payload or a division/modulo-by-zero inside a filter denies rather
/// than propagating a query error, so a malformed or adversarial predicate
/// can never distinguish "row exists but errors" from "row doesn't exist".
fn check_value(rls_filters: &[u8], doc: &Value, crs: &nodedb_crs::CrsContext<'_>) -> bool {
    let filters: Vec<ScanFilter> = match zerompk::from_msgpack(rls_filters) {
        Ok(f) => f,
        Err(_) => {
            tracing::warn!("RLS filter deserialization failed — denying access");
            return false;
        }
    };
    match ScanFilter::all_match_value(&filters, doc, crs) {
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
        assert!(rls_check_document(
            &[],
            &doc,
            &nodedb_crs::CrsContext::builtin_only()
        ));
    }

    #[test]
    fn matching_filter_allows() {
        let rls = make_rls_bytes(
            "user_id",
            FilterOp::Eq,
            nodedb_types::Value::String("42".into()),
        );
        let doc = json!({"user_id": "42", "name": "alice"});
        assert!(rls_check_document(
            &rls,
            &doc,
            &nodedb_crs::CrsContext::builtin_only()
        ));
    }

    #[test]
    fn non_matching_filter_denies() {
        let rls = make_rls_bytes(
            "user_id",
            FilterOp::Eq,
            nodedb_types::Value::String("42".into()),
        );
        let doc = json!({"user_id": "99", "name": "bob"});
        assert!(!rls_check_document(
            &rls,
            &doc,
            &nodedb_crs::CrsContext::builtin_only()
        ));
    }

    #[test]
    fn missing_field_denies() {
        let rls = make_rls_bytes(
            "user_id",
            FilterOp::Eq,
            nodedb_types::Value::String("42".into()),
        );
        let doc = json!({"name": "alice"});
        assert!(!rls_check_document(
            &rls,
            &doc,
            &nodedb_crs::CrsContext::builtin_only()
        ));
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
        assert!(rls_check_value(
            &rls,
            &ok,
            &nodedb_crs::CrsContext::builtin_only()
        ));
        assert!(!rls_check_value(
            &rls,
            &bad,
            &nodedb_crs::CrsContext::builtin_only()
        ));
        assert!(rls_check_value(
            &[],
            &bad,
            &nodedb_crs::CrsContext::builtin_only()
        ));
    }

    #[test]
    fn corrupt_filters_deny() {
        let corrupt = vec![0xFF, 0xFE, 0xFD];
        let doc = json!({"user_id": "42"});
        assert!(!rls_check_document(
            &corrupt,
            &doc,
            &nodedb_crs::CrsContext::builtin_only()
        ));
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
        assert!(rls_check_document(
            &rls,
            &doc_ok,
            &nodedb_crs::CrsContext::builtin_only()
        ));

        let doc_bad = json!({"user_id": "42", "status": "banned"});
        assert!(!rls_check_document(
            &rls,
            &doc_bad,
            &nodedb_crs::CrsContext::builtin_only()
        ));
    }
    #[test]
    fn native_read_policy_keeps_exact_decimal_fields() {
        let amount = Value::Decimal("9007199254740993.0001".parse().expect("decimal"));
        let filters = make_rls_bytes("amount", FilterOp::Eq, amount.clone());
        let doc = Value::Object([("amount".into(), amount)].into_iter().collect());
        let bytes = zerompk::to_msgpack_vec(&doc).expect("native body");
        let crs = nodedb_crs::CrsContext::builtin_only();
        assert!(rls_check_value(&filters, &doc, &crs));
        assert!(rls_check_msgpack_bytes(&filters, &bytes, &crs));
        let different = Value::Object(
            [(
                "amount".into(),
                Value::Decimal("9007199254740993.0002".parse().expect("decimal")),
            )]
            .into_iter()
            .collect(),
        );
        assert!(!rls_check_value(&filters, &different, &crs));
        assert!(!rls_check_msgpack_bytes(&filters, &[0x92, 0x01], &crs));
        assert!(!rls_check_value(&[0xff], &doc, &crs));
    }
}
