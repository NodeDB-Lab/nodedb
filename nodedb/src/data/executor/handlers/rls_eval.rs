//! Data Plane RLS filter evaluation for post-fetch / post-candidate operations.
//!
//! The Control Plane injects serialized `ScanFilter` bytes into physical plan
//! `rls_filters` fields. This module deserializes and evaluates those filters
//! against documents fetched by the Data Plane.
//!
//! **Security contract**: If `rls_filters` is non-empty and the document does
//! not pass all filters, the handler MUST return `NOT_FOUND` (no info leak).
//! Empty `rls_filters` means no RLS policies apply — allow unconditionally.

use crate::bridge::scan_filter::ScanFilter;

/// Evaluate RLS filters against a document.
///
/// Returns `true` if the document passes all RLS filters (or if no filters).
/// Returns `false` if any filter rejects the document (caller must deny).
///
/// Used by point-get and key-get handlers after fetching the raw document.
pub fn rls_check_document(rls_filters: &[u8], doc: &serde_json::Value) -> bool {
    if rls_filters.is_empty() {
        return true;
    }

    let filters: Vec<ScanFilter> = match zerompk::from_msgpack(rls_filters) {
        Ok(f) => f,
        Err(_) => {
            // Deserialization failure → deny (fail-closed).
            tracing::warn!("RLS filter deserialization failed — denying access");
            return false;
        }
    };

    let msgpack = nodedb_types::json_to_msgpack_or_empty(doc);
    filters.iter().all(|f| f.matches_binary(&msgpack))
}

/// Evaluate RLS filters against raw MessagePack document bytes.
///
/// Evaluates filters directly on msgpack bytes — no decode to serde_json::Value.
/// Returns `true` if passes. Returns `false` if any filter rejects.
pub fn rls_check_msgpack_bytes(rls_filters: &[u8], doc_bytes: &[u8]) -> bool {
    if rls_filters.is_empty() {
        return true;
    }

    let filters: Vec<ScanFilter> = match zerompk::from_msgpack(rls_filters) {
        Ok(f) => f,
        Err(_) => {
            tracing::warn!("RLS filter deserialization failed — denying access");
            return false;
        }
    };

    // Ensure bytes are standard msgpack for matches_binary.
    let mp = super::super::doc_format::json_to_msgpack(doc_bytes);
    filters.iter().all(|f| f.matches_binary(&mp))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_rls_bytes(field: &str, op: &str, value: nodedb_types::Value) -> Vec<u8> {
        let filter = ScanFilter {
            field: field.into(),
            op: op.into(),
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
        let rls = make_rls_bytes("user_id", "eq", nodedb_types::Value::String("42".into()));
        let doc = json!({"user_id": "42", "name": "alice"});
        assert!(rls_check_document(&rls, &doc));
    }

    #[test]
    fn non_matching_filter_denies() {
        let rls = make_rls_bytes("user_id", "eq", nodedb_types::Value::String("42".into()));
        let doc = json!({"user_id": "99", "name": "bob"});
        assert!(!rls_check_document(&rls, &doc));
    }

    #[test]
    fn missing_field_denies() {
        let rls = make_rls_bytes("user_id", "eq", nodedb_types::Value::String("42".into()));
        let doc = json!({"name": "alice"});
        assert!(!rls_check_document(&rls, &doc));
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
                op: "eq".into(),
                value: nodedb_types::Value::String("42".into()),
                clauses: Vec::new(),
                expr: None,
            },
            ScanFilter {
                field: "status".into(),
                op: "eq".into(),
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
