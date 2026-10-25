// SPDX-License-Identifier: BUSL-1.1

use loro::LoroValue;

use crate::engine::crdt::tenant_state::TenantCrdtEngine;

/// CRDT-backed Document Engine.
///
/// Documents in CRDT-enabled collections are stored as Loro documents instead
/// of raw MessagePack. This enables offline-first editing, conflict-free merging,
/// and full version history at the cost of higher storage overhead.
///
/// Standard documents use simple last-writer-wins semantics (see `DocumentEngine`).
/// This variant uses Loro CRDTs for collaborative workloads.
pub struct CrdtDocumentEngine<'a> {
    crdt: &'a mut TenantCrdtEngine,
}

impl<'a> CrdtDocumentEngine<'a> {
    pub fn new(crdt: &'a mut TenantCrdtEngine) -> Self {
        Self { crdt }
    }

    /// Read a CRDT-backed document as JSON.
    ///
    /// Returns the current converged state by reading the row's deep value
    /// from the Loro document and converting it to `serde_json::Value`.
    pub fn get(&self, collection: &str, doc_id: &str) -> Option<serde_json::Value> {
        let loro_val = self.crdt.read_row(collection, doc_id)?;
        Some(loro_value_to_json(&loro_val))
    }

    /// Check if a document exists in the CRDT store.
    pub fn exists(&self, collection: &str, doc_id: &str) -> bool {
        self.crdt.row_exists(collection, doc_id)
    }
}

/// Convert a `LoroValue` to `serde_json::Value`.
pub fn loro_value_to_json(val: &LoroValue) -> serde_json::Value {
    match val {
        LoroValue::Null => serde_json::Value::Null,
        LoroValue::Bool(b) => serde_json::Value::Bool(*b),
        LoroValue::I64(n) => serde_json::json!(*n),
        LoroValue::Double(f) => serde_json::json!(*f),
        LoroValue::String(s) => serde_json::Value::String(s.to_string()),
        LoroValue::Binary(b) => {
            serde_json::Value::Array(b.iter().map(|byte| serde_json::json!(*byte)).collect())
        }
        LoroValue::List(list) => {
            serde_json::Value::Array(list.iter().map(loro_value_to_json).collect())
        }
        LoroValue::Map(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .map(|(k, v)| (k.clone(), loro_value_to_json(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
        LoroValue::Container(_) => serde_json::Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::crdt::tenant_state::TenantCrdtEngine;
    use crate::types::TenantId;
    use nodedb_crdt::constraint::ConstraintSet;
    use nodedb_crdt::validator::ProposedChange;

    fn make_crdt_engine() -> TenantCrdtEngine {
        TenantCrdtEngine::new(TenantId::new(1), 1, ConstraintSet::new()).unwrap()
    }

    #[test]
    fn empty_get_returns_none() {
        let mut crdt = make_crdt_engine();
        let doc_engine = CrdtDocumentEngine::new(&mut crdt);
        assert!(doc_engine.get("col", "missing").is_none());
    }

    #[test]
    fn exists_returns_false_for_missing() {
        let mut crdt = make_crdt_engine();
        let doc_engine = CrdtDocumentEngine::new(&mut crdt);
        assert!(!doc_engine.exists("col", "missing"));
    }

    #[test]
    fn get_returns_actual_document_fields() {
        let mut crdt = make_crdt_engine();

        let change = ProposedChange {
            collection: "users".into(),
            row_id: "u1".into(),
            surrogate: nodedb_types::Surrogate::new(1),
            fields: vec![
                ("name".into(), LoroValue::String("Alice".into())),
                ("age".into(), LoroValue::I64(30)),
            ],
        };
        crdt.validate_and_apply(
            1,
            nodedb_crdt::CrdtAuthContext::default(),
            &change,
            b"delta".to_vec(),
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .unwrap();

        let doc_engine = CrdtDocumentEngine::new(&mut crdt);
        let doc = doc_engine.get("users", "u1").expect("should exist");

        assert_eq!(doc["name"], "Alice");
        assert_eq!(doc["age"], 30);
    }

    #[test]
    fn get_reflects_latest_state_after_update() {
        let mut crdt = make_crdt_engine();

        let change1 = ProposedChange {
            collection: "users".into(),
            row_id: "u1".into(),
            surrogate: nodedb_types::Surrogate::new(1),
            fields: vec![("name".into(), LoroValue::String("Alice".into()))],
        };
        crdt.validate_and_apply(
            1,
            nodedb_crdt::CrdtAuthContext::default(),
            &change1,
            b"d1".to_vec(),
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .unwrap();

        let change2 = ProposedChange {
            collection: "users".into(),
            row_id: "u1".into(),
            surrogate: nodedb_types::Surrogate::new(1),
            fields: vec![("name".into(), LoroValue::String("Bob".into()))],
        };
        crdt.validate_and_apply(
            1,
            nodedb_crdt::CrdtAuthContext::default(),
            &change2,
            b"d2".to_vec(),
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .unwrap();

        let doc_engine = CrdtDocumentEngine::new(&mut crdt);
        let doc = doc_engine.get("users", "u1").expect("should exist");
        assert_eq!(doc["name"], "Bob");
    }
}
