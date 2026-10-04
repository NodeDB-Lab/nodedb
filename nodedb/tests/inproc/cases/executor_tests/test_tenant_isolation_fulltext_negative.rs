// SPDX-License-Identifier: BUSL-1.1

//! Cross-tenant isolation: Full-Text Search engine — negative (write-collision) cases.
//!
//! Verifies that Tenant B indexing documents with the same terms as Tenant A
//! cannot contaminate Tenant A's search results.  After Tenant B's inserts,
//! Tenant A must see the same result count as before.

use nodedb::bridge::envelope::Status;
use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan, TextOp};
use nodedb_types::{DatabaseId, QualifiedCollection};

use super::helpers::*;

/// Tenant B indexing documents with the same search terms must not contaminate
/// Tenant A's full-text search results.
#[test]
fn fulltext_cross_tenant_index_does_not_contaminate_search() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Tenant A indexes 2 documents about "quantum".
    let docs_a = [
        (
            "a1",
            r#"{"title":"quantum computing","body":"qubits and gates"}"#,
        ),
        (
            "a2",
            r#"{"title":"quantum entanglement","body":"spooky action"}"#,
        ),
    ];
    for (id, val) in &docs_a {
        send_ok_as_tenant(
            &mut core,
            &mut tx,
            &mut rx,
            TENANT_A,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "articles"),
                document_id: id.to_string(),
                value: val.as_bytes().to_vec(),
                surrogate: doc_surrogate(id),
                pk_bytes: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            }),
        );
    }

    // Establish baseline count for Tenant A's search.
    let resp_baseline = send_raw_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Text(TextOp::Search {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "articles"),
            query: "quantum".into(),
            top_k: 20,
            mode: nodedb_types::text_search::QueryMode::And,
            fuzzy: false,
            rls_filters: Vec::new(),
            prefilter: None,
            field: None,
            filters: Vec::new(),
            scores: Vec::new(),
        }),
    );
    assert_eq!(resp_baseline.status, Status::Ok);
    let json_baseline = payload_json(&resp_baseline.payload);
    let baseline_count = serde_json::from_str::<serde_json::Value>(&json_baseline)
        .unwrap_or(serde_json::Value::Array(vec![]))
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);

    // Tenant B indexes 5 documents all containing "quantum".
    for i in 0..5u32 {
        let val = format!(r#"{{"title":"quantum doc {i}","body":"quantum research {i}"}}"#);
        send_ok_as_tenant(
            &mut core,
            &mut tx,
            &mut rx,
            TENANT_B,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "articles"),
                document_id: format!("b{i}"),
                value: val.into_bytes(),
                surrogate: doc_surrogate(&format!("b{i}")),
                pk_bytes: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            }),
        );
    }

    // Tenant A's search must return the same count as baseline.
    let resp_after = send_raw_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Text(TextOp::Search {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "articles"),
            query: "quantum".into(),
            top_k: 20,
            mode: nodedb_types::text_search::QueryMode::And,
            fuzzy: false,
            rls_filters: Vec::new(),
            prefilter: None,
            field: None,
            filters: Vec::new(),
            scores: Vec::new(),
        }),
    );
    assert_eq!(resp_after.status, Status::Ok);
    let json_after = payload_json(&resp_after.payload);
    let after_count = serde_json::from_str::<serde_json::Value>(&json_after)
        .unwrap_or(serde_json::Value::Array(vec![]))
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);

    assert_eq!(
        after_count, baseline_count,
        "Tenant B's FTS indexing must not inflate Tenant A's search results \
         (baseline={baseline_count}, after={after_count})"
    );

    // Tenant B's document IDs must not appear in Tenant A's results.
    for i in 0..5u32 {
        assert!(
            !json_after.contains(&format!("\"b{i}\"")),
            "Tenant B's doc b{i} must NOT appear in Tenant A's FTS results; got: {json_after}"
        );
    }
}
