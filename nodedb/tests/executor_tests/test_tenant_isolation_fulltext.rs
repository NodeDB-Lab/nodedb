//! Cross-tenant isolation: Full-Text Search engine.
//!
//! Tenant A indexes documents with text. Tenant B searches — must get zero results.

use nodedb::bridge::envelope::{PhysicalPlan, Status};
use nodedb::bridge::physical_plan::{DocumentOp, TextOp};

use crate::helpers::*;

const TENANT_A: u32 = 10;
const TENANT_B: u32 = 20;

#[test]
fn fulltext_search_isolated() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Tenant A inserts documents with searchable text.
    let docs = [
        (
            "d1",
            r#"{"title":"quantum computing basics","body":"An introduction to qubits and gates"}"#,
        ),
        (
            "d2",
            r#"{"title":"neural network primer","body":"Deep learning fundamentals explained"}"#,
        ),
        (
            "d3",
            r#"{"title":"quantum entanglement","body":"Spooky action at a distance explored"}"#,
        ),
    ];
    for (id, val) in &docs {
        send_ok_as_tenant(
            &mut core,
            &mut tx,
            &mut rx,
            TENANT_A,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: "articles".into(),
                document_id: id.to_string(),
                value: val.as_bytes().to_vec(),
                surrogate: nodedb_types::Surrogate::ZERO,
                pk_bytes: Vec::new(),
            }),
        );
    }

    // Tenant A can full-text search.
    let resp_a = send_raw_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_A,
        PhysicalPlan::Text(TextOp::Search {
            collection: "articles".into(),
            query: "quantum".into(),
            top_k: 5,
            fuzzy: false,
            rls_filters: Vec::new(),
        }),
    );
    assert_eq!(resp_a.status, Status::Ok);

    // Tenant B searches the same collection — must get zero results.
    let resp_b = send_raw_as_tenant(
        &mut core,
        &mut tx,
        &mut rx,
        TENANT_B,
        PhysicalPlan::Text(TextOp::Search {
            collection: "articles".into(),
            query: "quantum".into(),
            top_k: 5,
            fuzzy: false,
            rls_filters: Vec::new(),
        }),
    );
    assert_eq!(resp_b.status, Status::Ok);
    let json_b = payload_json(&resp_b.payload);
    let val: serde_json::Value =
        serde_json::from_str(&json_b).unwrap_or(serde_json::Value::Array(vec![]));
    let empty = vec![];
    let arr = val.as_array().unwrap_or(&empty);
    assert!(
        arr.is_empty(),
        "Tenant B fulltext search must return 0 results, got {}: {json_b}",
        arr.len()
    );
}
