// SPDX-License-Identifier: BUSL-1.1

//! `SELECT VERIFY_HASH_CHAIN('collection')`
//!
//! Dispatches `MetaOp::VerifyHashChain` to the core that owns the collection.
//! The Data Plane walks the chain over the raw stored rows. This handler only
//! authorizes the call and shapes the verdict as
//! `{valid, entries, last_hash, broken_at, document_id, expected, found}`.

use nodedb_sql::parser::query_function::VerifyHashChainArgs;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::dispatch_utils;
use crate::control::state::SharedState;
use crate::types::hash_chain::ChainVerdict;
use crate::types::{DatabaseId, TraceId};

use super::super::super::result::{DdlError, DdlResult};
use super::super::read_gate::CollectionReadGate;
use super::helpers::{err, single_result};

pub async fn verify_hash_chain(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    args: &VerifyHashChainArgs,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id;
    // A string-literal argument names the collection verbatim, as in every
    // other query function.
    let collection = args.collection.clone();
    if collection.is_empty() {
        return Err(err("42601", "VERIFY_HASH_CHAIN requires (collection)"));
    }

    // Each link covers its whole stored row, so a caller that cannot read
    // every row of the collection is refused rather than handed a verdict
    // over rows it cannot see.
    let gate = CollectionReadGate::open(state, identity, database_id, &collection)?;
    gate.require_document_engine(&collection, "VERIFY_HASH_CHAIN")?;
    gate.refuse_if_any_redaction(&collection, "the hash chain")?;

    let vshard = nodedb_types::CollectionKey::from_bare(database_id, &collection).vshard();
    let mut plan = PhysicalPlan::Meta(nodedb_physical::physical_plan::MetaOp::VerifyHashChain {
        collection: nodedb_types::QualifiedCollection::new(database_id, &collection),
    });
    gate.inject_rls(&mut plan)?;

    let response = dispatch_utils::dispatch_to_data_plane(
        state,
        tenant_id,
        database_id,
        vshard,
        plan,
        TraceId::ZERO,
    )
    .await
    .map_err(|e| DdlError::from_error_in_context("hash-chain verification failed", &e))?;

    let verdict: ChainVerdict = zerompk::from_msgpack(response.payload.as_ref())
        .map_err(|e| DdlError::internal(format!("hash-chain verdict does not decode: {e}")))?;
    Ok(single_result(&verdict_json(&verdict).to_string()))
}

/// The verdict as the JSON the API boundary returns.
fn verdict_json(verdict: &ChainVerdict) -> serde_json::Value {
    let brk = verdict.broken.as_ref();
    serde_json::json!({
        "valid": brk.is_none(),
        "entries": verdict.entries,
        "last_hash": verdict.last_hash,
        "broken_at": brk.map(|b| b.index),
        "document_id": brk.and_then(|b| b.document_id.clone()),
        "expected": brk.and_then(|b| b.expected.clone()),
        "found": brk.and_then(|b| b.found.clone()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::hash_chain::ChainBreak;

    #[test]
    fn an_intact_verdict_is_valid_with_no_break_fields() {
        let json = verdict_json(&ChainVerdict {
            entries: 3,
            last_hash: "ab".into(),
            broken: None,
        });
        assert_eq!(json["valid"], true);
        assert_eq!(json["entries"], 3);
        assert!(json["broken_at"].is_null());
    }

    #[test]
    fn a_break_names_its_position_row_and_links() {
        let json = verdict_json(&ChainVerdict {
            entries: 1,
            last_hash: "h1".into(),
            broken: Some(ChainBreak {
                index: 1,
                document_id: Some("00000002".into()),
                expected: Some("e".into()),
                found: Some("f".into()),
            }),
        });
        assert_eq!(json["valid"], false);
        assert_eq!(json["broken_at"], 1);
        assert_eq!(json["document_id"], "00000002");
        assert_eq!(json["expected"], "e");
        assert_eq!(json["found"], "f");
    }
}
