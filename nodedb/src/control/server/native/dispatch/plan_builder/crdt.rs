//! CRDT plan builders.

use nodedb_types::protocol::TextFields;
use sonic_rs;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::CrdtOp;
use crate::control::state::SharedState;

use super::require_doc_id;

pub(crate) fn build_read(fields: &TextFields, collection: &str) -> crate::Result<PhysicalPlan> {
    let document_id = require_doc_id(fields)?;
    Ok(PhysicalPlan::Crdt(CrdtOp::Read {
        collection: collection.to_string(),
        document_id,
    }))
}

pub(crate) fn build_apply(
    state: &SharedState,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let document_id = require_doc_id(fields)?;
    let delta = fields
        .delta
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'delta'".to_string(),
        })?
        .clone();
    let peer_id = fields.peer_id.unwrap_or(0);

    // Use provided mutation_id, or generate deterministic one from content hash.
    let mutation_id = fields.mutation_id.unwrap_or_else(|| {
        // Deterministic dedup key from (peer_id, delta).
        let mut combined = peer_id.to_le_bytes().to_vec();
        combined.extend_from_slice(&delta);
        crate::util::fnv1a_hash(&combined)
    });

    let surrogate = state
        .surrogate_assigner
        .assign(collection, document_id.as_bytes())?;

    Ok(PhysicalPlan::Crdt(CrdtOp::Apply {
        collection: collection.to_string(),
        document_id,
        delta,
        peer_id,
        mutation_id,
        surrogate,
    }))
}

pub(crate) fn build_alter_policy(
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let policy = fields
        .policy
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'policy'".to_string(),
        })?;
    let policy_json = sonic_rs::to_string(policy).map_err(|e| crate::Error::BadRequest {
        detail: format!("invalid policy: {e}"),
    })?;
    Ok(PhysicalPlan::Crdt(CrdtOp::SetPolicy {
        collection: collection.to_string(),
        policy_json,
    }))
}
