// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `SELECT ESTIMATE_COUNT('collection', 'field')` function.
//!
//! Dispatches [`DocumentOp::EstimateCount`] to the Data Plane for a fast
//! approximate count derived from HLL statistics and returns a single text row.
//! The handler builds [`DdlResult`] directly and carries no pgwire types.

use serde_json::{Map, Value as JsonValue};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TraceId};
use nodedb_physical::physical_plan::DocumentOp;

use super::super::result::{DdlError, DdlResult};
use super::read_gate::CollectionReadGate;

/// Execute `SELECT ESTIMATE_COUNT('collection', 'field')`.
pub async fn estimate_count(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    // Parse: SELECT ESTIMATE_COUNT('collection', 'field')
    let inner = sql
        .find('(')
        .and_then(|start| sql.rfind(')').map(|end| &sql[start + 1..end]));
    if let Some(args_str) = inner {
        let args: Vec<&str> = args_str
            .split(',')
            .map(|s| s.trim().trim_matches('\''))
            .collect();
        if args.len() >= 2 {
            // A string literal is data: the collection name resolves exactly as
            // written, with no case folding.
            let coll = args[0].to_string();
            let field = args[1].to_string();
            let tenant_id = identity.tenant_id;

            // `coll` and `field` are caller arguments. `EstimateCount` answers
            // from HLL statistics over every row and carries no filter slot, so
            // a read policy cannot be honored and an estimate over rows the
            // caller may not read is refused instead. A redacted column is
            // refused on the same grounds: the cardinality of a masked column is
            // not the cardinality being reported.
            let gate = CollectionReadGate::open(state, identity, database_id, &coll)?;
            gate.refuse_if_read_policy(&coll, "ESTIMATE_COUNT")?;
            gate.refuse_if_field_redacted(&coll, &field, "the estimated count")?;

            let vshard = crate::types::VShardId::from_collection_in_database(database_id, &coll);
            let plan = PhysicalPlan::Document(DocumentOp::EstimateCount {
                collection: nodedb_types::QualifiedCollection::new(database_id, &coll),
                field,
            });
            match crate::control::server::dispatch_utils::dispatch_to_data_plane(
                state,
                tenant_id,
                database_id,
                vshard,
                plan,
                TraceId::ZERO,
            )
            .await
            {
                Ok(resp) => {
                    let payload_text =
                        crate::data::executor::response_codec::decode_payload_to_json(
                            &resp.payload,
                        );
                    let columns = vec!["estimate_count".to_string()];
                    let mut row = Map::new();
                    row.insert(
                        "estimate_count".to_string(),
                        JsonValue::String(payload_text),
                    );
                    return Ok(vec![DdlResult::Rows(ShapedRows::text_rows(
                        columns,
                        vec![row],
                    ))]);
                }
                Err(e) => {
                    return Err(DdlError::new("XX000", e.to_string()));
                }
            }
        }
    }
    Err(DdlError::new(
        "42601",
        "usage: SELECT ESTIMATE_COUNT('collection', 'field')",
    ))
}
