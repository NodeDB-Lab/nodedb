// SPDX-License-Identifier: BUSL-1.1

//! Columnar implementation of [`EngineWriteResolver`].
//!
//! Resolves a governed columnar predicate `UPDATE`/`DELETE` to the concrete
//! rows it would write, then rebuilds it as `ColumnarOp::ResolvedUpdate` /
//! `ResolvedDelete`.

use async_trait::async_trait;
use nodedb_types::{RlsWriteCheck, Value};

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::maintenance::clone_materializer::dispatch_local;
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::ColumnarOp;

use super::resolved_rows::ResolvedRows;
use super::resolver::{EngineWriteResolver, WriteResolveContext};

/// A governed columnar predicate `UPDATE`/`DELETE`, extracted at interception.
pub struct ColumnarWriteResolver {
    collection: String,
    /// Serialized `Vec<ScanFilter>` — the statement's `WHERE` clause.
    filters: Vec<u8>,
    /// Field assignments for an `UPDATE`. Empty for a `DELETE`.
    updates: Vec<(String, Vec<u8>)>,
    is_update: bool,
    rls_write_check: RlsWriteCheck,
}

/// The resolver for `op`, or `None` when it carries no live write predicate.
/// Exhaustive over `ColumnarOp` — a new op fails to compile here.
pub(super) fn resolver_for_columnar_op(op: &ColumnarOp) -> Option<Box<dyn EngineWriteResolver>> {
    let (collection, filters, updates, is_update, rls_write_check) = match op {
        ColumnarOp::Update {
            collection,
            filters,
            updates,
            rls_write_check,
        } => (collection, filters, Some(updates), true, rls_write_check),
        ColumnarOp::Delete {
            collection,
            filters,
            rls_write_check,
        } => (collection, filters, None, false, rls_write_check),
        ColumnarOp::Insert { .. }
        | ColumnarOp::Scan { .. }
        | ColumnarOp::ResolvedUpdate { .. }
        | ColumnarOp::ResolvedDelete { .. }
        | ColumnarOp::ResolveDml { .. }
        | ColumnarOp::MaterializeScan { .. }
        // Refused at injection under a write policy; carries no predicate.
        | ColumnarOp::Truncate { .. } => return None,
    };
    if !rls_write_check.has_predicate() {
        return None;
    }
    Some(Box::new(ColumnarWriteResolver {
        collection: collection.as_str().to_owned(),
        filters: filters.clone(),
        updates: updates.cloned().unwrap_or_default(),
        is_update,
        rls_write_check: rls_write_check.clone(),
    }))
}

#[async_trait]
impl EngineWriteResolver for ColumnarWriteResolver {
    fn collection(&self) -> &str {
        &self.collection
    }

    fn build_resolve_op(&self) -> PhysicalPlan {
        PhysicalPlan::Columnar(ColumnarOp::ResolveDml {
            collection: nodedb_types::QualifiedCollection::from_stored(self.collection.clone()),
            filters: self.filters.clone(),
            updates: self.updates.clone(),
            is_update: self.is_update,
            rls_write_check: self.rls_write_check.clone(),
        })
    }

    /// A refused row surfaces as `DataPlane(RejectedAuthz)`, same as a direct
    /// predicate `UPDATE`/`DELETE` — both go through `admit_columnar_row`.
    async fn resolve(
        &self,
        state: &SharedState,
        ctx: WriteResolveContext,
        op: PhysicalPlan,
    ) -> crate::Result<ResolvedRows> {
        let collection = &self.collection;
        let resp =
            dispatch_local(state, ctx.tenant_id, ctx.database_id, collection, op, None).await?;
        if resp.status != Status::Ok {
            return Err(match resp.error_code {
                Some(code) => crate::Error::DataPlane(*code),
                None => crate::Error::Dispatch {
                    detail: format!(
                        "columnar predicate DML: resolve on '{collection}' returned status {:?} \
                         with no error code",
                        resp.status
                    ),
                },
            });
        }

        if self.is_update {
            let rows: Vec<(Value, Vec<Value>)> =
                zerompk::from_msgpack(&resp.payload).map_err(|e| crate::Error::Codec {
                    detail: format!(
                        "columnar predicate DML: could not decode resolved rows for \
                         '{collection}': {e}"
                    ),
                })?;
            Ok(ResolvedRows::Update(rows))
        } else {
            let pks: Vec<Value> =
                zerompk::from_msgpack(&resp.payload).map_err(|e| crate::Error::Codec {
                    detail: format!(
                        "columnar predicate DML: could not decode resolved pks for \
                         '{collection}': {e}"
                    ),
                })?;
            Ok(ResolvedRows::Delete(pks))
        }
    }

    fn apply(&self, resolved: ResolvedRows) -> crate::Result<PhysicalPlan> {
        Ok(PhysicalPlan::Columnar(match resolved {
            ResolvedRows::Update(rows) => ColumnarOp::ResolvedUpdate {
                collection: nodedb_types::QualifiedCollection::from_stored(self.collection.clone()),
                rows,
                rls_write_check: RlsWriteCheck::decided_earlier_in_request(),
            },
            ResolvedRows::Delete(pks) => ColumnarOp::ResolvedDelete {
                collection: nodedb_types::QualifiedCollection::from_stored(self.collection.clone()),
                pks,
                rls_write_check: RlsWriteCheck::decided_earlier_in_request(),
            },
            ResolvedRows::Kv { .. }
            | ResolvedRows::Document { .. }
            | ResolvedRows::Vector { .. }
            | ResolvedRows::Timeseries { .. }
            | ResolvedRows::GraphEdgeDeleteAdmitted => {
                return Err(crate::Error::Internal {
                    detail: format!(
                        "columnar write resolver for '{}' was handed another engine's \
                         resolution; resolver_for_plan dispatched the wrong engine",
                        self.collection
                    ),
                });
            }
        }))
    }
}
