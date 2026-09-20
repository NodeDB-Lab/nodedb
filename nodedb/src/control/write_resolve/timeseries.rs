// SPDX-License-Identifier: BUSL-1.1

//! Timeseries implementation of [`EngineWriteResolver`].
//!
//! Resolves a governed `TimeseriesOp::Ingest` to the canonical line protocol
//! it would store, then rebuilds it as an `"ilp-msgpack"` ingest. Rows don't
//! exist until rewritten to line protocol, so the resolve pass normalizes,
//! decides, and stamps timestamps before the policy check.

use async_trait::async_trait;
use nodedb_types::RlsWriteCheck;

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::maintenance::clone_materializer::dispatch_local;
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::TimeseriesOp;

use super::resolved_rows::ResolvedRows;
use super::resolver::{EngineWriteResolver, WriteResolveContext};

/// A governed timeseries ingest, extracted at interception.
pub struct TimeseriesWriteResolver {
    /// Routing collection — also the vshard key.
    collection: String,
    /// The intercepted ingest verbatim, live write predicate included.
    op: TimeseriesOp,
}

/// The resolver for `op`, or `None` when it carries no live write predicate.
/// Exhaustive over `TimeseriesOp` — a new op fails to compile here.
pub(super) fn resolver_for_timeseries_op(
    op: &TimeseriesOp,
) -> Option<Box<dyn EngineWriteResolver>> {
    let collection = match op {
        TimeseriesOp::Ingest {
            collection,
            rls_write_check,
            ..
        } => {
            if !rls_write_check.has_predicate() {
                return None;
            }
            collection
        }
        // Read-only: the scan writes nothing, and `ResolveIngest` is the
        // resolve pass itself.
        TimeseriesOp::Scan { .. } | TimeseriesOp::ResolveIngest(_) => return None,
        // Refused at injection under a write policy; carries no predicate.
        TimeseriesOp::Truncate { .. } => return None,
    };
    Some(Box::new(TimeseriesWriteResolver {
        collection: collection.as_str().to_owned(),
        op: op.clone(),
    }))
}

#[async_trait]
impl EngineWriteResolver for TimeseriesWriteResolver {
    fn collection(&self) -> &str {
        &self.collection
    }

    fn build_resolve_op(&self) -> PhysicalPlan {
        PhysicalPlan::Timeseries(TimeseriesOp::ResolveIngest(Box::new(self.op.clone())))
    }

    /// A refused line surfaces as `DataPlane(RejectedAuthz)`, same as a
    /// directly dispatched ingest — the resolve handler runs the same gate.
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
                        "timeseries governed ingest: resolve on '{collection}' returned status \
                         {:?} with no error code",
                        resp.status
                    ),
                },
            });
        }

        let lines: Vec<String> =
            zerompk::from_msgpack(&resp.payload).map_err(|e| crate::Error::Codec {
                detail: format!(
                    "timeseries governed ingest: could not decode resolved lines for \
                     '{collection}': {e}"
                ),
            })?;
        Ok(ResolvedRows::Timeseries { lines })
    }

    fn apply(&self, resolved: ResolvedRows) -> crate::Result<PhysicalPlan> {
        let ResolvedRows::Timeseries { lines } = resolved else {
            return Err(crate::Error::Internal {
                detail: format!(
                    "timeseries write resolver for '{}' was handed another engine's resolution; \
                     resolver_for_plan dispatched the wrong engine",
                    self.collection
                ),
            });
        };
        let payload = zerompk::to_msgpack_vec(&lines).map_err(|e| crate::Error::Codec {
            detail: format!(
                "timeseries governed ingest: could not encode resolved lines for '{}': {e}",
                self.collection
            ),
        })?;
        // Every other field carries over verbatim, unchanged.
        let TimeseriesOp::Ingest {
            collection,
            payload: _,
            format: _,
            wal_lsn,
            surrogates,
            provenance,
            rls_write_check: _,
            returning,
            rls_filters,
        } = self.op.clone()
        else {
            return Err(crate::Error::Internal {
                detail: format!(
                    "timeseries write resolver for '{}' holds a plan that is not an ingest",
                    self.collection
                ),
            });
        };
        Ok(PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection,
            payload,
            format: "ilp-msgpack".to_string(),
            wal_lsn,
            surrogates,
            provenance,
            rls_write_check: RlsWriteCheck::decided_earlier_in_request(),
            returning,
            rls_filters,
        }))
    }
}
