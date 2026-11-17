// SPDX-License-Identifier: BUSL-1.1

//! Drive one full-text rebuild step until every node it covers answers
//! `Installed`.
//!
//! A node's rebuild runs off its Data Plane core. Each `TextOp::RebuildIndex`
//! answers at once with the rebuild's progress, so the driver sends the op
//! again every [`POLL_INTERVAL`]. The wait is bounded by liveness, not by
//! the size of the collection: a node whose walk count moves keeps the step
//! alive. The count moves for every item the walk visits, replayed or
//! skipped. A node whose count stays still for [`STALL_LIMIT`] fails the
//! step, and so does a node that answers an error or does not answer one
//! call within [`CALL_DEADLINE`].
//!
//! Each poll reads the collection's declared fields from the catalog
//! again. Two statements that change the fields of one collection at once
//! then converge on the newest field set and share one rebuild per node:
//! the op is idempotent under its fields.

use std::time::{Duration, Instant};

use nodedb_physical::physical_plan::{TextOp, TextRebuildProgress};

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId};

use super::super::super::super::result::DdlError;
use super::super::index_fanout::{PeerCall, active_peers};

/// How often the driver asks a node where its rebuild stands.
const POLL_INTERVAL: Duration = Duration::from_millis(200);

/// How long a node's replayed count can stay still before the step fails.
const STALL_LIMIT: Duration = Duration::from_secs(120);

/// How long one call to a peer can take, retries included. A call answers
/// at once unless the peer's core is busy.
const CALL_DEADLINE: Duration = Duration::from_secs(30);

/// The nodes one step covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RebuildScope {
    /// This node's own rows only.
    ThisNode,
    /// This node and every active peer.
    EveryNode,
}

/// The collection one drive rebuilds.
#[derive(Clone, Copy)]
pub(super) struct RebuildStep<'a> {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub collection: &'a str,
}

impl RebuildStep<'_> {
    /// Names the step in every error message.
    fn what(&self) -> String {
        format!("full-text index rebuild on '{}'", self.collection)
    }

    /// The op over the fields the collection's records declare at the call, or
    /// `None` once the catalog holds the collection no more or holds it
    /// soft-dropped: its drop reclaims the text index.
    fn current_plan(&self, state: &SharedState) -> Result<Option<PhysicalPlan>, DdlError> {
        let in_context = |e: &crate::Error| DdlError::from_error_in_context(&self.what(), e);
        let catalog = state.credentials.catalog();
        let Some(coll) = catalog
            .get_collection(self.database_id, self.tenant_id.as_u64(), self.collection)
            .map_err(|e| in_context(&e))?
            .filter(|coll| coll.is_active)
        else {
            return Ok(None);
        };
        let set = super::super::text_fields::text_field_set(catalog, &coll)
            .map_err(|e| in_context(&e))?;
        Ok(Some(PhysicalPlan::Text(TextOp::RebuildIndex {
            collection: nodedb_types::QualifiedCollection::new(self.database_id, self.collection),
            text_fields: set.indexed().to_vec(),
            analyzer: set.analyzer().map(str::to_string),
            fuzzy_default: set.fuzzy_default(),
        })))
    }
}

/// Run `step` on the nodes `scope` covers until each answers `Installed`.
/// The nodes run concurrently. The first error fails the step.
pub(super) async fn drive_text_rebuild(
    state: &SharedState,
    step: RebuildStep<'_>,
    scope: RebuildScope,
) -> Result<(), DdlError> {
    let peers = match scope {
        RebuildScope::ThisNode => Vec::new(),
        RebuildScope::EveryNode => active_peers(state),
    };
    let peer_drives = peers
        .into_iter()
        .map(|node_id| drive_peer(state, step, node_id));
    futures::future::try_join(
        drive_local(state, step),
        futures::future::try_join_all(peer_drives),
    )
    .await
    .map(|_| ())
}

/// Poll this node's Data Plane until its rebuild is installed.
async fn drive_local(state: &SharedState, step: RebuildStep<'_>) -> Result<(), DdlError> {
    let what = step.what();
    let in_context = |e: &crate::Error| DdlError::from_error_in_context(&what, e);
    // The rebuild runs on the core that owns the collection's vShard, as
    // an index backfill does.
    let vshard = nodedb_types::CollectionKey::from_bare(step.database_id, step.collection).vshard();
    let mut stall = Stall::new();
    loop {
        let Some(plan) = step.current_plan(state)? else {
            return Ok(());
        };
        let response = crate::control::server::dispatch_utils::dispatch_to_data_plane(
            state,
            step.tenant_id,
            step.database_id,
            vshard,
            plan,
            TraceId::ZERO,
        )
        .await
        .map_err(|e| in_context(&e))?;
        if response.status == Status::Error {
            // A coded refusal keeps its SQLSTATE.
            return Err(match response.error_code.as_deref() {
                Some(code) => in_context(&crate::Error::DataPlane(code.clone())),
                None => DdlError::internal(format!(
                    "{what}: {}",
                    String::from_utf8_lossy(&response.payload)
                )),
            });
        }
        let progress = decode_progress(&response.payload, &what)?;
        if stall.installed_or_check(progress, &what, "this node")? {
            return Ok(());
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Poll peer `node_id` until its rebuild is installed.
async fn drive_peer(
    state: &SharedState,
    step: RebuildStep<'_>,
    node_id: u64,
) -> Result<(), DdlError> {
    let what = step.what();
    let node = format!("node {node_id}");
    let mut stall = Stall::new();
    loop {
        let Some(plan) = step.current_plan(state)? else {
            return Ok(());
        };
        let call = PeerCall::new(state, step.tenant_id, step.database_id, &plan, &what)?;
        let payloads = call.execute(node_id, CALL_DEADLINE).await?;
        let payload = payloads.first().ok_or_else(|| {
            DdlError::internal(format!(
                "peer {what} on {node}: the answer carries no payload"
            ))
        })?;
        let progress = decode_progress(payload, &what)?;
        if stall.installed_or_check(progress, &what, &node)? {
            return Ok(());
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

fn decode_progress(payload: &[u8], what: &str) -> Result<TextRebuildProgress, DdlError> {
    zerompk::from_msgpack(payload)
        .map_err(|e| DdlError::internal(format!("{what}: rebuild progress decode: {e}")))
}

/// The last replayed count one node reported, and when it last moved.
struct Stall {
    replayed: Option<u64>,
    moved_at: Instant,
}

impl Stall {
    fn new() -> Self {
        Self {
            replayed: None,
            moved_at: Instant::now(),
        }
    }

    /// `true` once `progress` is `Installed`. An error once the replayed
    /// count stayed still for [`STALL_LIMIT`].
    fn installed_or_check(
        &mut self,
        progress: TextRebuildProgress,
        what: &str,
        node: &str,
    ) -> Result<bool, DdlError> {
        let replayed = match progress {
            TextRebuildProgress::Installed { .. } => return Ok(true),
            TextRebuildProgress::Running { replayed } => replayed,
        };
        if self.replayed != Some(replayed) {
            self.replayed = Some(replayed);
            self.moved_at = Instant::now();
            return Ok(false);
        }
        if self.moved_at.elapsed() >= STALL_LIMIT {
            return Err(DdlError::new(
                "57014",
                format!(
                    "{what}: the rebuild walk on {node} did not move for {}s; \
                     run the statement again",
                    STALL_LIMIT.as_secs()
                ),
            ));
        }
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_moving_count_keeps_the_step_alive_and_installed_ends_it() {
        let mut stall = Stall::new();
        let running = |replayed| TextRebuildProgress::Running { replayed };
        assert!(
            !stall
                .installed_or_check(running(0), "w", "n")
                .expect("first")
        );
        stall.moved_at = Instant::now() - STALL_LIMIT;
        assert!(
            !stall
                .installed_or_check(running(5), "w", "n")
                .expect("moved"),
            "a moved count resets the stall clock"
        );
        stall.moved_at = Instant::now() - STALL_LIMIT;
        let stuck = stall
            .installed_or_check(running(5), "w", "n")
            .expect_err("a still count past the limit fails");
        assert_eq!(stuck.sqlstate, "57014");
        assert!(
            stall
                .installed_or_check(TextRebuildProgress::Installed { replayed: 5 }, "w", "n")
                .expect("installed")
        );
    }
}
