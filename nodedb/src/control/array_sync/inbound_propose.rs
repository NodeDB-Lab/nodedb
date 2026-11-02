// SPDX-License-Identifier: BUSL-1.1

//! Raft propose helpers for [`OriginArrayInbound`].
//!
//! These methods handle the Raft path (`propose_and_await`), plus the small
//! helpers for binding a put's cell surrogate and computing the destination
//! vShard.

use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::op::ArrayOp;
use nodedb_types::sync::wire::array::{ArrayRejectMsg, ArrayRejectReason};
use tracing::{error, warn};

use crate::control::array_catalog::tile_vshard::cell_tile_vshard;
use crate::control::wal_replication::ReplicatedEntry;
use crate::types::{TraceId, VShardId};

use super::inbound::OriginArrayInbound;
use super::reject::{build_reject, engine_reject};

impl OriginArrayInbound {
    fn consume_array_write_authorization(
        &self,
        authorization: crate::control::server::shared::authorization::AuthorizedCollection,
        array: &str,
        hlc: Hlc,
    ) -> Result<(), ArrayRejectMsg> {
        let (tenant_id, database_id, collection, permission) = authorization.into_scope();
        if tenant_id != self.tenant_id()
            || database_id != self.database_id()
            || collection != array
            || permission != crate::control::security::identity::Permission::Write
        {
            return Err(build_reject(
                array,
                hlc,
                ArrayRejectReason::PermissionDenied,
                "array authorization scope mismatch".to_string(),
            ));
        }
        Ok(())
    }

    /// Propose a `ReplicatedEntry` to Raft and await its commit + apply.
    ///
    /// Returns `Ok(())` when the entry has been committed and executed by the
    /// distributed applier. A failure maps to its reject reason through the
    /// typed error.
    pub(super) async fn propose_and_await(
        &self,
        mut entry: ReplicatedEntry,
        array: &str,
        hlc: Hlc,
        authorization: crate::control::server::shared::authorization::AuthorizedCollection,
    ) -> Result<(), ArrayRejectMsg> {
        self.consume_array_write_authorization(authorization, array, hlc)?;
        if entry.tenant_id != self.tenant_id().as_u64()
            || entry.database_id != self.database_id().as_u64()
        {
            return Err(build_reject(
                array,
                hlc,
                ArrayRejectReason::PermissionDenied,
                "array replicated entry scope mismatch".to_string(),
            ));
        }
        // Both proposers below take prebuilt bytes, so the floor is stamped
        // here: replicas hold the write until they applied the array's DDL.
        // The commit instant is stamped once, so every replica dates the
        // write alike.
        entry.write_hlc = self.shared().hlc_clock.now().wall_ns;
        crate::control::wal_replication::stamp_metadata_floor(self.shared(), &mut entry);
        crate::control::array_catalog::cell_route::stamp_incarnation(self.shared(), &mut entry);
        let vshard_id = entry.vshard_id;
        let idempotency_key = entry.idempotency_key;
        let data = entry.encode().map_err(|e| {
            error!(array = %array, error = %e, "array_inbound: replicated entry encode failed");
            engine_reject(array, hlc, "replicated entry encode failed", &e)
        })?;

        // The async proposer forwards to the group leader and waits for the
        // apply. It returns the apply payload directly.
        let async_proposer = self
            .shared()
            .async_raft_proposer()
            .map_err(|e| engine_reject(array, hlc, "raft proposer not available", &e))?
            .as_ref();
        let deadline =
            crate::control::server::shared::session::StatementDeadline::of_statement(self.shared())
                .tokio_instant();
        match async_proposer(vshard_id, idempotency_key, data, deadline).await {
            Ok(_payload) => Ok(()),
            Err(e) => {
                warn!(array = %array, error = %e, "array_inbound: raft propose+apply failed");
                Err(engine_reject(array, hlc, "raft propose failed", &e))
            }
        }
    }

    /// The bound surrogate of a put's cell, assigned at the array's home under
    /// the `(array, zerompk(coord))` key the SQL insert path binds. A delete
    /// or erasure names a coordinate, not a row, and carries none.
    pub(super) async fn cell_surrogate(
        &self,
        op: &ArrayOp,
    ) -> Result<Option<nodedb_types::Surrogate>, ArrayRejectMsg> {
        use nodedb_array::sync::op::ArrayOpKind;
        if !matches!(op.kind, ArrayOpKind::Put) {
            return Ok(None);
        }
        let reject = |context: &str, error: &crate::Error| {
            engine_reject(&op.header.array, op.header.hlc, context, error)
        };
        let pk = zerompk::to_msgpack_vec(&op.coord).map_err(|e| {
            reject(
                "array cell coord encode",
                &crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: e.to_string(),
                },
            )
        })?;
        let surrogate = crate::control::server::surrogate_exchange::assign_surrogate_routed(
            self.shared(),
            nodedb_types::CollectionKey::from_bare(self.database_id(), &op.header.array),
            self.tenant_id(),
            &pk,
            TraceId::ZERO,
        )
        .await
        .map_err(|e| reject("array cell surrogate assign", &e))?;
        Ok(Some(surrogate))
    }

    /// The vShard that owns this op's tile: the vShard a SQL put of the
    /// same cell routes to (`cell_tile_vshard`). The entry carries it, so
    /// every replica applies and places the cell on it.
    ///
    /// A reject when the array has no catalog row on this node, or the
    /// coordinate has no tile in the array's domain.
    pub(super) fn vshard_for_op(&self, op: &ArrayOp) -> Result<VShardId, ArrayRejectMsg> {
        let entry = {
            let mirror = match self.shared().array_catalog.read() {
                Ok(mirror) => mirror,
                Err(poisoned) => poisoned.into_inner(),
            };
            mirror.lookup_by_name_in_database(
                self.tenant_id(),
                self.database_id(),
                &op.header.array,
            )
        };
        let Some(entry) = entry else {
            return Err(build_reject(
                &op.header.array,
                op.header.hlc,
                ArrayRejectReason::ArrayUnknown,
                format!("array '{}' has no catalog row on Origin", op.header.array),
            ));
        };
        cell_tile_vshard(&entry, &op.coord).map_err(|e| {
            build_reject(
                &op.header.array,
                op.header.hlc,
                ArrayRejectReason::ShapeInvalid,
                format!("array op cell has no tile route: {e}"),
            )
        })
    }
}
