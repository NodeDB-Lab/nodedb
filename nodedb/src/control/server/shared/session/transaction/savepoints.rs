// SPDX-License-Identifier: BUSL-1.1

//! SAVEPOINT / RELEASE / ROLLBACK TO on an open transaction.

use std::collections::BTreeMap;

use crate::types::VShardId;

use super::super::connection::SessionId;
use super::super::state::{OverlayMarkers, SavepointEntry};
use super::super::store::SessionStore;

/// What a ROLLBACK TO must rewind: per-vShard Data-Plane overlay journal
/// markers, and the task-local DDL buffer length.
pub struct SavepointRewind {
    /// A vShard first staged AFTER the savepoint is absent; rewind it to
    /// all-zero markers.
    pub markers: BTreeMap<VShardId, OverlayMarkers>,
    pub ddl_buffer_len: usize,
}

impl SessionStore {
    /// Create a savepoint at the current tx_buffer position.
    ///
    /// `markers` maps each vShard that had staged writes at savepoint time to its
    /// Data-Plane value/TTL, GRAPH, and ARRAY overlay undo-journal lengths (captured via
    /// `MetaOp::MarkSavepoint`), so a later ROLLBACK TO can rewind every staging
    /// overlay to exactly this point. `ddl_buffer_len` is the task-local DDL
    /// buffer's length at savepoint time — `SessionStore` cannot read the
    /// task-local itself, so the caller (which runs inside the connection's
    /// task) supplies it.
    pub fn create_savepoint(
        &self,
        addr: impl Into<SessionId>,
        name: String,
        markers: BTreeMap<VShardId, OverlayMarkers>,
        ddl_buffer_len: usize,
    ) {
        self.write_session(addr, |session| {
            let buffer_len = session.tx_buffer.len();
            let pending_offset_len = session.pending_offset_commits.len();
            let pending_inference_len = session.pending_field_inference.len();
            session.savepoints.push(SavepointEntry {
                name,
                buffer_len,
                pending_offset_len,
                pending_inference_len,
                ddl_buffer_len,
                markers,
            });
        });
    }

    /// Release a savepoint: destroy the named savepoint and every savepoint
    /// established after it, keeping their buffered/staged effects (PostgreSQL
    /// semantics). Returns `Err` (SQLSTATE 3B001) if the name does not exist.
    pub fn release_savepoint(&self, addr: impl Into<SessionId>, name: &str) -> crate::Result<()> {
        self.write_session(addr, |session| {
            let pos = session
                .savepoints
                .iter()
                .rposition(|e| e.name == name)
                .ok_or_else(|| crate::Error::BadRequest {
                    detail: format!("savepoint \"{name}\" does not exist"),
                })?;
            session.savepoints.truncate(pos);
            Ok(())
        })
        .unwrap_or_else(|| {
            Err(crate::Error::BadRequest {
                detail: "no active session".to_string(),
            })
        })
    }

    /// Rollback to a savepoint: truncate tx_buffer to the saved position and
    /// return what the caller must rewind (see [`SavepointRewind`]).
    ///
    /// Returns `Err` if the savepoint does not exist (matches PostgreSQL behavior).
    pub fn rollback_to_savepoint(
        &self,
        addr: impl Into<SessionId>,
        name: &str,
    ) -> crate::Result<SavepointRewind> {
        self.write_session(addr, |session| {
            let pos = session
                .savepoints
                .iter()
                .rposition(|e| e.name == name)
                .ok_or_else(|| crate::Error::BadRequest {
                    detail: format!("savepoint \"{name}\" does not exist"),
                })?;
            let buffer_len = session.savepoints[pos].buffer_len;
            let pending_offset_len = session.savepoints[pos].pending_offset_len;
            let pending_inference_len = session.savepoints[pos].pending_inference_len;
            let ddl_buffer_len = session.savepoints[pos].ddl_buffer_len;
            let markers = session.savepoints[pos].markers.clone();
            if session.tx_buffer.len() != session.tx_lease_scopes.len() {
                return Err(crate::Error::Internal {
                    detail: "transaction lease scope holders are misaligned".into(),
                });
            }
            session.tx_buffer.truncate(buffer_len);
            session.tx_lease_scopes.truncate(buffer_len);
            session.pending_offset_commits.truncate(pending_offset_len);
            session
                .pending_field_inference
                .truncate(pending_inference_len);
            debug_assert_eq!(session.tx_buffer.len(), session.tx_lease_scopes.len());
            session.savepoints.truncate(pos + 1);
            Ok(SavepointRewind {
                markers,
                ddl_buffer_len,
            })
        })
        .unwrap_or_else(|| {
            Err(crate::Error::BadRequest {
                detail: "no active session".to_string(),
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use nodedb_physical::physical_plan::{MetaOp, PhysicalPlan};
    use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

    use crate::control::lease::QueryLeaseScope;
    use crate::control::server::shared::session::state::PendingOffsetCommit;
    use crate::types::{DatabaseId, Lsn, TenantId};

    fn task() -> PhysicalTask {
        PhysicalTask {
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(1),
            plan: PhysicalPlan::Meta(MetaOp::WalAppend {
                payload: Vec::new(),
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }
    }

    #[test]
    fn savepoint_rollback_truncates_aligned_lease_holders() {
        let store = SessionStore::new();
        let addr: std::net::SocketAddr = "127.0.0.1:6010".parse().expect("address");
        store.ensure_session(addr);
        store.begin(addr, Lsn::new(1), 0).expect("begin");

        let scope = Arc::new(QueryLeaseScope::empty());
        assert!(store.buffer_write(addr, task()));
        assert!(store.attach_tx_lease_scope_since(addr, 0, Arc::clone(&scope)));
        store.create_savepoint(addr, "sp".into(), BTreeMap::new(), 0);
        assert!(store.buffer_write(addr, task()));
        assert!(store.attach_tx_lease_scope_since(addr, 1, Arc::clone(&scope)));

        store
            .rollback_to_savepoint(addr, "sp")
            .expect("rollback to savepoint");
        store.read_session(addr, |session| {
            assert_eq!(session.tx_buffer.len(), 1);
            assert_eq!(session.tx_buffer.len(), session.tx_lease_scopes.len());
            assert!(session.tx_lease_scopes[0].is_some());
        });
    }

    #[test]
    fn rollback_to_savepoint_discards_deferred_offsets_after_the_mark() {
        let store = SessionStore::new();
        let addr: std::net::SocketAddr = "127.0.0.1:6013".parse().expect("address");
        store.ensure_session(addr);
        store.begin(addr, Lsn::new(1), 0).expect("begin");

        let before = PendingOffsetCommit {
            database_id: DatabaseId::DEFAULT,
            tenant_id: 1,
            stream: "orders".into(),
            group: "analytics".into(),
            partition_id: 0,
            offset: crate::event::cdc::CdcOffset::new(10, 1),
        };
        assert!(store.defer_offset_commit(addr, before));
        store.create_savepoint(addr, "sp".into(), BTreeMap::new(), 0);
        assert!(store.defer_offset_commit(
            addr,
            PendingOffsetCommit {
                database_id: DatabaseId::DEFAULT,
                tenant_id: 1,
                stream: "orders".into(),
                group: "analytics".into(),
                partition_id: 0,
                offset: crate::event::cdc::CdcOffset::new(20, 1),
            },
        ));

        store
            .rollback_to_savepoint(addr, "sp")
            .expect("rollback to savepoint");
        let pending = store.take_pending_offsets(addr);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].offset, crate::event::cdc::CdcOffset::new(10, 1));
    }
}
