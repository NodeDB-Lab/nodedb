// SPDX-License-Identifier: BUSL-1.1

//! Per-session read-your-writes floor tracking on `SessionStore`.
//!
//! Records the highest committed write version this session holds for each
//! `(database, tenant, collection, vShard)` it has written. A later
//! transaction's read-set capture is floored at it. This extends the
//! read-your-own-write exclusion the Calvin static builder applies to buffered
//! writes to the session's prior committed writes. Without it, OCC validation
//! sees the session's own committed write above a read that captured an older
//! version, and aborts the transaction on its own write.
//!
//! Only this session's own committed writes raise the floor. A concurrent
//! write by another session has a higher version that still exceeds the
//! floor, so a real conflict still aborts.

use nodedb_types::WriteVersion;

use super::connection::SessionId;
use crate::bridge::envelope::{PhysicalPlan, Response, Status};
use crate::control::security::identity::{Permission, required_permission};
use crate::control::server::shared::plan_util::extract_collection;
use crate::types::{DatabaseId, ReadVersions, TenantId, VShardId};

use super::store::SessionStore;

/// The scope of one own-write floor: database, tenant, collection, and the
/// vShard whose versions the floor is a position in.
pub type OwnWriteKey = (DatabaseId, TenantId, String, VShardId);

impl SessionStore {
    /// Record the versions a dispatched write of `plan` stamped, taken from
    /// its `response`. Every protocol calls this once per dispatched task.
    /// A read, a failed write, or a write of no single collection records
    /// nothing.
    pub fn note_own_write_response(
        &self,
        addr: impl Into<SessionId>,
        database_id: DatabaseId,
        tenant_id: TenantId,
        plan: &PhysicalPlan,
        response: &Response,
    ) {
        if response.status != Status::Ok
            || response.read_versions.is_empty()
            || !matches!(required_permission(plan), Permission::Write)
        {
            return;
        }
        let Some(collection) = extract_collection(plan) else {
            return;
        };
        self.note_own_write(
            addr,
            database_id,
            tenant_id,
            collection,
            &response.read_versions,
        );
    }

    /// Record the versions a committed write to `(database, tenant,
    /// collection)` stamped, keeping the maximum per vShard. `versions` come
    /// from the write's response. A write that stamped none sets no floor.
    /// The floors persist for the life of the session.
    pub fn note_own_write(
        &self,
        addr: impl Into<SessionId>,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
        versions: &ReadVersions,
    ) {
        if versions.is_empty() {
            return;
        }
        self.write_session(addr, |session| {
            for shard in versions.iter() {
                let key = (
                    database_id,
                    tenant_id,
                    collection.to_string(),
                    VShardId::new(shard.vshard),
                );
                let slot = session.own_write_versions.entry(key).or_default();
                *slot = (*slot).max(shard.version);
            }
        });
    }

    /// The session's own highest committed write version for `(database,
    /// tenant, collection)` on `vshard`, or `WriteVersion::ZERO` when the
    /// session never wrote it there.
    pub fn own_write_version(
        &self,
        addr: impl Into<SessionId>,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
        vshard: VShardId,
    ) -> WriteVersion {
        self.read_session(addr, |session| {
            session
                .own_write_versions
                .get(&(database_id, tenant_id, collection.to_string(), vshard))
                .copied()
                .unwrap_or_default()
        })
        .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::*;

    const HOME: VShardId = VShardId::new(4);

    fn addr() -> SocketAddr {
        "127.0.0.1:5601".parse().expect("test addr")
    }

    fn store_with_session() -> (SessionStore, SocketAddr) {
        let sessions = SessionStore::new();
        let a = addr();
        sessions.ensure_session(a);
        (sessions, a)
    }

    fn at(index: u64) -> ReadVersions {
        ReadVersions::single(HOME, WriteVersion::logged(1, index))
    }

    fn floor(
        sessions: &SessionStore,
        a: SocketAddr,
        tenant: u64,
        collection: &str,
    ) -> WriteVersion {
        sessions.own_write_version(
            a,
            DatabaseId::DEFAULT,
            TenantId::new(tenant),
            collection,
            HOME,
        )
    }

    #[test]
    fn absent_collection_returns_zero() {
        let (sessions, a) = store_with_session();
        assert_eq!(floor(&sessions, a, 1, "bread"), WriteVersion::ZERO);
    }

    #[test]
    fn records_and_returns_own_write_version() {
        let (sessions, a) = store_with_session();
        sessions.note_own_write(a, DatabaseId::DEFAULT, TenantId::new(1), "bread", &at(2));
        assert_eq!(floor(&sessions, a, 1, "bread"), WriteVersion::logged(1, 2));
    }

    #[test]
    fn keeps_the_maximum_version() {
        let (sessions, a) = store_with_session();
        sessions.note_own_write(a, DatabaseId::DEFAULT, TenantId::new(1), "bread", &at(5));
        // A lower version never lowers the floor.
        sessions.note_own_write(a, DatabaseId::DEFAULT, TenantId::new(1), "bread", &at(3));
        assert_eq!(floor(&sessions, a, 1, "bread"), WriteVersion::logged(1, 5));
    }

    #[test]
    fn a_write_with_no_versions_sets_no_floor() {
        let (sessions, a) = store_with_session();
        sessions.note_own_write(
            a,
            DatabaseId::DEFAULT,
            TenantId::new(1),
            "bread",
            &ReadVersions::new(),
        );
        assert_eq!(floor(&sessions, a, 1, "bread"), WriteVersion::ZERO);
    }

    fn kv_plan(collection: &str, write: bool) -> PhysicalPlan {
        let collection = nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, collection);
        PhysicalPlan::Kv(if write {
            nodedb_physical::physical_plan::KvOp::Put {
                collection,
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                ttl_ms: 0,
                surrogate: nodedb_types::Surrogate::new(1),
                returning: None,
                rls_filters: Vec::new(),
                provenance: None,
            }
        } else {
            nodedb_physical::physical_plan::KvOp::Get {
                collection,
                key: b"k".to_vec(),
                rls_filters: Vec::new(),
                surrogate_ceiling: None,
            }
        })
    }

    fn response(status: Status, versions: ReadVersions) -> Response {
        let mut response = crate::control::server::shared::write_admission::bare_ok_response(
            crate::types::RequestId::new(1),
        );
        response.status = status;
        response.read_versions = versions;
        response
    }

    #[test]
    fn a_dispatched_write_response_sets_the_floor() {
        let (sessions, a) = store_with_session();
        sessions.note_own_write_response(
            a,
            DatabaseId::DEFAULT,
            TenantId::new(1),
            &kv_plan("bread", true),
            &response(Status::Ok, at(9)),
        );
        assert_eq!(floor(&sessions, a, 1, "bread"), WriteVersion::logged(1, 9));
    }

    #[test]
    fn a_read_or_a_failed_write_sets_no_floor() {
        let (sessions, a) = store_with_session();
        sessions.note_own_write_response(
            a,
            DatabaseId::DEFAULT,
            TenantId::new(1),
            &kv_plan("bread", false),
            &response(Status::Ok, at(9)),
        );
        sessions.note_own_write_response(
            a,
            DatabaseId::DEFAULT,
            TenantId::new(1),
            &kv_plan("bread", true),
            &response(Status::Error, at(9)),
        );
        assert_eq!(floor(&sessions, a, 1, "bread"), WriteVersion::ZERO);
    }

    #[test]
    fn scoped_by_database_tenant_collection_and_vshard() {
        let (sessions, a) = store_with_session();
        sessions.note_own_write(a, DatabaseId::DEFAULT, TenantId::new(1), "bread", &at(7));
        // A different collection, tenant, or vShard sees no floor.
        assert_eq!(floor(&sessions, a, 1, "milk"), WriteVersion::ZERO);
        assert_eq!(floor(&sessions, a, 2, "bread"), WriteVersion::ZERO);
        assert_eq!(
            sessions.own_write_version(
                a,
                DatabaseId::DEFAULT,
                TenantId::new(1),
                "bread",
                VShardId::new(5),
            ),
            WriteVersion::ZERO
        );
    }
}
