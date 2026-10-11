// SPDX-License-Identifier: BUSL-1.1

//! Fixture for the sync-dispatch proposal tests.
//!
//! Both dispatch shapes (`response.rs` and `write.rs`) propose the write and
//! answer with the applied entry's payload, and refuse a write on a state with
//! no proposer. Testing that needs a `SharedState` and a proposer that stands
//! in for an applying Raft group, which is the same fixture for both files.

use std::sync::Arc;

use nodedb_physical::physical_plan::TextOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use crate::bridge::dispatch::{CoreChannelDataSide, Dispatcher};
use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::audit::NoopAuditEmitter;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::authorization::{AuthorizedTask, authorize_task_set};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, ReadVersions, TenantId, VShardId};
use crate::wal::WalManager;

pub(super) const COLLECTION: &str = "docs";

pub(super) fn tenant() -> TenantId {
    TenantId::new(1)
}

pub(super) fn vshard() -> VShardId {
    nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, COLLECTION).vshard()
}

/// A `SharedState` whose bridge's Data-Plane side the test drives by hand.
pub(super) fn fixture() -> (Arc<SharedState>, CoreChannelDataSide, tempfile::TempDir) {
    let directory = tempfile::tempdir().expect("temporary WAL directory");
    let wal = Arc::new(
        WalManager::open_for_testing(&directory.path().join("sync-durability.wal"))
            .expect("test WAL"),
    );
    let (dispatcher, mut sides) = Dispatcher::new(1, 64);
    let side = sides.pop().expect("one data side");
    let state = SharedState::new(dispatcher, wal).expect("shared state");
    (state, side, directory)
}

/// A raw proposer that stands in for a data group applying every entry: it
/// answers each proposal with the payload `applied`.
pub(super) fn applying_proposer() -> Arc<crate::control::wal_replication::AsyncRaftProposer> {
    Arc::new(|_vshard, _key, _data, _deadline| {
        Box::pin(async { Ok((b"applied".to_vec(), ReadVersions::new())) })
    })
}

/// A write-class FTS delete on the test collection, authorized for dispatch.
pub(super) fn authorized_write(state: &SharedState) -> AuthorizedTask {
    authorized_plan(
        state,
        PhysicalPlan::Text(TextOp::FtsDeleteDoc {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
            surrogate: Some(nodedb_types::Surrogate::new(1)),
            provenance: None,
        }),
    )
}

/// `plan` on the test collection, authorized for dispatch.
fn authorized_plan(state: &SharedState, plan: PhysicalPlan) -> AuthorizedTask {
    let task = PhysicalTask {
        tenant_id: tenant(),
        database_id: DatabaseId::DEFAULT,
        vshard_id: vshard(),
        plan,
        post_set_op: PostSetOp::None,
        txn_id: None,
    };
    let identity = AuthenticatedIdentity::new_internal_service(
        1,
        "sync-durability-test",
        tenant(),
        Vec::new(),
        true,
        None,
        AuthenticatedIdentity::default_database_set(true),
    );
    authorize_task_set(
        &identity,
        std::slice::from_ref(&task),
        &state.permissions,
        &state.roles,
        &NoopAuditEmitter,
    )
    .expect("authorize test task")
    .into_tasks()
    .into_iter()
    .next()
    .expect("one authorized task")
}
