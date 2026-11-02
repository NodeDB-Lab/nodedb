// SPDX-License-Identifier: BUSL-1.1

//! Keeping the newest-ordinal cache equal to storage as committed writes
//! publish.
//!
//! Every committed write publishes its version note once its install
//! settled (`CoreLoop::publish_write`), and the note lands here. A
//! keyed write raises its slice to the row's newest stored version, read
//! back from storage, so a version below the row's newest (a history
//! install) raises nothing past it. A note with no per-key identity forgets
//! the collection and the vShard the write applied on, and the next read
//! recomputes them.

use nodedb_types::StorageKey;

use crate::engine::graph::edge_store::EdgeRef;
use crate::types::{DatabaseId, KeyRepr, TenantId, VShardId};

use super::super::CoreLoop;
use super::cache::{SliceKey, collection_vshard};

impl CoreLoop {
    /// Fold a committed write of `collection` into the cache. `key` names
    /// the written row, `None` for a write whose rows carry no per-key
    /// identity. `vshard` is the vShard the write applied on: an array
    /// tile write lands on its tile's vShard, not its collection's.
    pub(in crate::data::executor) fn track_newest_write(
        &mut self,
        db: DatabaseId,
        tenant: TenantId,
        collection: &str,
        key: Option<&KeyRepr>,
        vshard: VShardId,
    ) {
        let home = collection_vshard(db, collection);
        let raised = match key {
            None => None,
            Some(KeyRepr::Surrogate(surrogate)) => self
                .sparse
                .versioned_latest_sys_from(
                    db.as_u64(),
                    tenant.as_u64(),
                    collection,
                    &StorageKey::for_surrogate(nodedb_types::Surrogate::new(*surrogate)),
                )
                .ok()
                .flatten()
                .map(|newest| vec![(home, newest)]),
            Some(KeyRepr::KvKey(kv_key)) => self
                .kv_engine
                .key_newest_ordinal(db.as_u64(), tenant.as_u64(), collection, kv_key)
                .map(|newest| vec![(home, newest)]),
            Some(KeyRepr::Edge { src, label, dst }) => self
                .edge_store
                .latest_version_ordinal(EdgeRef::new(db, tenant, collection, src, label, dst))
                .ok()
                .flatten()
                .map(|newest| {
                    vec![
                        (VShardId::from_key(src.as_bytes()), newest),
                        (VShardId::from_key(dst.as_bytes()), newest),
                    ]
                }),
        };
        // A row no store answers for lives in an engine the keyed lookups
        // do not read, so its collection is recomputed instead.
        let Some(raised) = raised else {
            self.newest_ordinals
                .forget_collection(db, tenant, collection, home);
            self.newest_ordinals.forget_vshard(vshard);
            return;
        };
        for (vshard, newest) in raised {
            self.newest_ordinals.raise(
                &SliceKey {
                    vshard,
                    db,
                    tenant,
                    collection: Box::from(collection),
                },
                newest,
            );
        }
    }

    /// Forget every cached newest ordinal: the stores changed in a way no
    /// write note describes (core open, snapshot install, purge, drop). The
    /// next read recomputes from storage.
    pub(crate) fn forget_newest_ordinals(&mut self) {
        self.newest_ordinals.clear();
    }
}
