// SPDX-License-Identifier: BUSL-1.1

//! The fence keys a plan names, in the forms an install arms them.
//!
//! An install keys each collection it writes by the database its stored
//! name names, and each sum target and its graph state by the request's
//! database. A key built here in another form never meets the install's
//! key, so the fence silently admits what it holds. Each builder here
//! matches one arming form.
//!
//! A name that parses as no stored name files a report, and the task
//! becomes a task of its whole database.

use nodedb_array::types::ArrayId;
use nodedb_types::QualifiedCollection;
use nodedb_types::sync::wire::SyncProvenance;

use crate::types::{DatabaseId, TenantId, TxnId};

use super::super::state::FenceKey;
use super::class::{Footprint, Scope};

/// The site a malformed name is reported at, and what the task does.
const MALFORMED_SITE: &str = "fence_footprint: the task waits as a task of its database";

/// The keys of one task, built from its plan.
pub(super) struct KeySet<'s> {
    scope: &'s Scope<'s>,
    keys: Vec<FenceKey>,
    malformed: bool,
}

impl<'s> KeySet<'s> {
    pub(super) fn new(scope: &'s Scope<'s>) -> Self {
        Self {
            scope,
            keys: Vec::new(),
            malformed: false,
        }
    }

    /// The collection stored as `stored`, keyed by the database the name
    /// names: the form an install arms its written collections in.
    pub(super) fn stored(mut self, stored: &str) -> Self {
        match FenceKey::of_stored_collection(stored) {
            Ok(key) => self.keys.push(key),
            Err(error) => {
                crate::diag::stored_collection_name_malformed(MALFORMED_SITE, stored, &error);
                self.malformed = true;
            }
        }
        self
    }

    /// The collection a plan names.
    pub(super) fn collection(self, collection: &QualifiedCollection) -> Self {
        self.stored(collection.as_str())
    }

    /// `name` qualified for the request's database. A name that cannot be
    /// qualified files a report and marks the set malformed.
    fn qualify(&mut self, name: &str) -> Option<QualifiedCollection> {
        self.qualify_in(self.scope.db, name)
    }

    /// `name` qualified for `db`. A name that cannot be qualified files a
    /// report and marks the set malformed.
    fn qualify_in(&mut self, db: DatabaseId, name: &str) -> Option<QualifiedCollection> {
        match QualifiedCollection::from_request(db, name) {
            Ok(qualified) => Some(qualified),
            Err(error) => {
                crate::diag::stored_collection_name_malformed(MALFORMED_SITE, name, &error);
                self.malformed = true;
                None
            }
        }
    }

    /// A materialized-sum target, qualified for the request's database:
    /// the form an install arms its sum targets in.
    pub(super) fn sum_target(mut self, name: &str) -> Self {
        if let Some(qualified) = self.qualify(name) {
            self.keys.push(FenceKey::Collection {
                db: self.scope.db,
                name: qualified.as_str().to_owned(),
            });
        }
        self
    }

    /// A collection a meta op names bare, in the request's database.
    pub(super) fn requested(mut self, name: &str) -> Self {
        match self.qualify(name) {
            Some(qualified) => self.stored(qualified.as_str()),
            None => self,
        }
    }

    /// The array `id`, by the stored name its writes go by.
    pub(super) fn array(self, id: &ArrayId) -> Self {
        let stored = QualifiedCollection::new(id.database_id, &id.name);
        self.stored(stored.as_str())
    }

    /// The graph state of the request's tenant.
    pub(super) fn graph(self) -> Self {
        let tenant = self.scope.tenant;
        self.graph_of(tenant)
    }

    /// The graph state of `tenant` in the request's database.
    pub(super) fn graph_of(mut self, tenant: TenantId) -> Self {
        self.keys.push(FenceKey::Graph {
            db: self.scope.db,
            tenant,
        });
        self
    }

    /// The collection `name` of database `db`, stored or bare.
    pub(super) fn named_in(mut self, db: DatabaseId, name: &str) -> Self {
        match self.qualify_in(db, name) {
            Some(qualified) => self.stored(qualified.as_str()),
            None => self,
        }
    }

    /// The high-water mark of the sync stream `provenance` names. Producer
    /// zero takes no part in producer sequencing: its frame advances no
    /// mark, and nothing is keyed.
    pub(super) fn sync(mut self, provenance: Option<&SyncProvenance>) -> Self {
        self.keys.extend(provenance.and_then(FenceKey::sync_stream));
        self
    }

    /// The tenant-wide CRDT state of `tenant` in the request's database.
    pub(super) fn crdt_tenant_of(mut self, tenant: TenantId) -> Self {
        self.keys.push(FenceKey::CrdtTenant {
            db: self.scope.db,
            tenant,
        });
        self
    }

    /// The overlay of transaction `txn`.
    pub(super) fn txn_overlay(mut self, txn: TxnId) -> Self {
        self.keys.push(FenceKey::TxnOverlay(txn));
        self
    }

    /// The footprint a malformed name leaves: the request's whole
    /// database, or every state of the core when a key spans databases.
    fn malformed_footprint(&self) -> Footprint {
        if self.keys.iter().any(|key| key.database().is_none()) {
            return Footprint::Core;
        }
        self.scope.whole_database()
    }

    /// A read of the keys.
    pub(super) fn read(self) -> Footprint {
        if self.malformed {
            return self.malformed_footprint();
        }
        Footprint::Keys(self.keys)
    }

    /// A write of the keys, by a task whose events carry its vShard.
    pub(super) fn write(self) -> Footprint {
        if self.malformed {
            return self.malformed_footprint();
        }
        Footprint::Write {
            keys: self.keys,
            vshard: self.scope.vshard,
        }
    }
}

/// A read of `collection` alone.
pub(super) fn read_of(collection: &QualifiedCollection, scope: &Scope<'_>) -> Footprint {
    KeySet::new(scope).collection(collection).read()
}

/// A write of `collection` alone.
pub(super) fn write_of(collection: &QualifiedCollection, scope: &Scope<'_>) -> Footprint {
    KeySet::new(scope).collection(collection).write()
}

#[cfg(test)]
pub(super) mod tests {
    use super::super::registry::tests::NO_REGISTRY;
    use super::*;
    use crate::types::VShardId;

    pub(in crate::data::executor::core_loop::install_fence) const TENANT: TenantId =
        TenantId::new(1);
    pub(in crate::data::executor::core_loop::install_fence) const VSHARD: VShardId =
        VShardId::new(3);

    /// The scope of a request of the default database, outside any
    /// transaction, on a core whose registries hold nothing.
    pub(in crate::data::executor::core_loop::install_fence) fn scope() -> Scope<'static> {
        Scope {
            db: DatabaseId::DEFAULT,
            tenant: TENANT,
            vshard: VSHARD,
            txn: None,
            stream: crate::bridge::envelope::RequestStream::Internal,
            registry: &NO_REGISTRY,
        }
    }

    pub(in crate::data::executor::core_loop::install_fence) fn coll(
        name: &str,
    ) -> QualifiedCollection {
        QualifiedCollection::new(DatabaseId::DEFAULT, name)
    }

    pub(in crate::data::executor::core_loop::install_fence) fn key(name: &str) -> FenceKey {
        FenceKey::Collection {
            db: DatabaseId::DEFAULT,
            name: name.into(),
        }
    }

    pub(in crate::data::executor::core_loop::install_fence) fn graph() -> FenceKey {
        FenceKey::Graph {
            db: DatabaseId::DEFAULT,
            tenant: TENANT,
        }
    }

    pub(in crate::data::executor::core_loop::install_fence) fn write(
        keys: Vec<FenceKey>,
    ) -> Footprint {
        Footprint::Write {
            keys,
            vshard: VSHARD,
        }
    }

    pub(in crate::data::executor::core_loop::install_fence) fn whole() -> Footprint {
        scope().whole_database()
    }

    /// A sum target keys as the install arms it: qualified for the
    /// request's database, whether the plan names it bare or qualified.
    #[test]
    fn a_sum_target_keys_in_the_form_the_install_arms() {
        let other = DatabaseId::new(7);
        let scope = Scope {
            db: other,
            ..scope()
        };
        let armed = FenceKey::Collection {
            db: other,
            name: QualifiedCollection::new(other, "acct").as_str().into(),
        };
        let bare = KeySet::new(&scope).sum_target("acct").write();
        let qualified = KeySet::new(&scope).sum_target("7/acct").write();
        let expected = Footprint::Write {
            keys: vec![armed],
            vshard: VSHARD,
        };
        assert_eq!(bare, expected);
        assert_eq!(qualified, expected);
        // The stored form of the same collection is the same key.
        assert_eq!(KeySet::new(&scope).stored("7/acct").write(), expected);
    }

    #[test]
    fn a_sum_target_of_another_database_is_the_whole_database() {
        assert_eq!(KeySet::new(&scope()).sum_target("7/acct").write(), whole());
    }

    #[test]
    fn an_array_keys_by_its_database_and_stored_name() {
        let id = ArrayId::in_database(TENANT, DatabaseId::new(7), "grid");
        assert_eq!(
            KeySet::new(&scope()).array(&id).read(),
            Footprint::Keys(vec![FenceKey::Collection {
                db: DatabaseId::new(7),
                name: "7/grid".into(),
            }])
        );
    }

    #[test]
    fn a_malformed_stored_name_is_the_whole_database() {
        assert_eq!(KeySet::new(&scope()).stored("x/y/z").read(), whole());
    }

    /// A sync stream spans databases, so no whole database covers it.
    #[test]
    fn a_malformed_name_beside_a_sync_stream_is_a_core_task() {
        let provenance = SyncProvenance {
            producer_id: 5,
            epoch: 1,
            stream_id: 8,
            seq: 1,
        };
        assert_eq!(
            KeySet::new(&scope())
                .stored("x/y/z")
                .sync(Some(&provenance))
                .write(),
            Footprint::Core
        );
    }

    #[test]
    fn a_sync_stream_keys_its_producer_and_stream() {
        let provenance = SyncProvenance {
            producer_id: 5,
            epoch: 1,
            stream_id: 8,
            seq: 1,
        };
        assert_eq!(
            KeySet::new(&scope()).sync(Some(&provenance)).read(),
            Footprint::Keys(vec![FenceKey::SyncStream {
                producer_id: 5,
                stream_id: 8,
            }])
        );
        let local = SyncProvenance {
            producer_id: 0,
            ..provenance
        };
        assert_eq!(
            KeySet::new(&scope()).sync(Some(&local)).read(),
            Footprint::Keys(Vec::new())
        );
    }

    #[test]
    fn a_crdt_tenant_and_an_overlay_key_in_their_forms() {
        assert_eq!(
            KeySet::new(&scope())
                .crdt_tenant_of(TENANT)
                .txn_overlay(TxnId::new(3))
                .read(),
            Footprint::Keys(vec![
                FenceKey::CrdtTenant {
                    db: DatabaseId::DEFAULT,
                    tenant: TENANT,
                },
                FenceKey::TxnOverlay(TxnId::new(3)),
            ])
        );
    }
}
