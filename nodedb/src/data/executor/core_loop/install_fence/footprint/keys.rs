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

use crate::types::TenantId;

use super::super::state::FenceKey;
use super::class::{Footprint, Scope};

/// The site a malformed name is reported at, and what the task does.
const MALFORMED_SITE: &str = "fence_footprint: the task waits as a task of its database";

/// The keys of one task, built from its plan.
pub(super) struct KeySet<'s> {
    scope: &'s Scope,
    keys: Vec<FenceKey>,
    malformed: bool,
}

impl<'s> KeySet<'s> {
    pub(super) fn new(scope: &'s Scope) -> Self {
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
        match QualifiedCollection::from_request(self.scope.db, name) {
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

    /// A read of the keys.
    pub(super) fn read(self) -> Footprint {
        if self.malformed {
            return self.scope.whole_database();
        }
        Footprint::Keys(self.keys)
    }

    /// A write of the keys, by a task whose events carry its vShard.
    pub(super) fn write(self) -> Footprint {
        if self.malformed {
            return self.scope.whole_database();
        }
        Footprint::Write {
            keys: self.keys,
            vshard: self.scope.vshard,
        }
    }
}

/// A read of `collection` alone.
pub(super) fn read_of(collection: &QualifiedCollection, scope: &Scope) -> Footprint {
    KeySet::new(scope).collection(collection).read()
}

/// A write of `collection` alone.
pub(super) fn write_of(collection: &QualifiedCollection, scope: &Scope) -> Footprint {
    KeySet::new(scope).collection(collection).write()
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::types::{DatabaseId, VShardId};

    pub(in crate::data::executor::core_loop::install_fence) const TENANT: TenantId =
        TenantId::new(1);
    pub(in crate::data::executor::core_loop::install_fence) const VSHARD: VShardId =
        VShardId::new(3);

    pub(in crate::data::executor::core_loop::install_fence) fn scope() -> Scope {
        Scope {
            db: DatabaseId::DEFAULT,
            tenant: TENANT,
            vshard: VSHARD,
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
}
