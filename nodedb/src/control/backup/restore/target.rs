// SPDX-License-Identifier: BUSL-1.1

//! Where one backed-up database's rows restore to.
//!
//! A backup names each database by its id on the source cluster. The
//! restore maps that id to the destination database of the same name. A
//! section's collection names come from the source Data Plane, qualified for
//! the source database. Each re-issue resolves them to the bare catalog name,
//! then qualifies that name for the destination database.

use nodedb_types::{CollectionKey, DatabaseId, QualifiedCollection};

/// The source database of a backup section and the destination database
/// its rows restore into.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DatabaseTarget {
    /// The database id the backup recorded.
    pub source: DatabaseId,
    /// The database id the rows restore into.
    pub dest: DatabaseId,
    /// The id of the RESTORE that re-issues the rows. Every re-issued write
    /// stamps it on its write mark, so a retry of the same envelope knows its
    /// own writes. `0` for a re-issue that is no RESTORE: its writes mark as
    /// user writes.
    pub restore_id: u64,
    /// The closed cut the RESTORE takes effect at (`restore::cut`). A
    /// destination version above it is a write that followed the RESTORE,
    /// and stays current. `i64::MAX` for a re-issue that is no RESTORE: it
    /// sets the backup's state over every destination version.
    pub restore_cut: i64,
}

/// What every re-issued write of one RESTORE carries: its id and its cut.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RestoreMark {
    /// See [`DatabaseTarget::restore_id`].
    pub id: u64,
    /// See [`DatabaseTarget::restore_cut`].
    pub cut: i64,
}

/// A restored collection's names in the destination database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RestoredName {
    /// Bare catalog name. It keys the vShard, the surrogate binds and the
    /// catalog row.
    pub bare: String,
    /// The name the destination Data Plane stores the collection under.
    pub stored: QualifiedCollection,
}

impl RestoredName {
    /// The canonical key of the collection in `database_id`.
    pub fn key(&self, database_id: DatabaseId) -> CollectionKey<'_> {
        CollectionKey::from_bare(database_id, &self.bare)
    }
}

fn malformed(key: &str, why: &str) -> crate::Error {
    let prefix: String = key.chars().take(64).collect();
    crate::Error::Serialization {
        format: "backup".into(),
        detail: format!("restore: section key '{prefix}' is malformed: {why}"),
    }
}

impl DatabaseTarget {
    /// The mark every re-issued write of this target carries.
    pub fn mark(&self) -> RestoreMark {
        RestoreMark {
            id: self.restore_id,
            cut: self.restore_cut,
        }
    }

    /// The destination names of a collection the source Data Plane stored
    /// as `source_stored`.
    pub fn resolve(&self, source_stored: &str) -> crate::Result<RestoredName> {
        let key = CollectionKey::from_qualified_str(self.source, source_stored)?;
        Ok(self.bare(key.name()))
    }

    /// The destination names of the bare catalog name `bare`.
    pub fn bare(&self, bare: &str) -> RestoredName {
        RestoredName {
            bare: bare.to_string(),
            stored: QualifiedCollection::new(self.dest, bare),
        }
    }

    /// The part after `"{db}:{tid}:"` of a scoped section key. The key's
    /// database must be this target's source database and its tenant must be
    /// `tenant_id`.
    pub fn scoped_rest<'k>(&self, key: &'k str, tenant_id: u64) -> crate::Result<&'k str> {
        let mut parts = key.splitn(3, ':');
        let (Some(db), Some(tid), Some(rest)) = (parts.next(), parts.next(), parts.next()) else {
            return Err(malformed(key, "expected '{db}:{tenant}:{collection}'"));
        };
        if db.parse::<u64>().ok() != Some(self.source.as_u64()) {
            return Err(malformed(
                key,
                &format!(
                    "it sits in the section of database {}, but names another database",
                    self.source.as_u64()
                ),
            ));
        }
        if tid.parse::<u64>().ok() != Some(tenant_id) {
            return Err(malformed(
                key,
                &format!("it names a tenant other than {tenant_id}"),
            ));
        }
        if rest.is_empty() {
            return Err(malformed(key, "the collection is empty"));
        }
        Ok(rest)
    }

    /// The destination names of the collection a `"{db}:{tid}:{collection}"`
    /// section key names.
    pub fn resolve_scoped(&self, key: &str, tenant_id: u64) -> crate::Result<RestoredName> {
        self.resolve(self.scoped_rest(key, tenant_id)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TARGET: DatabaseTarget = DatabaseTarget {
        source: DatabaseId::new(1025),
        dest: DatabaseId::new(2048),
        restore_id: 0,
        restore_cut: i64::MAX,
    };

    #[test]
    fn a_source_name_moves_to_the_destination_database() {
        let name = TARGET
            .resolve("1025/orders")
            .expect("qualified for the source");
        assert_eq!(name.bare, "orders");
        assert_eq!(name.stored.as_str(), "2048/orders");
        assert_eq!(
            name.key(TARGET.dest),
            CollectionKey::from_bare(DatabaseId::new(2048), "orders")
        );
    }

    #[test]
    fn a_name_qualified_for_another_database_is_refused() {
        assert!(TARGET.resolve("7/orders").is_err());
        assert!(TARGET.resolve("orders").is_err());
    }

    #[test]
    fn the_default_database_keeps_bare_names() {
        let target = DatabaseTarget {
            source: DatabaseId::DEFAULT,
            dest: DatabaseId::DEFAULT,
            restore_id: 0,
            restore_cut: i64::MAX,
        };
        let name = target
            .resolve("orders")
            .expect("bare in the default database");
        assert_eq!(name.stored.as_str(), "orders");
    }

    #[test]
    fn a_scoped_key_must_name_the_source_database_and_tenant() {
        let name = TARGET
            .resolve_scoped("1025:7:1025/metrics", 7)
            .expect("scoped key of the source database");
        assert_eq!(name.stored.as_str(), "2048/metrics");
        assert!(TARGET.resolve_scoped("0:7:metrics", 7).is_err());
        assert!(TARGET.resolve_scoped("1025:8:1025/metrics", 7).is_err());
        assert!(TARGET.resolve_scoped("1025:7", 7).is_err());
    }
}
