// SPDX-License-Identifier: BUSL-1.1

//! `MirrorCut` host-side effect: install a source cut's catalog entries, then
//! raise the mirror's cut state.

use nodedb_cluster::MetadataEntry;
use nodedb_types::{DatabaseId, MirrorCutAdvance, MirrorStatus};
use tracing::debug;

use super::types::MetadataCommitApplier;
use crate::control::catalog_entry::{self, CatalogEntry};
use crate::control::security::catalog::SystemCatalog;

/// Whether the database takes another cut. PROMOTE fixed a sealed mirror's
/// last cut, and a database with no origin follows nothing.
fn takes_cuts(catalog: &SystemCatalog, database: DatabaseId) -> crate::Result<bool> {
    let Some(descriptor) = catalog.get_database(database)? else {
        return Ok(false);
    };
    let Some(origin) = descriptor.mirror_origin else {
        return Ok(false);
    };
    Ok(match origin.status {
        MirrorStatus::Bootstrapping { .. }
        | MirrorStatus::Following
        | MirrorStatus::Degraded
        | MirrorStatus::Disconnected
        | MirrorStatus::Failed { .. } => true,
        MirrorStatus::Promoting { .. } | MirrorStatus::Promoted => false,
    })
}

impl MetadataCommitApplier {
    /// Apply one `MirrorCut`.
    ///
    /// The cut state is the last write. A crash before it leaves the row
    /// behind the cut, Raft re-delivers the entry, and the catalog entries
    /// re-apply as their stamped versions allow. A crash after it finds the
    /// row at the cut and changes nothing. Every node computes the next state
    /// from the same row in log order, so each reaches the same verdicts.
    pub(super) async fn apply_mirror_cut(
        &self,
        database: u64,
        advance: &MirrorCutAdvance,
        catalog: &[Vec<u8>],
        raft_index: u64,
    ) -> Result<(), crate::Error> {
        let id = DatabaseId::new(database);
        let store = self.credentials.catalog();
        if !takes_cuts(store, id)? {
            debug!(
                database,
                cut = advance.cut,
                "mirror cut: takes no more cuts"
            );
            return Ok(());
        }
        let current = store.get_mirror_cut_state(id)?.unwrap_or_default();
        let Some(next) = current.advanced(advance) else {
            debug!(
                database,
                cut = advance.cut,
                epoch = advance.epoch,
                ready_cut = current.ready_cut,
                fence_epoch = current.fence_epoch,
                "mirror cut: a former applier's or a replayed entry changes nothing"
            );
            return Ok(());
        };
        if next.ready_cut > current.ready_cut {
            for payload in catalog {
                let ddl = MetadataEntry::CatalogDdl {
                    payload: payload.clone(),
                };
                self.apply_catalog_ddl(&ddl, raft_index).await?;
            }
        }
        let state = MetadataEntry::CatalogDdl {
            payload: catalog_entry::encode(&CatalogEntry::PutMirrorCutState {
                database_id: database,
                state: Box::new(next),
            })?,
        };
        self.apply_catalog_ddl(&state, raft_index).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_cluster::{MetadataApplier, encode_entry};
    use nodedb_types::MirrorGroupPosition;

    use super::super::test_fixture::applier_with_shared_at;
    use super::*;
    use crate::control::mirror::MirrorVerdict;
    use crate::control::mirror::test_fixture::mirror_descriptor;
    use crate::control::security::catalog::database_types::{DatabaseDescriptor, DatabaseStatus};
    use crate::control::state::SharedState;

    const DB: u64 = 1200;

    fn descriptor(status: MirrorStatus) -> DatabaseDescriptor {
        mirror_descriptor(DB, "mirror_db", status)
    }

    fn ddl(entry: &CatalogEntry) -> Vec<u8> {
        encode_entry(&MetadataEntry::CatalogDdl {
            payload: catalog_entry::encode(entry).expect("encode"),
        })
        .expect("encode")
    }

    fn cut(cut: u64, epoch: u64, catalog: Vec<Vec<u8>>) -> Vec<u8> {
        encode_entry(&MetadataEntry::MirrorCut {
            database: DB,
            advance: MirrorCutAdvance {
                cut,
                epoch,
                applied_cut: cut.saturating_sub(1),
                applied_cut_hlc_ns: 5,
                cap_ordinal: 6,
                positions: vec![MirrorGroupPosition {
                    group_id: 1,
                    log_index: cut * 10,
                }],
            },
            catalog,
        })
        .expect("encode")
    }

    async fn following_mirror() -> (
        super::MetadataCommitApplier,
        Arc<SharedState>,
        tempfile::TempDir,
    ) {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let (applier, state) = applier_with_shared_at(tmp.path(), "test.wal");
        let put = ddl(&CatalogEntry::PutDatabase(Box::new(descriptor(
            MirrorStatus::Following,
        ))));
        assert_eq!(applier.apply(&[(1, put)]).await, 1);
        (applier, state, tmp)
    }

    fn mirror_id() -> DatabaseId {
        DatabaseId::new(DB)
    }

    /// The catalog entries and the raised cut land at one log index, and the
    /// watch releases the cut's entries on the node that applied it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_cut_installs_its_catalog_and_raises_ready_at_one_index() {
        let (applier, state, _tmp) = following_mirror().await;
        assert_eq!(
            state.mirror.cuts.verdict(mirror_id(), 1, 3),
            MirrorVerdict::Wait
        );

        let child = DatabaseDescriptor {
            id: DatabaseId::new(DB + 1),
            name: "installed_with_the_cut".to_string(),
            status: DatabaseStatus::Active,
            mirror_origin: None,
            ..descriptor(MirrorStatus::Following)
        };
        let payload = catalog_entry::encode(&CatalogEntry::PutDatabase(Box::new(child.clone())))
            .expect("encode");
        assert_eq!(applier.apply(&[(2, cut(1, 3, vec![payload]))]).await, 2);

        let catalog = state.credentials.catalog();
        assert!(catalog.get_database(child.id).expect("read").is_some());
        let row = catalog
            .get_mirror_cut_state(mirror_id())
            .expect("read")
            .expect("the row");
        assert_eq!(row.ready_cut, 1);
        assert_eq!(row.epoch_of(1), Some(3));
        assert_eq!(row.positions[0].log_index, 10);
        assert_eq!(
            state.mirror.cuts.verdict(mirror_id(), 1, 3),
            MirrorVerdict::Apply
        );
        assert_eq!(
            state.mirror.cuts.verdict(mirror_id(), 1, 2),
            MirrorVerdict::Skip,
            "a former applier's copy of the cut skips"
        );
    }

    /// A former applier's cut is a no-op: its catalog entries install nothing
    /// and its cut stays unready.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_stale_epochs_cut_changes_nothing() {
        let (applier, state, _tmp) = following_mirror().await;
        assert_eq!(applier.apply(&[(2, cut(1, 5, Vec::new()))]).await, 2);

        let child = DatabaseDescriptor {
            id: DatabaseId::new(DB + 2),
            name: "never_installed".to_string(),
            status: DatabaseStatus::Active,
            mirror_origin: None,
            ..descriptor(MirrorStatus::Following)
        };
        let payload = catalog_entry::encode(&CatalogEntry::PutDatabase(Box::new(child.clone())))
            .expect("encode");
        assert_eq!(applier.apply(&[(3, cut(2, 4, vec![payload]))]).await, 3);

        let catalog = state.credentials.catalog();
        assert!(catalog.get_database(child.id).expect("read").is_none());
        let row = catalog
            .get_mirror_cut_state(mirror_id())
            .expect("read")
            .expect("the row");
        assert_eq!(row.ready_cut, 1);
    }

    /// A new applier's fence at `ready_cut` releases a partial cut the former
    /// applier left in the lanes.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_fence_releases_a_former_appliers_partial_cut() {
        let (applier, state, _tmp) = following_mirror().await;
        assert_eq!(applier.apply(&[(2, cut(1, 4, Vec::new()))]).await, 2);
        assert_eq!(
            state.mirror.cuts.verdict(mirror_id(), 2, 4),
            MirrorVerdict::Wait
        );
        assert_eq!(applier.apply(&[(3, cut(1, 5, Vec::new()))]).await, 3);
        assert_eq!(
            state.mirror.cuts.verdict(mirror_id(), 2, 4),
            MirrorVerdict::Skip
        );
        assert_eq!(
            state.mirror.cuts.verdict(mirror_id(), 2, 5),
            MirrorVerdict::Wait
        );
    }

    /// A sealed mirror takes no cut, and its entries past the seal skip.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_sealed_mirror_takes_no_cut() {
        let (applier, state, _tmp) = following_mirror().await;
        assert_eq!(applier.apply(&[(2, cut(1, 4, Vec::new()))]).await, 2);
        let seal = ddl(&CatalogEntry::PutDatabase(Box::new(descriptor(
            MirrorStatus::Promoting { final_cut: 1 },
        ))));
        assert_eq!(applier.apply(&[(3, seal)]).await, 3);
        assert_eq!(applier.apply(&[(4, cut(2, 4, Vec::new()))]).await, 4);

        let row = state
            .credentials
            .catalog()
            .get_mirror_cut_state(mirror_id())
            .expect("read")
            .expect("the row");
        assert_eq!(row.ready_cut, 1);
        assert_eq!(
            state.mirror.cuts.verdict(mirror_id(), 2, 4),
            MirrorVerdict::Skip
        );
    }

    /// A database that follows nothing takes no row.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_database_that_follows_nothing_takes_no_row() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let (applier, state) = applier_with_shared_at(tmp.path(), "test.wal");
        assert_eq!(applier.apply(&[(1, cut(1, 1, Vec::new()))]).await, 1);
        assert!(
            state
                .credentials
                .catalog()
                .get_mirror_cut_state(mirror_id())
                .expect("read")
                .is_none()
        );
    }

    /// A re-delivered entry changes nothing.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_redelivered_cut_changes_nothing() {
        let (applier, state, _tmp) = following_mirror().await;
        let entry = cut(1, 4, Vec::new());
        assert_eq!(applier.apply(&[(2, entry.clone())]).await, 2);
        let before = state
            .credentials
            .catalog()
            .get_mirror_cut_state(mirror_id())
            .expect("read");
        assert_eq!(applier.apply(&[(2, entry)]).await, 2);
        assert_eq!(
            state
                .credentials
                .catalog()
                .get_mirror_cut_state(mirror_id())
                .expect("read"),
            before
        );
    }
}
