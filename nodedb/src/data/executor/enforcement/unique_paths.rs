// SPDX-License-Identifier: BUSL-1.1

//! The UNIQUE paths a probe judges on one core.
//!
//! A write's UNIQUE paths come from its judged config: the pinned config of
//! its proposer, else the core's own. A probe reads index entries, and a
//! core holds entries only for the paths its maintained set keeps. The set
//! follows the data log, so every replica holds the same set at one write.
//!
//! Each pinned UNIQUE path takes one outcome:
//!
//! * Maintained by a matching unique owner: the probe judges it.
//! * `Building` and not so maintained: skipped. The build's own duplicate
//!   scan judges the rows it finds.
//! * `Ready` and not so maintained: the write is refused with
//!   `StalePinnedSchema`. Either the core retired the index before the
//!   write, or its proposer pinned an index whose build this core has not
//!   applied. The refusal is final, since every replica reaches it at one
//!   position.
//!
//! A matching unique owner keeps the path's entries in the pinned path's
//! shape: the same array flag, case folding and predicate. The maintained
//! spec's `unique` names whether a unique owner of that shape is held. A
//! declared field and the top-level mode are never unique, so they never
//! match. The two specs then differ only in name and build state, so the
//! probe reads the stored entries by the pinned spec.

use nodedb_physical::physical_plan::RegisteredIndexState;

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::maintained_indexes::MaintainedIndexes;
use crate::engine::document::store::IndexPath;

/// The collection whose pinned UNIQUE paths one probe judges.
#[derive(Debug, Clone, Copy)]
pub(in crate::data::executor) struct UniqueTarget<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
}

/// The pinned UNIQUE paths of `target` the probe judges, in pinned order.
/// Fails with `StalePinnedSchema` when a `Ready` UNIQUE path has no
/// matching unique owner in `maintained`.
pub(in crate::data::executor) fn judged_unique_paths<'p>(
    maintained: &MaintainedIndexes,
    target: UniqueTarget<'_>,
    pinned: &'p [IndexPath],
) -> crate::Result<Vec<&'p IndexPath>> {
    let mut judged = Vec::new();
    for path in pinned.iter().filter(|path| path.unique) {
        let kept = maintained.path(
            target.database_id,
            target.tid,
            target.collection,
            &path.path,
        );
        if kept.is_some_and(|kept| matches_unique(&kept, path)) {
            judged.push(path);
            continue;
        }
        match path.state {
            RegisteredIndexState::Building => {}
            RegisteredIndexState::Ready => return Err(stale_unique(target.collection, path)),
        }
    }
    Ok(judged)
}

/// Whether `kept`, the maintained spec of a path, holds unique entries in
/// the shape of the pinned UNIQUE path `pinned`.
fn matches_unique(kept: &IndexPath, pinned: &IndexPath) -> bool {
    kept.unique
        && kept.is_array == pinned.is_array
        && kept.case_insensitive == pinned.case_insensitive
        && kept.predicate == pinned.predicate
}

/// The refusal of a write pinned to the `Ready` UNIQUE path `path`, which
/// this core does not maintain as unique.
fn stale_unique(collection: &str, path: &IndexPath) -> crate::Error {
    crate::Error::DataPlane(ErrorCode::StalePinnedSchema {
        detail: format!(
            "collection '{collection}': the pinned unique index '{}' on '{}' is ready, but this \
             core maintains no unique index of its shape on that path",
            path.name, path.path
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::core_loop::doc_config_key::DocConfigKey;
    use crate::engine::document::predicate::IndexPredicate;
    use crate::types::{DatabaseId, TenantId};

    const TARGET: UniqueTarget<'static> = UniqueTarget {
        database_id: 0,
        tid: 1,
        collection: "codes",
    };

    fn key() -> DocConfigKey {
        (DatabaseId::new(0), TenantId::new(1), "codes".to_string())
    }

    fn unique(state: RegisteredIndexState) -> IndexPath {
        IndexPath {
            unique: true,
            state,
            ..IndexPath::new("code")
        }
    }

    fn maintaining(paths: Vec<IndexPath>) -> MaintainedIndexes {
        let mut maintained = MaintainedIndexes::default();
        maintained.insert_for_test(key(), paths);
        maintained
    }

    fn is_stale(verdict: &crate::Result<Vec<&IndexPath>>) -> bool {
        matches!(
            verdict,
            Err(crate::Error::DataPlane(ErrorCode::StalePinnedSchema { detail }))
                if detail.contains("codes") && detail.contains("code")
        )
    }

    #[test]
    fn a_building_path_this_core_does_not_maintain_is_skipped() {
        let pinned = vec![unique(RegisteredIndexState::Building)];
        let judged = judged_unique_paths(&MaintainedIndexes::default(), TARGET, &pinned);
        assert!(judged.expect("no refusal").is_empty());
    }

    #[test]
    fn a_ready_path_this_core_does_not_maintain_is_refused() {
        let pinned = vec![unique(RegisteredIndexState::Ready)];
        let judged = judged_unique_paths(&MaintainedIndexes::default(), TARGET, &pinned);
        assert!(is_stale(&judged), "{judged:?}");
    }

    #[test]
    fn a_ready_path_maintained_as_unique_is_judged() {
        let pinned = vec![IndexPath::new("other"), unique(RegisteredIndexState::Ready)];
        let maintained = maintaining(vec![unique(RegisteredIndexState::Ready)]);
        let judged = judged_unique_paths(&maintained, TARGET, &pinned).expect("judged");
        assert_eq!(judged, vec![&pinned[1]]);
    }

    #[test]
    fn a_building_path_maintained_as_unique_is_judged() {
        let pinned = vec![unique(RegisteredIndexState::Building)];
        let maintained = maintaining(vec![unique(RegisteredIndexState::Ready)]);
        let judged = judged_unique_paths(&maintained, TARGET, &pinned).expect("judged");
        assert_eq!(judged, vec![&pinned[0]]);
    }

    #[test]
    fn a_path_maintained_only_by_a_plain_owner_does_not_count() {
        let maintained = maintaining(vec![IndexPath::new("code")]);
        let ready = vec![unique(RegisteredIndexState::Ready)];
        let judged = judged_unique_paths(&maintained, TARGET, &ready);
        assert!(is_stale(&judged), "{judged:?}");
        let building = vec![unique(RegisteredIndexState::Building)];
        let judged = judged_unique_paths(&maintained, TARGET, &building);
        assert!(judged.expect("no refusal").is_empty());
    }

    #[test]
    fn a_case_folding_mismatch_does_not_count() {
        let maintained = maintaining(vec![IndexPath {
            case_insensitive: true,
            ..unique(RegisteredIndexState::Ready)
        }]);
        let ready = vec![unique(RegisteredIndexState::Ready)];
        let judged = judged_unique_paths(&maintained, TARGET, &ready);
        assert!(is_stale(&judged), "{judged:?}");
        let building = vec![unique(RegisteredIndexState::Building)];
        let judged = judged_unique_paths(&maintained, TARGET, &building);
        assert!(judged.expect("no refusal").is_empty());
    }

    #[test]
    fn a_predicate_or_array_mismatch_does_not_count() {
        let partial = IndexPath {
            predicate: Some(IndexPredicate::parse("active = true").expect("predicate")),
            ..unique(RegisteredIndexState::Ready)
        };
        let array = IndexPath {
            is_array: true,
            ..unique(RegisteredIndexState::Ready)
        };
        let ready = vec![unique(RegisteredIndexState::Ready)];
        for kept in [partial, array] {
            let judged = judged_unique_paths(&maintaining(vec![kept]), TARGET, &ready);
            assert!(is_stale(&judged), "{judged:?}");
        }
    }

    #[test]
    fn another_collection_maintaining_the_path_does_not_count() {
        let maintained = maintaining(vec![unique(RegisteredIndexState::Ready)]);
        let other = UniqueTarget {
            collection: "others",
            ..TARGET
        };
        let pinned = vec![unique(RegisteredIndexState::Ready)];
        let judged = judged_unique_paths(&maintained, other, &pinned);
        assert!(
            matches!(
                &judged,
                Err(crate::Error::DataPlane(ErrorCode::StalePinnedSchema { detail }))
                    if detail.contains("others")
            ),
            "{judged:?}"
        );
    }
}
