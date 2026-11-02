// SPDX-License-Identifier: BUSL-1.1

//! The merge rule of restored ended versions into a key's stored versions.
//!
//! A key's versions are a set keyed by `sys_from`, and their intervals never
//! overlap: the live row covers `[sys_from, ∞)` and each ended version its
//! `[sys_from, sys_to)`. A restored ended version merges when its interval
//! overlaps no stored interval and no other restored one. A restored
//! version equal to the stored version at the same `sys_from` is that
//! version again (a re-restore) and installs nothing. Any other overlap
//! refuses the whole restore, and nothing is written.

use super::KvEngine;
use crate::engine::kv::engine_helpers::table_key;
use crate::engine::kv::history::HistoryVersion;

/// A restored ended version of a KV key overlaps a version the key stores,
/// or another restored version, and is not the same version.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "KV restore into '{collection}' refused: key {key} gets the ended version \
     [{restored_from}, {restored_to}), which overlaps the version [{held_from}, {held_to}) it \
     holds; a key's versions never overlap, so restore into a key whose history does not \
     overlap the backup's, or remove the key first"
)]
pub struct KvHistoryOverlap {
    pub collection: String,
    /// The key, as lossy UTF-8.
    pub key: String,
    pub restored_from: i64,
    pub restored_to: i64,
    pub held_from: i64,
    /// `i64::MAX` for the live row, which no version ends yet.
    pub held_to: i64,
}

/// Whether `[a_from, a_to)` and `[b_from, b_to)` share an ordinal.
fn overlaps(a_from: i64, a_to: i64, b_from: i64, b_to: i64) -> bool {
    a_from < b_to && b_from < a_to
}

impl KvEngine {
    /// The restored ended versions of `key` that the merge installs: every
    /// version of `versions` the key does not already store. `Err` when one
    /// overlaps a stored or another restored version, and is not the stored
    /// version at its `sys_from`. Reads only.
    pub fn merge_key_history(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        key: &[u8],
        versions: Vec<HistoryVersion>,
    ) -> Result<Vec<HistoryVersion>, KvHistoryOverlap> {
        let table = self
            .tables
            .get(&table_key(database_id, tenant_id, collection));
        let held: &[HistoryVersion] = table.map_or(&[], |t| t.history().versions_of(key));
        let live = table
            .and_then(|t| t.find_entry(key))
            .map(|entry| entry.sys_from);
        let refuse = |restored: &HistoryVersion, held_from: i64, held_to: i64| KvHistoryOverlap {
            collection: collection.to_owned(),
            key: String::from_utf8_lossy(key).into_owned(),
            restored_from: restored.sys_from,
            restored_to: restored.sys_to,
            held_from,
            held_to,
        };

        let mut fresh: Vec<HistoryVersion> = Vec::with_capacity(versions.len());
        for version in versions {
            // A zero-width version is visible at no ordinal: the store never
            // keeps one.
            if version.sys_to <= version.sys_from {
                continue;
            }
            if let Some(same) = held.iter().find(|h| h.sys_from == version.sys_from) {
                if *same == version {
                    continue;
                }
                return Err(refuse(&version, same.sys_from, same.sys_to));
            }
            if let Some(clash) = held
                .iter()
                .find(|h| overlaps(version.sys_from, version.sys_to, h.sys_from, h.sys_to))
            {
                return Err(refuse(&version, clash.sys_from, clash.sys_to));
            }
            if let Some(start) = live
                && overlaps(version.sys_from, version.sys_to, start, i64::MAX)
            {
                return Err(refuse(&version, start, i64::MAX));
            }
            if let Some(clash) = fresh
                .iter()
                .find(|f| overlaps(version.sys_from, version.sys_to, f.sys_from, f.sys_to))
            {
                return Err(refuse(&version, clash.sys_from, clash.sys_to));
            }
            fresh.push(version);
        }
        Ok(fresh)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use super::*;

    const NOW: u64 = 1_000_000;

    fn engine() -> KvEngine {
        KvEngine::new(NOW, 16, 0.75, 4, 64, 1000, 1024)
    }

    fn version(sys_from: i64, sys_to: i64, value: &[u8]) -> HistoryVersion {
        HistoryVersion {
            sys_from,
            sys_to,
            value: value.to_vec(),
            expire_at_ms: 0,
            surrogate: Surrogate::new(3),
        }
    }

    fn history(e: &KvEngine) -> Vec<(i64, i64)> {
        e.key_history(0, 1, "c", b"k")
            .iter()
            .map(|v| (v.sys_from, v.sys_to))
            .collect()
    }

    #[test]
    fn disjoint_versions_merge_and_a_re_restore_installs_nothing() {
        let mut e = engine();
        e.restore_key_history(0, 1, "c", b"k", vec![version(10, 20, b"a")])
            .expect("merge");
        e.restore_key_history(
            0,
            1,
            "c",
            b"k",
            vec![version(10, 20, b"a"), version(30, 40, b"b")],
        )
        .expect("merge");
        assert_eq!(history(&e), vec![(10, 20), (30, 40)]);
    }

    #[test]
    fn an_overlapping_version_refuses_the_whole_restore() {
        let mut e = engine();
        e.restore_key_history(0, 1, "c", b"k", vec![version(10, 20, b"a")])
            .expect("merge");
        let refused = e
            .restore_key_history(
                0,
                1,
                "c",
                b"k",
                vec![version(50, 60, b"x"), version(15, 25, b"y")],
            )
            .expect_err("overlap");
        assert_eq!((refused.held_from, refused.held_to), (10, 20));
        assert_eq!(refused.key, "k");
        assert_eq!(history(&e), vec![(10, 20)], "nothing is written");
    }

    #[test]
    fn a_different_version_at_the_same_start_is_refused() {
        let mut e = engine();
        e.restore_key_history(0, 1, "c", b"k", vec![version(10, 20, b"a")])
            .expect("merge");
        assert!(
            e.restore_key_history(0, 1, "c", b"k", vec![version(10, 20, b"other")])
                .is_err()
        );
        assert!(
            e.restore_key_history(0, 1, "c", b"k", vec![version(10, 15, b"a")])
                .is_err(),
            "a shorter interval at the same start is another version"
        );
    }

    #[test]
    fn a_version_overlapping_the_live_row_or_another_restored_one_is_refused() {
        let mut e = engine();
        let scope = e.open_write_scope(Some(100));
        e.put(crate::engine::kv::KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "c",
            key: b"k",
            value: b"live",
            ttl_ms: 0,
            now_ms: NOW,
            surrogate: Surrogate::new(3),
        })
        .expect("put");
        e.close_write_scope(scope);
        let live = e
            .restore_key_history(0, 1, "c", b"k", vec![version(90, 110, b"a")])
            .expect_err("overlaps the live row");
        assert_eq!((live.held_from, live.held_to), (100, i64::MAX));
        assert!(
            e.restore_key_history(
                0,
                1,
                "c",
                b"k",
                vec![version(10, 30, b"a"), version(20, 40, b"b")]
            )
            .is_err()
        );
        e.restore_key_history(0, 1, "c", b"k", vec![version(90, 100, b"a")])
            .expect("ends where the live row starts");
    }
}
