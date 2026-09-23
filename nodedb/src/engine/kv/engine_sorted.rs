// SPDX-License-Identifier: BUSL-1.1

//! KvEngine methods for sorted index lifecycle and query.
//!
//! Extends `KvEngine` with:
//! - `register_sorted_index()` / `drop_sorted_index()` — DDL
//! - `sorted_index_rank()` / `sorted_index_top_k()` / etc. — query
//!
//! Write-time maintenance is NOT here. `KvEngine::put` / `delete` /
//! `atomic_put` / `tick_expiry` reach `SortedIndexManager::on_put` /
//! `on_delete` directly, alongside the secondary-index update they already do
//! from the same field extraction — one entry point per write path, so there is
//! no second place to edit that turns out to be called by nothing.

use super::engine::KvEngine;
use super::engine_helpers::table_key;
use super::sorted_index::manager::SortedIndexDef;

/// Parameters for [`KvEngine::sorted_index_range`].
#[derive(Debug, Clone, Copy)]
pub struct SortedIndexRangeParams<'a> {
    pub database_id: u64,
    pub tenant_id: u64,
    pub index_name: &'a str,
    pub score_min: Option<&'a [u8]>,
    pub score_max: Option<&'a [u8]>,
    pub now_ms: u64,
}

impl KvEngine {
    /// Register a new sorted index with backfill from existing KV data.
    ///
    /// Scans the hash table for all entries, extracts sort key columns,
    /// and populates the order-statistic tree. Returns backfill count.
    pub fn register_sorted_index(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        def: SortedIndexDef,
    ) -> u32 {
        let tkey = table_key(database_id, tenant_id, collection);
        let now_ms = super::current_ms();

        // Name the collection even when it holds no rows yet: the checkpoint
        // writer recovers a collection's identity from these reverse maps, and
        // an unnamed collection gets no checkpoint file — which would drop this
        // registration from the checkpoint while WAL truncation deleted the
        // record that carries it.
        self.hash_to_tenant.entry(tkey).or_insert(tenant_id);
        self.hash_to_collection
            .entry(tkey)
            .or_insert_with(|| collection.to_string());

        // Collect existing entries from the hash table for backfill.
        let entries: Vec<(Vec<u8>, Vec<u8>)> = self
            .tables
            .get(&tkey)
            .map(|t| {
                let (entries, _) = t.scan(0, usize::MAX, now_ms, None);
                entries
                    .into_iter()
                    .map(|(k, v)| (k.to_vec(), v.to_vec()))
                    .collect()
            })
            .unwrap_or_default();

        self.sorted_indexes
            .register(database_id, tenant_id, def, entries.into_iter())
    }

    /// Drop a sorted index. Returns `true` if it existed.
    pub fn drop_sorted_index(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        index_name: &str,
    ) -> bool {
        self.sorted_indexes.drop(database_id, tenant_id, index_name)
    }

    /// Check if any sorted indexes exist for this tenant/collection.
    pub fn has_sorted_indexes(&self, database_id: u64, tenant_id: u64, collection: &str) -> bool {
        let tkey = table_key(database_id, tenant_id, collection);
        self.sorted_indexes.has_indexes(tkey)
    }

    // ── Query methods ──────────────────────────────────────────────────

    pub fn sorted_index_rank(
        &self,
        database_id: u64,
        tenant_id: u64,
        index_name: &str,
        primary_key: &[u8],
        now_ms: u64,
    ) -> Option<u32> {
        self.sorted_indexes
            .rank(database_id, tenant_id, index_name, primary_key, now_ms)
    }

    pub fn sorted_index_top_k(
        &self,
        database_id: u64,
        tenant_id: u64,
        index_name: &str,
        k: u32,
        now_ms: u64,
    ) -> Option<Vec<(u32, Vec<u8>)>> {
        self.sorted_indexes
            .top_k(database_id, tenant_id, index_name, k, now_ms)
    }

    pub fn sorted_index_range(
        &self,
        params: SortedIndexRangeParams<'_>,
    ) -> Option<Vec<(u32, Vec<u8>)>> {
        let SortedIndexRangeParams {
            database_id,
            tenant_id,
            index_name,
            score_min,
            score_max,
            now_ms,
        } = params;
        self.sorted_indexes.range(
            database_id,
            tenant_id,
            index_name,
            score_min,
            score_max,
            now_ms,
        )
    }

    pub fn sorted_index_count(
        &self,
        database_id: u64,
        tenant_id: u64,
        index_name: &str,
        now_ms: u64,
    ) -> Option<u32> {
        self.sorted_indexes
            .count(database_id, tenant_id, index_name, now_ms)
    }

    pub fn sorted_index_score(
        &self,
        database_id: u64,
        tenant_id: u64,
        index_name: &str,
        primary_key: &[u8],
    ) -> Option<Vec<u8>> {
        self.sorted_indexes
            .score(database_id, tenant_id, index_name, primary_key)
    }

    pub fn sorted_index_def(
        &self,
        database_id: u64,
        tenant_id: u64,
        index_name: &str,
    ) -> Option<&SortedIndexDef> {
        self.sorted_indexes
            .get_def(database_id, tenant_id, index_name)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use crate::engine::kv::sorted_index::key::{SortColumn, SortDirection, SortKeyEncoder};
    use crate::engine::kv::sorted_index::window::WindowConfig;
    use crate::engine::kv::{AtomicKeyCtx, KvPutParams, KvScanParams, admit_any};

    use super::*;

    fn now() -> u64 {
        1_000_000
    }

    fn make_engine() -> KvEngine {
        KvEngine::new(now(), 16, 0.75, 4, 64, 1000, 1024)
    }

    /// Build the full-visibility, no-filter scan params used by the normalizer.
    fn scan_params<'a>(collection: &'a str, count: usize, now_ms: u64) -> KvScanParams<'a> {
        KvScanParams {
            database_id: 0,
            tenant_id: 1,
            collection,
            cursor: &[],
            count,
            now_ms,
            match_pattern: None,
            filter_field: None,
            filter_value: None,
            surrogate_ceiling: None,
        }
    }

    /// An unwindowed leaderboard on `score` DESC, keyed on `player_id`.
    ///
    /// Built inline rather than through the Data Plane's
    /// `build_sorted_index_def`: this is an engine unit test, and the engine does
    /// not depend on the executor that owns that builder.
    fn leaderboard_def(collection: &str, name: &str) -> SortedIndexDef {
        SortedIndexDef {
            name: name.into(),
            collection: collection.into(),
            key_column: "player_id".into(),
            encoder: SortKeyEncoder::new(vec![SortColumn {
                name: "score".into(),
                direction: SortDirection::Desc,
            }]),
            window: WindowConfig::none(),
        }
    }

    /// A leaderboard row whose `score` is a NUMBER, which is what a SQL
    /// `INSERT ... (score INT)` stores and what the sort-key encoders assume.
    /// `mp_obj` above builds string-valued fields, which sort as UTF-8 and would
    /// hide an ordering bug behind lexicographic luck.
    fn mp_scored(player_id: &str, score: i64) -> Vec<u8> {
        nodedb_types::json_to_msgpack(&serde_json::json!({
            "player_id": player_id,
            "score": score,
        }))
        .expect("encode leaderboard row")
    }

    fn put_scored(e: &mut KvEngine, collection: &str, player_id: &str, score: i64) {
        e.put(KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection,
            key: player_id.as_bytes(),
            value: &mp_scored(player_id, score),
            ttl_ms: 0,
            now_ms: now(),
            surrogate: Surrogate::ZERO,
        });
    }

    fn ranked_keys(entries: Option<Vec<(u32, Vec<u8>)>>) -> Vec<String> {
        entries
            .unwrap_or_default()
            .into_iter()
            .map(|(_, key)| String::from_utf8_lossy(&key).into_owned())
            .collect()
    }

    /// Helper: create a MessagePack-encoded JSON object value.
    fn mp_obj(fields: &[(&str, &str)]) -> Vec<u8> {
        let obj: serde_json::Map<String, serde_json::Value> = fields
            .iter()
            .map(|(k, v)| (k.to_string(), serde_json::Value::String(v.to_string())))
            .collect();
        nodedb_types::json_to_msgpack(&serde_json::Value::Object(obj)).unwrap()
    }

    /// Registration must adopt the rows that are already there.
    ///
    /// An index that starts empty and only tracks later writes disagrees with its
    /// own collection from the moment it is created, and nothing in the read path
    /// re-checks the table — `top_k` returns the tree verbatim.
    #[test]
    fn sorted_index_backfills_rows_written_before_registration() {
        let mut e = make_engine();
        let n = now();

        put_scored(&mut e, "players", "p1", 10);
        put_scored(&mut e, "players", "p2", 30);
        put_scored(&mut e, "players", "p3", 20);

        let backfilled = e.register_sorted_index(0, 1, "players", leaderboard_def("players", "lb"));

        assert_eq!(backfilled, 3, "every pre-existing row must be indexed");
        assert_eq!(e.sorted_index_count(0, 1, "lb", n), Some(3));
        assert_eq!(
            ranked_keys(e.sorted_index_top_k(0, 1, "lb", 10, n)),
            vec!["p2", "p3", "p1"],
            "backfill must order by the indexed column, highest first"
        );
    }

    /// Rows written after registration must be tracked, and the index must hold
    /// exactly the collection's rows — no more, no fewer — however they arrived.
    #[test]
    fn sorted_index_holds_the_same_rows_as_the_collection() {
        let mut e = make_engine();
        let n = now();

        put_scored(&mut e, "players", "p1", 10);
        e.register_sorted_index(0, 1, "players", leaderboard_def("players", "lb"));
        put_scored(&mut e, "players", "p2", 30);
        put_scored(&mut e, "players", "p3", 20);

        let mut stored: Vec<String> = Vec::new();
        e.scan_for_each(scan_params("players", usize::MAX, n), |key, _value| {
            stored.push(String::from_utf8_lossy(key).into_owned());
            Ok(())
        })
        .expect("scan the collection");
        stored.sort();

        let mut indexed = ranked_keys(e.sorted_index_top_k(0, 1, "lb", u32::MAX, n));
        indexed.sort();

        assert_eq!(
            indexed, stored,
            "the index must answer with the collection's row set, not a subset"
        );
    }

    /// `INCR` / `CAS` / `GETSET` / `TRANSFER` reach the store through the atomic
    /// write body, not through `put`. Maintaining the index on only one of the two
    /// routes leaves `RANK` / `TOPK` answering from the pre-update score with
    /// nothing to signal it.
    ///
    /// `incr` is the route exercised here because it is the one that genuinely
    /// rewrites the indexed column: on a typed row it re-writes the first numeric
    /// field in place, which is what `KV_INCR` and RESP `ZINCRBY` do to a
    /// leaderboard score. (`getset` and `cas` replace the first STRING field, so
    /// neither can move `score` — they are the wrong shape to test an ordering
    /// change with, not a second version of this case.)
    #[test]
    fn sorted_index_tracks_an_atomic_update() {
        let mut e = make_engine();
        let n = now();

        put_scored(&mut e, "players", "p1", 10);
        put_scored(&mut e, "players", "p2", 30);
        e.register_sorted_index(0, 1, "players", leaderboard_def("players", "lb"));
        assert_eq!(e.sorted_index_rank(0, 1, "lb", b"p1", n), Some(2));

        let updated = e.incr(
            AtomicKeyCtx {
                database_id: 0,
                tenant_id: 1,
                collection: "players",
                key: b"p1",
                now_ms: n,
                surrogate: Surrogate::ZERO,
            },
            89,
            // `ttl_ms == 0` preserves whatever TTL the key already has, so the
            // increment under test is the only thing this write changes.
            0,
            &nodedb_physical::physical_plan::KvCounterShape::Raw,
            &admit_any,
        );
        assert_eq!(
            updated.ok().map(|result| result.value),
            Some(99),
            "p1's score must become 10 + 89"
        );

        assert_eq!(
            ranked_keys(e.sorted_index_top_k(0, 1, "lb", 10, n)),
            vec!["p1", "p2"],
            "the updated score must re-order the leaderboard"
        );
        assert_eq!(e.sorted_index_rank(0, 1, "lb", b"p1", n), Some(1));
        assert_eq!(
            e.sorted_index_count(0, 1, "lb", n),
            Some(2),
            "an update re-keys a row, it does not add one"
        );
    }

    /// A DELETE must take the row out of the index too, or the deleted key keeps
    /// its rank and displaces every live key below it.
    #[test]
    fn sorted_index_tracks_a_delete() {
        let mut e = make_engine();
        let n = now();

        put_scored(&mut e, "players", "p1", 10);
        put_scored(&mut e, "players", "p2", 30);
        e.register_sorted_index(0, 1, "players", leaderboard_def("players", "lb"));

        assert_eq!(e.delete(0, 1, "players", &[b"p2".to_vec()], n), 1);

        assert_eq!(e.sorted_index_rank(0, 1, "lb", b"p2", n), None);
        assert_eq!(e.sorted_index_count(0, 1, "lb", n), Some(1));
        assert_eq!(
            ranked_keys(e.sorted_index_top_k(0, 1, "lb", 10, n)),
            vec!["p1"]
        );
    }

    /// `RANGE(index, lo, hi)` bounds arrive as the leading column's raw value
    /// bytes; the tree is keyed by length-prefixed, direction-complemented
    /// composite keys. Comparing the two spaces directly matches nothing, so the
    /// bounds must be lifted into the key space — including the swap a descending
    /// column forces.
    #[test]
    fn sorted_index_range_selects_by_score() {
        let mut e = make_engine();
        let n = now();

        put_scored(&mut e, "players", "p1", 10);
        put_scored(&mut e, "players", "p2", 30);
        put_scored(&mut e, "players", "p3", 20);
        e.register_sorted_index(0, 1, "players", leaderboard_def("players", "lb"));

        let bound = |v: i64| SortKeyEncoder::encode_i64(v).to_vec();

        let mid = e.sorted_index_range(SortedIndexRangeParams {
            database_id: 0,
            tenant_id: 1,
            index_name: "lb",
            score_min: Some(&bound(15)),
            score_max: Some(&bound(30)),
            now_ms: n,
        });
        let mut keys = ranked_keys(mid);
        keys.sort();
        assert_eq!(
            keys,
            vec!["p2", "p3"],
            "an inclusive [15, 30] window must hold exactly the rows scoring 20 and 30"
        );

        let all = e.sorted_index_range(SortedIndexRangeParams {
            database_id: 0,
            tenant_id: 1,
            index_name: "lb",
            score_min: None,
            score_max: None,
            now_ms: n,
        });
        assert_eq!(
            ranked_keys(all).len(),
            3,
            "an unbounded range must return every indexed row"
        );

        let below = e.sorted_index_range(SortedIndexRangeParams {
            database_id: 0,
            tenant_id: 1,
            index_name: "lb",
            score_min: None,
            score_max: Some(&bound(10)),
            now_ms: n,
        });
        assert_eq!(
            ranked_keys(below),
            vec!["p1"],
            "an upper bound must include the row sitting exactly on it"
        );
    }

    /// The reaper must remove a sorted-index entry along with the row.
    #[test]
    fn sorted_index_cleaned_on_ttl_reap() {
        let mut e = make_engine();
        let n = now();

        // Register before the PUTs: `register_sorted_index` backfills against
        // wall-clock now, which is far past this test's synthetic `now()`.
        e.register_sorted_index(0, 1, "players", leaderboard_def("players", "lb"));

        e.put(KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "players",
            key: b"p1",
            value: &mp_obj(&[("player_id", "p1"), ("score", "200")]),
            ttl_ms: 5000,
            now_ms: n,
            surrogate: Surrogate::ZERO,
        });
        e.put(KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "players",
            key: b"p2",
            value: &mp_obj(&[("player_id", "p2"), ("score", "100")]),
            ttl_ms: 0,
            now_ms: n,
            surrogate: Surrogate::ZERO,
        });

        assert_eq!(e.sorted_index_rank(0, 1, "lb", b"p1", n), Some(1));
        assert_eq!(e.sorted_index_rank(0, 1, "lb", b"p2", n), Some(2));

        let reaped = e.tick_expiry(n + 5000);
        assert_eq!(reaped.len(), 1);

        assert_eq!(
            e.sorted_index_rank(0, 1, "lb", b"p1", n + 5000),
            None,
            "the expired leader must not still hold a rank"
        );
        assert_eq!(
            e.sorted_index_rank(0, 1, "lb", b"p2", n + 5000),
            Some(1),
            "the live player must move up, not stay shifted down by a ghost"
        );
        assert_eq!(
            e.sorted_index_top_k(0, 1, "lb", 10, n + 5000),
            Some(vec![(1, b"p2".to_vec())]),
            "top_k must not return the expired key"
        );
    }

    /// TRUNCATE must take the sorted indexes with the rows.
    ///
    /// They live in their own manager rather than in the `KvIndexSet` that
    /// `truncate` drops, so forgetting them strands the tree. That is the same
    /// hard-wrong-answer as an unreaped expiry, by another route: `rank` / `top_k`
    /// never re-check the table, so a truncated collection would keep serving
    /// ranked keys for rows that no longer exist.
    #[test]
    fn sorted_index_cleaned_on_truncate() {
        let mut e = make_engine();
        let n = now();

        e.register_sorted_index(0, 1, "players", leaderboard_def("players", "lb"));
        e.put(KvPutParams {
            database_id: 0,
            tenant_id: 1,
            collection: "players",
            key: b"p1",
            value: &mp_obj(&[("player_id", "p1"), ("score", "200")]),
            ttl_ms: 0,
            now_ms: n,
            surrogate: Surrogate::ZERO,
        });
        assert_eq!(e.sorted_index_rank(0, 1, "lb", b"p1", n), Some(1));

        assert_eq!(e.truncate(0, 1, "players"), 1);

        assert_eq!(e.total_entries(), 0);
        assert_eq!(
            e.sorted_index_rank(0, 1, "lb", b"p1", n),
            None,
            "a truncated collection must not leave its leaderboard ranking ghosts"
        );
        assert_eq!(
            e.sorted_index_top_k(0, 1, "lb", 10, n),
            None,
            "the sorted index itself must be gone, as the secondary indexes are"
        );
    }
}
