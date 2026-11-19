// SPDX-License-Identifier: BUSL-1.1

//! One sorted index: its definition, its order-statistic tree, and the reads
//! it answers.
//!
//! The manager holds one of these per registered index. A transaction builds
//! a private one from the collection's base rows with its staged writes
//! folded in. Both answer through the same methods, so a read inside a
//! transaction ranks, counts and windows exactly like an autocommit read.

use super::manager::SortedIndexDef;
use super::tree::OrderStatTree;
use super::windowed_query::{self, SortedIndexRef};

/// A sorted index: definition plus tree.
pub struct SortedIndex {
    pub(super) def: SortedIndexDef,
    pub(super) tree: OrderStatTree,
}

impl std::fmt::Debug for SortedIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SortedIndex")
            .field("name", &self.def.name)
            .field("collection", &self.def.collection)
            .field("count", &self.tree.count())
            .finish()
    }
}

impl SortedIndex {
    /// Build an index over `rows`, `(primary_key_bytes, value_bytes)` pairs.
    ///
    /// A row whose sort column is missing or NULL is left out. Returns the index and the
    /// number of rows it holds.
    pub fn build(
        def: SortedIndexDef,
        rows: impl Iterator<Item = (Vec<u8>, Vec<u8>)>,
    ) -> (Self, u32) {
        let mut tree = OrderStatTree::new();
        let mut indexed = 0u32;
        for (pk_bytes, value_bytes) in rows {
            if let Some(sort_key) = extract_sort_key_from_value(&def, &value_bytes) {
                tree.insert(sort_key, pk_bytes);
                indexed += 1;
            }
        }
        (Self { def, tree }, indexed)
    }

    /// The definition this index was built from.
    pub fn def(&self) -> &SortedIndexDef {
        &self.def
    }

    fn index_ref(&self) -> SortedIndexRef<'_> {
        SortedIndexRef {
            def: &self.def,
            tree: &self.tree,
        }
    }

    /// The 1-based rank of `primary_key`.
    ///
    /// A windowed index counts only the entries inside the current window.
    pub fn rank(&self, primary_key: &[u8], now_ms: u64) -> Option<u32> {
        if self.def.window.is_unwindowed() {
            return self.tree.rank(primary_key);
        }
        windowed_query::windowed_rank(&self.index_ref(), primary_key, now_ms)
    }

    /// The top `k` entries as `(rank, primary_key)` pairs.
    pub fn top_k(&self, k: u32, now_ms: u64) -> Vec<(u32, Vec<u8>)> {
        if self.def.window.is_unwindowed() {
            return self
                .tree
                .top_k(k)
                .into_iter()
                .enumerate()
                .map(|(i, (_, pk))| (i as u32 + 1, pk.to_vec()))
                .collect();
        }
        windowed_query::windowed_top_k(&self.index_ref(), k, now_ms)
    }

    /// The entries in a score range as `(rank, primary_key)` pairs.
    ///
    /// `score_min` and `score_max` are [`value`](super::value) encodings of
    /// the index's LEADING sort column, not encoded tree keys. The caller names a score. Only the index's own
    /// encoder knows the framing and direction that turn it into a bound the
    /// tree compares against.
    pub fn range(
        &self,
        score_min: Option<&[u8]>,
        score_max: Option<&[u8]>,
        now_ms: u64,
    ) -> Vec<(u32, Vec<u8>)> {
        let (lower, upper) = self
            .def
            .encoder
            .first_column_range_bounds(score_min, score_max);
        let entries = self.tree.range(lower.as_deref(), upper.as_deref());

        if self.def.window.is_unwindowed() {
            return entries
                .into_iter()
                .filter_map(|(_, pk)| {
                    let rank = self.tree.rank(pk)?;
                    Some((rank, pk.to_vec()))
                })
                .collect();
        }
        windowed_query::windowed_range(&self.index_ref(), &entries, now_ms)
    }

    /// The number of entries, inside the current window for a windowed index.
    pub fn count(&self, now_ms: u64) -> u32 {
        if self.def.window.is_unwindowed() {
            return self.tree.count();
        }
        windowed_query::windowed_count(&self.index_ref(), now_ms)
    }

    /// The sort key of `primary_key` (ZSCORE equivalent).
    pub fn score(&self, primary_key: &[u8]) -> Option<Vec<u8>> {
        self.tree.get_sort_key(primary_key).map(|s| s.to_vec())
    }
}

/// The sort key of a MessagePack-encoded KV value. `None` when the value is
/// not a map, or a sort column is missing or NULL.
pub(super) fn extract_sort_key_from_value(
    def: &SortedIndexDef,
    value_bytes: &[u8],
) -> Option<Vec<u8>> {
    let doc: serde_json::Value = nodedb_types::json_from_msgpack(value_bytes).ok()?;
    def.encoder.key_for_document(&doc)
}

#[cfg(test)]
mod tests {
    use super::super::key::SortKeyEncoder;
    use super::super::key::{SortColumn, SortDirection};
    use super::super::value::{encode_float, encode_int};
    use super::super::window::WindowConfig;
    use super::*;

    /// 2024-05-02 15:36:40 UTC, a Thursday.
    const NOW_MS: u64 = 1_714_664_200_000;
    /// 2024-05-02 00:00:00 UTC.
    const DAY_START_MS: u64 = 1_714_608_000_000;
    /// Monday 2024-04-29 00:00:00 UTC.
    const WEEK_START_MS: u64 = 1_714_348_800_000;
    /// 2024-05-01 00:00:00 UTC.
    const MONTH_START_MS: u64 = 1_714_521_600_000;

    fn def(window: WindowConfig, ts_direction: SortDirection) -> SortedIndexDef {
        SortedIndexDef {
            name: "lb".into(),
            collection: "scores".into(),
            key_column: "player".into(),
            encoder: SortKeyEncoder::new(vec![
                SortColumn {
                    name: "score".into(),
                    direction: SortDirection::Desc,
                },
                SortColumn {
                    name: "ts".into(),
                    direction: ts_direction,
                },
            ]),
            window,
        }
    }

    /// A row with `ts` set to `ts`, or with no `ts` field when `None`.
    fn row(player: &str, score: i64, ts: Option<serde_json::Value>) -> (Vec<u8>, Vec<u8>) {
        let mut doc = serde_json::json!({ "player": player, "score": score });
        if let Some(ts) = ts {
            doc["ts"] = ts;
        }
        let value = nodedb_types::json_to_msgpack(&doc).expect("encode row");
        (player.as_bytes().to_vec(), value)
    }

    fn ms(ts: u64) -> Option<serde_json::Value> {
        Some(serde_json::json!(ts))
    }

    fn build(def: SortedIndexDef, rows: Vec<(Vec<u8>, Vec<u8>)>) -> SortedIndex {
        SortedIndex::build(def, rows.into_iter()).0
    }

    fn pks(entries: &[(u32, Vec<u8>)]) -> Vec<(u32, &str)> {
        entries
            .iter()
            .map(|(rank, pk)| (*rank, std::str::from_utf8(pk).expect("utf-8 pk")))
            .collect()
    }

    /// `old` has the top score but a timestamp before the window start.
    /// `fresh` and `late` are inside the window. Every windowed read must
    /// hide `old` and rank the other two as if `old` did not exist.
    fn assert_hides_old_keeps_fresh(idx: &SortedIndex, now_ms: u64) {
        assert_eq!(idx.count(now_ms), 2);
        assert_eq!(idx.rank(b"old", now_ms), None);
        assert_eq!(idx.rank(b"fresh", now_ms), Some(1));
        assert_eq!(idx.rank(b"late", now_ms), Some(2));
        assert_eq!(pks(&idx.top_k(10, now_ms)), vec![(1, "fresh"), (2, "late")]);
        assert_eq!(pks(&idx.top_k(1, now_ms)), vec![(1, "fresh")]);
        assert_eq!(
            pks(&idx.range(None, None, now_ms)),
            vec![(1, "fresh"), (2, "late")]
        );
        let score_200 = encode_int(200);
        let score_300 = encode_int(300);
        assert_eq!(
            pks(&idx.range(Some(&score_200), Some(&score_300), now_ms)),
            vec![(1, "fresh")]
        );
    }

    fn window_rows(start_ms: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
        vec![
            row("old", 300, ms(start_ms - 1)),
            row("fresh", 200, ms(start_ms)),
            row("late", 100, ms(NOW_MS)),
        ]
    }

    #[test]
    fn daily_window_hides_entries_before_today_utc_start() {
        let idx = build(
            def(WindowConfig::daily("ts"), SortDirection::Asc),
            window_rows(DAY_START_MS),
        );
        assert_hides_old_keeps_fresh(&idx, NOW_MS);
    }

    #[test]
    fn daily_window_hides_entries_from_days_ago() {
        let idx = build(
            def(WindowConfig::daily("ts"), SortDirection::Asc),
            vec![
                row("old", 300, ms(DAY_START_MS - 30 * 86_400_000)),
                row("fresh", 200, ms(DAY_START_MS + 1)),
                row("late", 100, ms(NOW_MS)),
            ],
        );
        assert_hides_old_keeps_fresh(&idx, NOW_MS);
    }

    #[test]
    fn weekly_window_starts_at_monday_utc() {
        let idx = build(
            def(WindowConfig::weekly("ts"), SortDirection::Asc),
            window_rows(WEEK_START_MS),
        );
        assert_hides_old_keeps_fresh(&idx, NOW_MS);
    }

    #[test]
    fn monthly_window_starts_at_the_first_of_the_month_utc() {
        let idx = build(
            def(WindowConfig::monthly("ts"), SortDirection::Asc),
            window_rows(MONTH_START_MS),
        );
        assert_hides_old_keeps_fresh(&idx, NOW_MS);
    }

    #[test]
    fn desc_timestamp_column_windows_the_same_as_asc() {
        let idx = build(
            def(WindowConfig::daily("ts"), SortDirection::Desc),
            window_rows(DAY_START_MS),
        );
        assert_hides_old_keeps_fresh(&idx, NOW_MS);
    }

    #[test]
    fn float_timestamp_column_windows_by_its_value() {
        let start = DAY_START_MS as f64;
        let idx = build(
            def(WindowConfig::daily("ts"), SortDirection::Asc),
            vec![
                row("old", 300, Some(serde_json::json!(start - 0.5))),
                row("fresh", 200, Some(serde_json::json!(start + 0.5))),
                row("late", 100, Some(serde_json::json!(NOW_MS as f64 + 0.25))),
            ],
        );
        assert_hides_old_keeps_fresh(&idx, NOW_MS);
    }

    #[test]
    fn float_timestamp_column_with_desc_direction_windows_by_its_value() {
        let start = DAY_START_MS as f64;
        let idx = build(
            def(WindowConfig::daily("ts"), SortDirection::Desc),
            vec![
                row("old", 300, Some(serde_json::json!(start - 0.5))),
                row("fresh", 200, Some(serde_json::json!(start + 0.5))),
                row("late", 100, Some(serde_json::json!(NOW_MS as f64 + 0.25))),
            ],
        );
        assert_hides_old_keeps_fresh(&idx, NOW_MS);
    }

    #[test]
    fn iso_8601_string_timestamp_column_windows_by_its_instant() {
        let idx = build(
            def(WindowConfig::daily("ts"), SortDirection::Asc),
            vec![
                row("old", 300, Some(serde_json::json!("2024-05-01T23:59:59Z"))),
                row(
                    "fresh",
                    200,
                    Some(serde_json::json!("2024-05-02T00:00:00Z")),
                ),
                row("late", 100, Some(serde_json::json!("2024-05-02T15:00:00Z"))),
            ],
        );
        assert_hides_old_keeps_fresh(&idx, NOW_MS);
    }

    #[test]
    fn custom_window_shows_nothing_when_now_is_outside_it() {
        let (start, end) = (DAY_START_MS, DAY_START_MS + 3_600_000);
        let idx = build(
            def(WindowConfig::custom("ts", start, end), SortDirection::Asc),
            vec![
                row("a", 300, ms(start)),
                row("b", 200, ms(start + 1)),
                row("c", 100, ms(end + 1)),
            ],
        );
        for now_ms in [start - 1, end, end + 86_400_000] {
            assert_eq!(idx.count(now_ms), 0);
            assert!(idx.top_k(10, now_ms).is_empty());
            assert!(idx.range(None, None, now_ms).is_empty());
            for pk in [&b"a"[..], &b"b"[..], &b"c"[..]] {
                assert_eq!(idx.rank(pk, now_ms), None);
            }
        }
    }

    #[test]
    fn custom_window_shows_only_entries_inside_its_range() {
        let (start, end) = (DAY_START_MS, DAY_START_MS + 3_600_000);
        let idx = build(
            def(WindowConfig::custom("ts", start, end), SortDirection::Asc),
            vec![
                row("before", 400, ms(start - 1)),
                row("first", 300, ms(start)),
                row("last", 200, ms(end - 1)),
                row("after", 100, ms(end)),
            ],
        );
        let now_ms = start + 60_000;
        assert_eq!(idx.count(now_ms), 2);
        assert_eq!(idx.rank(b"before", now_ms), None);
        assert_eq!(idx.rank(b"first", now_ms), Some(1));
        assert_eq!(idx.rank(b"last", now_ms), Some(2));
        assert_eq!(idx.rank(b"after", now_ms), None);
        assert_eq!(pks(&idx.top_k(10, now_ms)), vec![(1, "first"), (2, "last")]);
        assert_eq!(
            pks(&idx.range(None, None, now_ms)),
            vec![(1, "first"), (2, "last")]
        );
    }

    #[test]
    fn entry_with_a_null_timestamp_is_outside_every_window() {
        let idx = build(
            def(WindowConfig::daily("ts"), SortDirection::Asc),
            vec![
                row("null_ts", 300, Some(serde_json::Value::Null)),
                row("fresh", 200, ms(DAY_START_MS)),
            ],
        );
        assert_eq!(idx.count(NOW_MS), 1);
        assert_eq!(idx.rank(b"null_ts", NOW_MS), None);
        assert_eq!(idx.rank(b"fresh", NOW_MS), Some(1));
        assert_eq!(pks(&idx.top_k(10, NOW_MS)), vec![(1, "fresh")]);
        assert_eq!(pks(&idx.range(None, None, NOW_MS)), vec![(1, "fresh")]);
    }

    #[test]
    fn entry_without_a_timestamp_field_is_not_indexed() {
        let (idx, indexed) = SortedIndex::build(
            def(WindowConfig::daily("ts"), SortDirection::Asc),
            vec![row("no_ts", 300, None), row("fresh", 200, ms(DAY_START_MS))].into_iter(),
        );
        assert_eq!(indexed, 1);
        assert_eq!(idx.count(NOW_MS), 1);
        assert_eq!(idx.rank(b"no_ts", NOW_MS), None);
        assert_eq!(idx.rank(b"fresh", NOW_MS), Some(1));
    }

    #[test]
    fn top_zero_of_a_windowed_index_is_empty() {
        let idx = build(
            def(WindowConfig::daily("ts"), SortDirection::Asc),
            window_rows(DAY_START_MS),
        );
        assert!(idx.top_k(0, NOW_MS).is_empty());
    }

    #[test]
    fn unwindowed_index_shows_every_entry() {
        let idx = build(
            def(WindowConfig::none(), SortDirection::Asc),
            window_rows(DAY_START_MS),
        );
        assert_eq!(idx.count(NOW_MS), 3);
        assert_eq!(idx.rank(b"old", NOW_MS), Some(1));
    }

    /// A row keyed by backfill and the same row keyed by write maintenance
    /// must get one sort key, for every value kind.
    #[test]
    fn backfill_and_write_path_build_identical_keys_for_every_value_kind() {
        use super::super::manager::SortedIndexManager;

        let kinds = [
            serde_json::json!(null),
            serde_json::json!(true),
            serde_json::json!(false),
            serde_json::json!(11),
            serde_json::json!(-11),
            serde_json::json!(u64::MAX),
            serde_json::json!(10.5),
            serde_json::json!(-0.0),
            serde_json::json!(""),
            serde_json::json!("a\u{0}b"),
            serde_json::json!([1, "two"]),
            serde_json::json!({"k": [null]}),
        ];
        let rows: Vec<(Vec<u8>, Vec<u8>)> = kinds
            .iter()
            .enumerate()
            .map(|(i, score)| {
                let doc = serde_json::json!({ "score": score, "ts": NOW_MS });
                let value = nodedb_types::json_to_msgpack(&doc).expect("encode row");
                (format!("pk{i}").into_bytes(), value)
            })
            .collect();

        let table = super::super::super::engine_helpers::table_key(0, 1, "scores");
        let unwindowed = || def(WindowConfig::none(), SortDirection::Desc);

        let mut backfilled = SortedIndexManager::new();
        let indexed = backfilled.register(0, 1, unwindowed(), rows.clone().into_iter());
        assert_eq!(indexed as usize, kinds.len(), "every value kind is indexed");

        let mut written = SortedIndexManager::new();
        written.register(0, 1, unwindowed(), std::iter::empty());
        for (pk, value) in &rows {
            let doc = super::super::super::engine_helpers::decode_document(value);
            written.on_put(table, pk, doc.as_ref());
        }

        for (pk, _) in &rows {
            let backfill_key = backfilled.score(0, 1, "lb", pk);
            assert!(backfill_key.is_some());
            assert_eq!(backfill_key, written.score(0, 1, "lb", pk));
        }
    }

    /// The reported case: 11 must sort above 10.5 in one column.
    #[test]
    fn integer_and_float_scores_rank_by_numeric_value() {
        let rows = |scores: &[(&str, serde_json::Value)]| -> Vec<(Vec<u8>, Vec<u8>)> {
            scores
                .iter()
                .map(|(pk, score)| {
                    let doc = serde_json::json!({ "score": score, "ts": NOW_MS });
                    let value = nodedb_types::json_to_msgpack(&doc).expect("encode row");
                    (pk.as_bytes().to_vec(), value)
                })
                .collect()
        };
        let scores = [
            ("eleven", serde_json::json!(11)),
            ("ten_half", serde_json::json!(10.5)),
            ("ten", serde_json::json!(10)),
            ("big_int", serde_json::json!((1i64 << 53) + 1)),
            ("big_float", serde_json::json!((1i64 << 53) as f64)),
        ];

        let mut desc = def(WindowConfig::none(), SortDirection::Asc);
        desc.encoder = SortKeyEncoder::new(vec![SortColumn {
            name: "score".into(),
            direction: SortDirection::Desc,
        }]);
        let idx = build(desc, rows(&scores[..]));
        assert_eq!(
            pks(&idx.top_k(10, NOW_MS)),
            vec![
                (1, "big_int"),
                (2, "big_float"),
                (3, "eleven"),
                (4, "ten_half"),
                (5, "ten")
            ]
        );

        let mut asc = def(WindowConfig::none(), SortDirection::Asc);
        asc.encoder = SortKeyEncoder::new(vec![SortColumn {
            name: "score".into(),
            direction: SortDirection::Asc,
        }]);
        let idx = build(asc, rows(&scores[..]));
        assert_eq!(
            pks(&idx.top_k(3, NOW_MS)),
            vec![(1, "ten"), (2, "ten_half"), (3, "eleven")]
        );

        // An inclusive [10.5, 11] range holds exactly 10.5 and 11.
        let entries = idx.range(Some(&encode_float(10.5)), Some(&encode_int(11)), NOW_MS);
        let mut in_range = pks(&entries);
        in_range.sort();
        assert_eq!(in_range, vec![(2, "ten_half"), (3, "eleven")]);
    }
}
