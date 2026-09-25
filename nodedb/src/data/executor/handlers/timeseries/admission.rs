// SPDX-License-Identifier: BUSL-1.1

//! Record-boundary admission checks for timeseries ingest.
//!
//! Every check here answers the same question BEFORE the first row of a WAL
//! record is written: can the memtable take this record WHOLE? A "no" means the
//! caller must flush first — never partway through.
//!
//! That ordering is what makes the partition stamp honest.
//! `flush_ts_collection` labels the partition it writes with the records it
//! holds (`timeseries_checkpoint::stamp`), and boot replay skips exactly those.
//! A record is noted only once all of its rows landed. A flush that fires
//! between two rows of record L writes a partition holding SOME of L whose
//! stamp does not name L, so replay appends every one of L's rows a second
//! time — on an append-only engine nothing masks that.

use std::collections::HashSet;

use crate::engine::timeseries::columnar_memtable::{ColumnType, ColumnarMemtable};
use crate::engine::timeseries::ilp::IlpLine;
use crate::engine::timeseries::ilp_ingest;

/// Whether every symbol column's dictionary can absorb this batch's distinct
/// values without hitting `max_tag_cardinality`.
///
/// Cardinality exhaustion is the reason a mid-record stop is otherwise
/// unavoidable, and the reason it cannot be handled by retrying a suffix:
/// `SymbolDictionary::resolve` fails only for values NOT already in the
/// dictionary, so once a dictionary is full the lines carrying new tag values
/// fail while lines reusing existing values keep succeeding. The failures are
/// INTERLEAVED through the batch, not a suffix of it, so the count of accepted
/// lines does not identify the consumed prefix.
///
/// Answering this up front lets the caller flush — which resets the
/// dictionaries — and then take the record whole, preserving exactly the
/// cardinality progress today's behaviour makes with none of the mid-record
/// flush.
///
/// A `false` here does not promise the batch then fits: a single batch with
/// more distinct values than `max_tag_cardinality` cannot fit in any
/// generation, and its excess rows are honestly reported as `rejected`. What
/// it does guarantee is that the decision is made once, before any row lands.
///
/// ## Cost
///
/// The common case is O(1) per symbol column. A batch of `n` lines can add at
/// most `n` new values to a column, so a column whose dictionary plus `n`
/// still fits under the ceiling cannot possibly overflow and is skipped
/// without touching a line. That bound is exact, not a heuristic — it never
/// skips a column that would have overflowed — and it retires every batch that
/// is not genuinely near the ceiling, which is all of them until a collection
/// approaches its cardinality limit.
///
/// Only a column that could overflow pays the full pass: `O(lines)` borrowed
/// `&str` dictionary probes plus one `HashSet<&str>` sized to that column's
/// NEW distinct values (not to the batch). Nothing is allocated per line and
/// no value is copied — the set borrows out of the parsed lines. The pass
/// re-does probes the ingest would do anyway, without the row writes; that
/// repeat is the price of knowing the answer before the first write instead of
/// after a partial one.
pub(super) fn has_tag_headroom(
    memtable: &ColumnarMemtable,
    lines: &[IlpLine<'_>],
    max_tag_cardinality: u32,
) -> bool {
    let ceiling = max_tag_cardinality as usize;
    for (idx, (col_name, col_type)) in memtable.schema().columns.iter().enumerate() {
        if *col_type != ColumnType::Symbol {
            continue;
        }
        // A symbol column with no dictionary cannot be resolved at all; that is
        // a per-row error `ingest_row` reports on its own, not a headroom
        // question, so it is not this check's to answer.
        let Some(dict) = memtable.symbol_dict(idx) else {
            continue;
        };
        // Upper bound first: one line contributes at most one new value, so if
        // the dictionary plus the whole batch still fits, no pass can find an
        // overflow. Keeps the gate off the hot path for every batch that is not
        // near the ceiling.
        if dict.len().saturating_add(lines.len()) <= ceiling {
            continue;
        }
        let mut fresh: HashSet<&str> = HashSet::new();
        for line in lines {
            let value = symbol_value(line, col_name);
            if dict.get_id(value).is_none() {
                fresh.insert(value);
            }
        }
        if dict.len().saturating_add(fresh.len()) > ceiling {
            return false;
        }
    }
    true
}

/// Whether some symbol column in `symbol_columns` receives more distinct
/// values from `lines` than `max_tag_cardinality` allows.
///
/// Such a batch cannot fit in any memtable generation. A flush resets the
/// dictionaries, and some of its rows are rejected all the same. A batch no
/// longer than the ceiling cannot trip it, so that case costs nothing.
pub(super) fn exceeds_tag_ceiling(
    symbol_columns: &[String],
    lines: &[IlpLine<'_>],
    max_tag_cardinality: u32,
) -> bool {
    let ceiling = max_tag_cardinality as usize;
    if lines.len() <= ceiling {
        return false;
    }
    symbol_columns.iter().any(|col_name| {
        let distinct: HashSet<&str> = lines
            .iter()
            .map(|line| symbol_value(line, col_name))
            .collect();
        distinct.len() > ceiling
    })
}

/// The value `ingest_batch_with_lvc` would resolve for symbol column
/// `col_name` on `line`.
///
/// Mirrors that function's resolution order exactly — tag first, then string
/// field, else the empty string. Any divergence would make the headroom check
/// answer about a different set of values than the ingest actually inserts.
fn symbol_value<'l, 'a: 'l>(line: &'l IlpLine<'a>, col_name: &str) -> &'l str {
    let tag: Option<&'l str> = line
        .tags
        .iter()
        .find(|(key, _)| key.as_ref() == col_name)
        .map(|(_, value)| value.as_ref());
    tag.or_else(|| ilp_ingest::find_field_str_ref(&line.fields, col_name))
        .unwrap_or_default()
}
