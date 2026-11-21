// SPDX-License-Identifier: BUSL-1.1

//! Splits a run of keys into batches that each fit one RPC frame.
//!
//! A routed assign carries one batch per request. The home leader proposes
//! one batch as one `SurrogateBind` entry. Both travel in one frame, so both
//! split here, against `ASSIGN_SURROGATE_KEY_BUDGET`.

use nodedb_cluster::rpc_codec::ASSIGN_SURROGATE_KEY_OVERHEAD;

/// The bytes `pk` counts against a batch budget.
fn key_cost(pk: &[u8]) -> usize {
    pk.len().saturating_add(ASSIGN_SURROGATE_KEY_OVERHEAD)
}

/// `pks` cut into contiguous batches, in order, each costing at most
/// `budget`.
///
/// A key whose own cost passes `budget` forms a batch alone. An empty `pks`
/// yields no batch.
pub(super) fn key_batches<'s, 'k>(pks: &'s [&'k [u8]], budget: usize) -> Vec<&'s [&'k [u8]]> {
    let mut batches = Vec::new();
    let mut start = 0;
    let mut cost: usize = 0;
    for (at, pk) in pks.iter().enumerate() {
        let next = key_cost(pk);
        if at > start && cost.saturating_add(next) > budget {
            batches.push(&pks[start..at]);
            start = at;
            cost = 0;
        }
        cost = cost.saturating_add(next);
    }
    if start < pks.len() {
        batches.push(&pks[start..]);
    }
    batches
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(lens: &[usize]) -> Vec<Vec<u8>> {
        lens.iter()
            .enumerate()
            .map(|(at, &len)| vec![at as u8; len])
            .collect()
    }

    fn cost_of(batch: &[&[u8]]) -> usize {
        batch.iter().map(|pk| key_cost(pk)).sum()
    }

    #[test]
    fn no_keys_is_no_batch() {
        assert!(key_batches(&[], 1024).is_empty());
    }

    #[test]
    fn keys_under_the_budget_are_one_batch() {
        let owned = keys(&[10, 20, 30]);
        let pks: Vec<&[u8]> = owned.iter().map(Vec::as_slice).collect();
        let batches = key_batches(&pks, 1024);
        assert_eq!(batches, vec![pks.as_slice()]);
    }

    /// A batch over the budget splits into batches that each fit it, and
    /// every key keeps its place.
    #[test]
    fn keys_over_the_budget_split_in_order() {
        let owned = keys(&[40; 10]);
        let pks: Vec<&[u8]> = owned.iter().map(Vec::as_slice).collect();
        let budget = 3 * key_cost(&owned[0]);
        let batches = key_batches(&pks, budget);
        assert_eq!(batches.len(), 4);
        for batch in &batches {
            assert!(cost_of(batch) <= budget);
        }
        let rejoined: Vec<&[u8]> = batches.concat();
        assert_eq!(rejoined, pks);
    }

    /// A key larger than the budget travels alone, and its neighbours still
    /// travel together.
    #[test]
    fn an_oversized_key_forms_a_batch_alone() {
        let owned = keys(&[8, 8, 500, 8, 8]);
        let pks: Vec<&[u8]> = owned.iter().map(Vec::as_slice).collect();
        let batches = key_batches(&pks, 100);
        let lens: Vec<usize> = batches.iter().map(|batch| batch.len()).collect();
        assert_eq!(lens, vec![2, 1, 2]);
        assert_eq!(batches[1][0].len(), 500);
    }
}
