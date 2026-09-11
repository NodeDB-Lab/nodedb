// SPDX-License-Identifier: BUSL-1.1

//! The shared "dedupe, resolve, push" step every materialized-sum and
//! period-lock resolution loop performs once per candidate `(target, value)`
//! pair.

use std::collections::{HashMap, HashSet};

use nodedb_physical::physical_plan::ResolvedSumTarget;
use nodedb_types::Surrogate;

/// The resolution a statement builds up, one entry per DISTINCT
/// `(target, value)` pair.
///
/// A page or scan that touches the same target row many times resolves it
/// once: `seen` answers the dedupe in O(1) per candidate, and `entries` keeps
/// first-resolution order, which is the order the plan carries.
#[derive(Default)]
pub struct ResolvedTargets {
    entries: Vec<ResolvedSumTarget>,
    /// Every value a lookup already ran for, per target. Values the lookup
    /// left unresolved count too, so a repeated value never re-runs it. Keyed
    /// target-first so the membership check borrows both halves.
    seen: HashMap<String, HashSet<String>>,
}

impl ResolvedTargets {
    pub fn new() -> Self {
        Self::default()
    }

    /// The resolved entries in first-resolution order.
    pub fn into_vec(self) -> Vec<ResolvedSumTarget> {
        self.entries
    }

    /// Resolve one `(target, value)` pair, skipping the push when `lookup`
    /// names no row.
    ///
    /// `lookup` differs by caller: a period-lock lookup can name no reference
    /// row (`Ok(None)` skips the push, leaving the value unresolved for
    /// `check_period_lock` to refuse), while a materialized-sum lookup always
    /// resolves or fails outright — its `Err` propagates through the `?`
    /// below before `Ok(None)` could ever apply.
    pub async fn resolve(
        &mut self,
        target: &str,
        value: String,
        lookup: impl AsyncFnOnce(&str) -> crate::Result<Option<Surrogate>>,
    ) -> crate::Result<()> {
        if self
            .seen
            .get(target)
            .is_some_and(|values| values.contains(value.as_str()))
        {
            return Ok(());
        }
        let surrogate = lookup(&value).await?;
        match self.seen.get_mut(target) {
            Some(values) => {
                values.insert(value.clone());
            }
            None => {
                self.seen
                    .insert(target.to_string(), HashSet::from([value.clone()]));
            }
        }
        if let Some(surrogate) = surrogate {
            self.entries
                .push(ResolvedSumTarget::new(target, value, surrogate));
        }
        Ok(())
    }
}
