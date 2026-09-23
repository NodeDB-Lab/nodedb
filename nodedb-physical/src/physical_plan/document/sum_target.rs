// SPDX-License-Identifier: Apache-2.0

//! The identity of one materialized-sum target row, as the Control Plane
//! resolved it.
//!
//! A binding is `source.join_column -> target.target_column`, and one source
//! collection may drive SEVERAL bindings. Two of them can share a join column
//! and still point at different target collections — `entries.account_id` into
//! both `accounts.balance` and `audit_totals.balance`, say. The join VALUE those
//! two bindings read off a row is then identical, and it names two different
//! rows in two different collections.
//!
//! So a resolution keyed by the join value alone cannot express the answer. It
//! resolves the first binding, sees the value already present, skips the second,
//! and every consumer that looks the value up gets the FIRST binding's target
//! row — which the read-modify-write then writes the second binding's balance
//! into. No error is raised on either plane: both totals are simply wrong.
//!
//! The key is therefore the PAIR, and every producer and every consumer keys on
//! the pair.

use nodedb_types::Surrogate;

/// The `(target collection, join value)` pair one resolution entry answers for.
///
/// `target_collection` is the catalog name the binding carries — never
/// db-qualified — because that is the form both planes compare bindings by.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct SumTargetKey {
    /// TARGET collection of the binding this key belongs to.
    pub target_collection: String,
    /// Join-key value a source row carries for that binding's join column.
    pub join_value: String,
}

impl SumTargetKey {
    /// The key naming `target_collection`'s row for `join_value`.
    pub fn new(target_collection: impl Into<String>, join_value: impl Into<String>) -> Self {
        Self {
            target_collection: target_collection.into(),
            join_value: join_value.into(),
        }
    }
}

/// One resolved materialized-sum target: which row a
/// `(target collection, join value)` pair names.
///
/// Produced on the Control Plane, where the primary-key → surrogate map lives,
/// and consumed on the Data Plane, which never resolves anything itself.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ResolvedSumTarget {
    /// TARGET collection of the binding this entry was resolved for, or `None`
    /// for an entry lifted out of a replicated record written before the target
    /// collection travelled on the wire.
    ///
    /// `None` matches ANY binding of the source by join value alone — exactly
    /// what such a record meant when it was written, and the only reading of it
    /// that keeps a node able to replay its own committed log across the
    /// upgrade. Nothing produces `None` at plan time.
    pub target_collection: Option<String>,
    /// Join-key value that names the target row.
    pub join_value: String,
    /// The target row's stable cross-engine identity.
    pub surrogate: Surrogate,
}

impl ResolvedSumTarget {
    /// The entry a binding's resolution produces.
    pub fn new(
        target_collection: impl Into<String>,
        join_value: impl Into<String>,
        surrogate: Surrogate,
    ) -> Self {
        Self {
            target_collection: Some(target_collection.into()),
            join_value: join_value.into(),
            surrogate,
        }
    }

    /// An entry that names no target collection — see
    /// [`ResolvedSumTarget::target_collection`]. Only the replicated-record
    /// decoder builds these.
    pub fn untargeted(join_value: impl Into<String>, surrogate: Surrogate) -> Self {
        Self {
            target_collection: None,
            join_value: join_value.into(),
            surrogate,
        }
    }

    /// Whether this entry answers for `target_collection`'s `join_value`.
    pub fn addresses(&self, target_collection: &str, join_value: &str) -> bool {
        self.join_value == join_value
            && self
                .target_collection
                .as_deref()
                .is_none_or(|declared| declared == target_collection)
    }

    /// Whether this entry answers for `key`.
    pub fn matches_key(&self, key: &SumTargetKey) -> bool {
        self.addresses(&key.target_collection, &key.join_value)
    }
}

/// The materialized-sum resolution one committed transaction's writes to one
/// SOURCE collection fold into their targets.
///
/// A transaction redo record carries source post-images only. Every replica
/// applying it folds the source rows into their target rows, so the target
/// identities travel with the redo, keyed by source collection.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct RedoSumTargets {
    /// SOURCE collection the transaction wrote.
    pub collection: String,
    /// Every target row the transaction's writes to `collection` resolved.
    pub resolved: Vec<ResolvedSumTarget>,
    /// TARGET collections whose delta travels on its own `ApplyBalanceDelta`
    /// task, so the fold skips them.
    pub deferred: Vec<String>,
}

/// The surrogate `resolved` binds `target_collection`'s `join_value` to.
///
/// The one lookup both planes use, so the Control Plane's "this one travels on
/// its own task" and the Data Plane's "nobody else is applying this one" can
/// never read the table differently.
pub fn resolved_sum_surrogate(
    resolved: &[ResolvedSumTarget],
    target_collection: &str,
    join_value: &str,
) -> Option<Surrogate> {
    resolved
        .iter()
        .find(|entry| entry.addresses(target_collection, join_value))
        .map(|entry| entry.surrogate)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two bindings of one source that share a join column resolve to two
    /// SEPARATE entries, and each lookup returns its own target's row.
    #[test]
    fn one_join_value_resolves_per_target_collection() {
        let resolved = vec![
            ResolvedSumTarget::new("accounts", "acc-1", Surrogate::new(500)),
            ResolvedSumTarget::new("audit_totals", "acc-1", Surrogate::new(900)),
        ];
        assert_eq!(
            resolved_sum_surrogate(&resolved, "accounts", "acc-1"),
            Some(Surrogate::new(500))
        );
        assert_eq!(
            resolved_sum_surrogate(&resolved, "audit_totals", "acc-1"),
            Some(Surrogate::new(900))
        );
    }

    /// A target the resolution does not cover reports nothing, even when some
    /// OTHER target carries the same join value.
    #[test]
    fn a_foreign_target_is_not_answered_by_a_matching_join_value() {
        let resolved = vec![ResolvedSumTarget::new(
            "accounts",
            "acc-1",
            Surrogate::new(500),
        )];
        assert_eq!(
            resolved_sum_surrogate(&resolved, "audit_totals", "acc-1"),
            None
        );
    }

    /// An entry decoded from a pre-widening replicated record answers for every
    /// binding, which is what that record meant when it was written.
    #[test]
    fn an_untargeted_entry_answers_any_target() {
        let resolved = vec![ResolvedSumTarget::untargeted("acc-1", Surrogate::new(500))];
        assert_eq!(
            resolved_sum_surrogate(&resolved, "accounts", "acc-1"),
            Some(Surrogate::new(500))
        );
        assert_eq!(
            resolved_sum_surrogate(&resolved, "audit_totals", "acc-1"),
            Some(Surrogate::new(500))
        );
    }
}
