// SPDX-License-Identifier: BUSL-1.1

//! The pre-/post-image pair a single write produces, plus the scope and
//! resolved cross-collection identity one enforcement pass runs against.
//!
//! Every write-path enforcement (materialized sums, hash chain, BALANCED)
//! needs to know not just *what* the row now contains but *how it got there*.
//! Passing a single document plus a separate "is this a delete" flag lets a
//! caller describe a mutation that cannot exist, and — worse — lets a caller
//! that only has the post-image describe an UPDATE as though it were an
//! INSERT. [`RowImages`] makes the shape of the mutation the type itself, so
//! that class of caller mistake stops compiling.

use nodedb_physical::physical_plan::ResolvedSumTarget;

use crate::types::Lsn;

/// The row images a single write produces.
///
/// The variant IS the shape of the mutation. A caller holding only a
/// post-image physically cannot construct [`RowImages::Update`], because that
/// variant demands the pre-image too — which is what stops an update being
/// accounted as an insert. That was the concrete bug this type replaces: the
/// old API took one document and derived one positive delta from it, so a
/// materialized total only ever incremented, and a DELETE never subtracted at
/// all.
///
/// There is deliberately **no** `Move` variant. A write that changes a join
/// key is an [`RowImages::Update`] like any other; the materialized-sum fold
/// derives the two-target split itself from `old_doc[join] != new_doc[join]`,
/// where it has the binding in hand and the comparison means something.
/// Hoisting the move into this type would push it onto the hash chain and
/// BALANCED as well — neither of which knows what a join key is — forcing both
/// to carry a case that is meaningless to them.
///
/// Consumers match exhaustively with no `_ =>` arm. That is the point: adding
/// a fourth mutation shape makes the compiler name every enforcement that must
/// decide what it means, instead of letting a new shape fall silently into a
/// catch-all that treats it like the old ones.
pub(in crate::data::executor) enum RowImages<'a> {
    /// A row that did not exist before this write. Post-image only.
    Insert {
        /// The row as it now stands.
        new_doc: &'a serde_json::Value,
    },
    /// A row that no longer exists after this write. Pre-image only — an
    /// enforcement folding a running total must subtract this, which is
    /// impossible for an API that can only report a post-image.
    Delete {
        /// The row as it stood before this write.
        old_doc: &'a serde_json::Value,
    },
    /// A row that existed before and after. Both images are required, so the
    /// net effect is always the difference between them, never the post-image
    /// counted as if it were new.
    Update {
        /// The row as it stood before this write.
        old_doc: &'a serde_json::Value,
        /// The row as it now stands.
        new_doc: &'a serde_json::Value,
    },
}

/// Scope plus resolved cross-collection identity for one enforcement pass.
pub(in crate::data::executor) struct EnforcementCtx<'a> {
    /// Database the write landed in.
    pub database_id: u64,
    /// Tenant the write landed in.
    pub tid: u64,
    /// Collection the written row belongs to — the *source* collection, not
    /// any target an enforcement may fan out to.
    pub collection: &'a str,
    /// `(target collection, join-key value)` → the surrogate of the target row
    /// that pair identifies, for every target this write may touch (both sides
    /// of a join-key change on an [`RowImages::Update`]).
    ///
    /// The target collection is half the key, not decoration. One source
    /// collection may drive two bindings that read the SAME join column into
    /// DIFFERENT target collections; keyed on the value alone the second
    /// binding's fold would address the first binding's row and both stored
    /// totals would be wrong, with no error raised on either plane.
    ///
    /// This is resolved on the **Control Plane** at plan time and travels on
    /// the plan. The Data Plane never derives it, and must never try to: the
    /// primary-key → surrogate map lives in the catalog redb, which is
    /// Control-Plane state. A Data-Plane-local copy of it would be catalog
    /// state shared across the plane boundary — exactly what the plane rules
    /// forbid — and it would have to be kept coherent with catalog mutations
    /// happening on the other side of the SPSC bridge.
    ///
    /// A target absent from this slice was not resolvable at plan time; that
    /// is a planning outcome to be reported, not a lookup for the Data Plane
    /// to retry locally.
    pub resolved_targets: &'a [ResolvedSumTarget],
    /// Materialized-sum TARGET collections whose delta this write must NOT
    /// apply, because the Control Plane settled it at plan time and appended an
    /// [`ApplyBalanceDelta`](nodedb_physical::physical_plan::DocumentOp::ApplyBalanceDelta)
    /// task of its own, homed on the target's vShard.
    ///
    /// A target that homes elsewhere has no rows on this core, so applying its
    /// delta inside this transaction would write the balance into a store no
    /// reader of the target collection consults — and, once the appended task
    /// runs, count it twice.
    ///
    /// Read off the plan, never re-derived: the Control Plane decided the
    /// deferral when it appended the sibling task, and a second derivation is
    /// free to disagree with the first. Empty for every write with no deferred
    /// binding, which is every write on a collection whose targets are
    /// co-resident and every write on a collection with no binding at all.
    pub deferred_sum_targets: &'a [String],
    /// WAL LSN the Control Plane allocated for this write, or `None` for
    /// writes with no threaded LSN. Enforcements that persist derived state
    /// stamp it with this so replay can tell what it has already absorbed.
    pub wal_lsn: Option<Lsn>,
    /// Who checks the actual final UNIQUE claims of derived target rows.
    pub target_unique: crate::data::executor::enforcement::unique::UniqueJudge,
}
