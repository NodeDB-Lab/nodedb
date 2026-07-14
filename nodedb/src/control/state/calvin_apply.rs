// SPDX-License-Identifier: BUSL-1.1

//! Sidecar value type for Calvin apply results (RETURNING rows).

use crate::bridge::envelope::Response;

/// The applied Data-Plane result for one completed Calvin transaction, carried
/// via [`SharedState::calvin_apply_results`] from the per-vShard scheduler to
/// the coordinator's completion path.
///
/// A cross-shard COMMIT may legitimately have MANY primary-write participants —
/// e.g. a multi-collection interactive transaction whose collections live on
/// different vShards deposits one plain (affected-count) write per participant.
/// Those coalesce: the sidecar keeps a single applied [`Single`](Self::Single)
/// response for the affected-count / rows the coordinator surfaces (which it
/// discards anyway for a COMMIT tag), and the extra plain-write siblings do not
/// conflict.
///
/// [`Conflict`](Self::Conflict) is recorded ONLY when two participants each carry
/// RETURNING rows — a cross-shard RETURNING union, which is genuinely
/// unsupported. The coordinator then fails the statement loudly rather than
/// returning one shard's partial rows.
///
/// [`SharedState::calvin_apply_results`]: crate::control::state::SharedState::calvin_apply_results
pub enum CalvinApplyResult {
    /// A participant's applied response. `has_returning` is true iff this
    /// participant's slice carried RETURNING rows (a plain affected-count
    /// write is false). Used to detect a genuine cross-shard RETURNING union.
    Single {
        response: Response,
        has_returning: bool,
    },
    /// Two or more RETURNING-bearing participants deposited for one `TxnId` —
    /// a cross-shard RETURNING union, which is unsupported. Drained as a typed
    /// error.
    Conflict,
}
