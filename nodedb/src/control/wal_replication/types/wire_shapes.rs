// SPDX-License-Identifier: BUSL-1.1

//! Small wire-format types embedded inside [`super::ReplicatedWrite`].

// ── Replicated write envelope ───────────────────────────────────────

/// One edge of an `EdgePutBatch` / `EdgeDeleteBatch` in the cross-node wire
/// shape. Endpoint surrogates are bare `u32`, not `Surrogate`. Followers bind
/// both verbatim on apply (never re-allocate).
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ReplicatedBatchEdge {
    pub collection: String,
    pub src_id: String,
    pub label: String,
    pub dst_id: String,
    /// Leader-assigned global surrogate for the source node (binding key =
    /// `src_id.as_bytes()`).
    pub src_surrogate: u32,
    /// Leader-assigned global surrogate for the destination node (binding key =
    /// `dst_id.as_bytes()`).
    pub dst_surrogate: u32,
}

/// The fields of one `ApplyBalanceDelta`, borrowed from the plan or the wire
/// entry. Shared by the encode and decode helpers so both name every field.
#[derive(Debug, Clone, Copy)]
pub struct BalanceDeltaFields<'a> {
    /// TARGET collection, db-qualified.
    pub collection: &'a str,
    /// Target row's storage key — hex-encoded surrogate.
    pub document_id: &'a str,
    /// Target row's global identity.
    pub surrogate: u32,
    /// The balance column this delta moves.
    pub column: &'a str,
    /// Signed amount as an exact decimal string.
    pub delta: &'a str,
    /// Binding's join column, for the typed not-found error on apply.
    pub join_column: &'a str,
    /// Join value that resolved to `surrogate`.
    pub join_value: &'a str,
    /// The TARGET collection's declared `PRIMARY KEY` column, when it has one.
    pub declared_primary_key: Option<&'a str>,
}

/// One entry of a write's materialized-sum resolution: which target row a
/// binding's `(target collection, join value)` pair names. Supersedes the
/// `(join_value, surrogate)` pairs in `*_sum_targets`, which can't tell apart
/// two bindings sharing a join column into different targets.
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
pub struct ReplicatedSumTarget {
    /// TARGET collection of the binding this entry was resolved for, as the
    /// catalog names it.
    pub target_collection: String,
    /// Join-key value naming the target row.
    pub join_value: String,
    /// The target row's surrogate.
    pub surrogate: u32,
}

/// One row of a `ColumnarBulkDmlResolved` write: carries the resolved row
/// image, not anything a follower would re-derive. `new_row_msgpack` is
/// empty for a delete row.
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
pub struct ColumnarResolvedRow {
    /// MessagePack-encoded primary key value.
    pub pk_msgpack: Vec<u8>,
    /// MessagePack-encoded full post-image row. Empty for a delete.
    pub new_row_msgpack: Vec<u8>,
}

/// One mutation of a `KvResolvedWrite`. Surrogate is a bare `u32`; decode binds
/// it through the surrogate assigner. `precondition` is a drift check, not a
/// CAS condition: `None` means the key must be ABSENT, `Some(bytes)` that it
/// must hold exactly those stored bytes.
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
pub enum KvResolvedMutationWire {
    Put {
        collection: String,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_ms: u64,
        /// Absolute expiry instant resolved by the proposing node, `0` for
        /// none. No applying node may re-derive it from its own clock.
        expire_at_ms: u64,
        surrogate: u32,
        precondition: Option<Vec<u8>>,
    },
    Delete {
        collection: String,
        key: Vec<u8>,
        precondition: Option<Vec<u8>>,
    },
    Expire {
        collection: String,
        key: Vec<u8>,
        ttl_ms: u64,
        /// See `ReplicatedWrite::KvExpire::resolved_now_ms`. Per-mutation here
        /// because one resolved write can carry several.
        resolved_now_ms: u64,
        precondition: Option<Vec<u8>>,
    },
    Persist {
        collection: String,
        key: Vec<u8>,
        precondition: Option<Vec<u8>>,
    },
}

/// One mutation of a `DocumentResolvedWrite`. Surrogate is a bare `u32`,
/// bound via the surrogate assigner on decode. `value` is the pre-encode
/// MessagePack body; a strict collection encodes its Binary Tuple on the way
/// to disk. `precondition` is a drift check on raw stored bytes: `None` means
/// the row must be ABSENT, `Some(bytes)` an exact match.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum DocumentResolvedMutationWire {
    Put {
        collection: String,
        document_id: String,
        surrogate: u32,
        value: Vec<u8>,
        precondition: Option<Vec<u8>>,
        /// `(target collection, join value)` -> target surrogate, resolved by
        /// the proposing node; no applying node can resolve it locally.
        resolved_sum_targets: Vec<ReplicatedSumTarget>,
    },
    Delete {
        collection: String,
        document_id: String,
        surrogate: u32,
        precondition: Option<Vec<u8>>,
        /// See `Put::resolved_sum_targets`.
        resolved_sum_targets: Vec<ReplicatedSumTarget>,
    },
}

/// Whether a `ConstraintChange` installs (`Set`) or removes (`Drop`) a
/// collection's constraint set on every replica.
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
pub enum ConstraintChangeOp {
    Set,
    Drop,
}
