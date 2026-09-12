// SPDX-License-Identifier: BUSL-1.1

//! The one call every point / upsert write path makes to run its
//! image-folding enforcement.
//!
//! [`funnel::run_write_enforcement`] takes decoded documents. Every handler
//! that reaches it holds bytes instead, in one of two encodings that decode
//! differently and fail differently:
//!
//! * a **submitted** body is MessagePack for every storage mode (a strict
//!   collection encodes its Binary Tuple on the way to disk), and a body with
//!   no readable fields carries no column any binding can read — so it folds to
//!   nothing rather than failing the write;
//! * a **stored** body is whatever the collection actually persists, so it
//!   needs the collection's own format, and one that will not decode is
//!   corruption: treating an unreadable pre-image as "no pre-image" turns an
//!   UPDATE into an INSERT and credits a target with the row's whole new value
//!   on top of the contribution it already holds.
//!
//! Encoding the distinction in [`ImageBody`] is what keeps each handler from
//! re-deciding it, and keeps the "which of these is a hard error" answer in one
//! place instead of eight.
//!
//! # Nothing is decoded for a collection that declares nothing
//!
//! Folding a write's images means DECODING its stored pre-image, and a stored
//! body is only guaranteed to be a readable document for a collection that
//! declares what its columns mean. A caller that decodes unconditionally fails
//! every write to a constraint-free collection carrying an opaque body. So the
//! whole hook short-circuits on
//! [`EnforcementOptions::has_image_enforcement`](nodedb_physical::physical_plan::EnforcementOptions::has_image_enforcement).

use redb::WriteTransaction;

use nodedb_physical::physical_plan::ResolvedSumTarget;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::enforcement::funnel::{self, WriteEnforcementOutcome};
use crate::data::executor::enforcement::images::{EnforcementCtx, RowImages};
use crate::data::executor::enforcement::materialized_sum::apply::TargetWrite;
use crate::data::executor::handlers::document::read::decode::decode_scanned_document;
use crate::types::{DatabaseId, Lsn, TenantId};

/// Where one image's bytes came from, which is what decides how they decode.
pub(in crate::data::executor) enum ImageBody<'a> {
    /// The body the caller SUBMITTED — MessagePack for every storage mode.
    Submitted(&'a [u8]),
    /// The body as STORED — a Binary Tuple on a strict collection.
    Stored(&'a [u8]),
}

/// The mutation one write performed, with each image tagged by its encoding.
///
/// Mirrors [`RowImages`] one level down: the variant IS the shape of the
/// mutation, so a caller holding only a post-image cannot describe an update.
pub(in crate::data::executor) enum WriteImages<'a> {
    /// A row that did not exist before this write.
    Insert { new: ImageBody<'a> },
    /// A row that existed before and after.
    Update {
        old: ImageBody<'a>,
        new: ImageBody<'a>,
    },
    /// A row that no longer exists after this write.
    Delete { old: ImageBody<'a> },
}

/// The mutation shape, kept apart from the decoded images so it survives an
/// image that turns out to carry no readable document.
#[derive(Clone, Copy)]
enum Shape {
    Insert,
    Update,
    Delete,
}

/// Scope plus resolved cross-collection identity for one hook call.
pub(in crate::data::executor) struct HookCtx<'a> {
    pub database_id: u64,
    pub tid: u64,
    /// The SOURCE collection the row was written to.
    pub collection: &'a str,
    /// `(target collection, join-key value)` → target row surrogate, resolved
    /// on the Control Plane at plan time and carried on the plan. Never derived
    /// here. See [`EnforcementCtx::resolved_targets`] for why the target
    /// collection is half the key.
    pub resolved_targets: &'a [ResolvedSumTarget],
    /// Materialized-sum TARGET collections whose delta the Control Plane
    /// deferred onto its own `ApplyBalanceDelta` task — see
    /// [`EnforcementCtx::deferred_sum_targets`]. Empty for every op whose plan
    /// carries no deferral slot.
    pub deferred_sum_targets: &'a [String],
    pub wal_lsn: Option<Lsn>,
}

impl HookCtx<'_> {
    fn config_key(&self) -> (DatabaseId, TenantId, String) {
        (
            DatabaseId::new(self.database_id),
            TenantId::new(self.tid),
            self.collection.to_string(),
        )
    }
}

/// Whether this collection declares enforcement that folds a write's images.
///
/// A caller uses this to skip work it would otherwise pay for on every write —
/// most usefully the pre-write read of the prior row.
pub(in crate::data::executor) fn folds_images(core: &CoreLoop, ctx: &HookCtx<'_>) -> bool {
    core.doc_configs
        .get(&ctx.config_key())
        .is_some_and(|config| config.enforcement.has_image_enforcement())
}

/// Run image-folding enforcement for one write, inside the caller's transaction.
///
/// `txn` is the caller's: every derived target write lands in it, so the source
/// row and everything its constraints implied commit or roll back as one unit.
/// On `Err` the caller must drop `txn` without committing — and, when the error
/// arrives after `apply_point_put` already ran, reverse that call's in-memory
/// side effects via
/// [`abort_after_apply`](crate::data::executor::enforcement::chain_guard::abort_after_apply).
pub(in crate::data::executor) fn run(
    core: &mut CoreLoop,
    txn: &WriteTransaction,
    ctx: &HookCtx<'_>,
    images: WriteImages<'_>,
) -> crate::Result<WriteEnforcementOutcome> {
    if !folds_images(core, ctx) {
        return Ok(WriteEnforcementOutcome::default());
    }

    // The shape is carried separately from the decoded images and never
    // re-derived from which of them came back `Some`. An UPDATE whose incoming
    // body carries no readable document would otherwise present as
    // "pre-image only" — indistinguishable from a DELETE, which subtracts the
    // row's whole contribution from a total the row still contributes to.
    let (shape, old_doc, new_doc) = match images {
        WriteImages::Insert { new } => (Shape::Insert, None, decode(core, ctx, new)?),
        WriteImages::Update { old, new } => (
            Shape::Update,
            decode(core, ctx, old)?,
            decode(core, ctx, new)?,
        ),
        WriteImages::Delete { old } => (Shape::Delete, decode(core, ctx, old)?, None),
    };

    let row_images = match (shape, old_doc.as_ref(), new_doc.as_ref()) {
        (Shape::Insert, _, Some(new_doc)) => RowImages::Insert { new_doc },
        (Shape::Delete, Some(old_doc), _) => RowImages::Delete { old_doc },
        (Shape::Update, Some(old_doc), Some(new_doc)) => RowImages::Update { old_doc, new_doc },
        // An image the caller declared present did not decode into a document.
        // It carries no column any binding or BALANCED definition can read, so
        // the write folds to nothing rather than to a mutation of a different
        // shape.
        (Shape::Insert, _, None) | (Shape::Delete, None, _) | (Shape::Update, _, _) => {
            return Ok(WriteEnforcementOutcome::default());
        }
    };

    funnel::run_write_enforcement(
        core,
        txn,
        EnforcementCtx {
            database_id: ctx.database_id,
            tid: ctx.tid,
            collection: ctx.collection,
            resolved_targets: ctx.resolved_targets,
            deferred_sum_targets: ctx.deferred_sum_targets,
            wal_lsn: ctx.wal_lsn,
        },
        row_images,
    )
}

/// Durable redo entries for the derived target rows one write updated.
///
/// A derived write lands in a DIFFERENT collection from the statement's own,
/// and no WAL record names it: the statement's redo describes the source row
/// only. Without an entry per target, a WAL-only restart replays the source
/// rows and leaves every total as it stood BEFORE the statement — the stored
/// balance and the `SUM(...)` over the source rows would disagree, which is
/// precisely what the constraint exists to prevent.
///
/// `collection` is therefore always `Some(target)`: the redo record must name
/// the target collection, and it homes to that collection's vShard rather than
/// the statement's.
pub(in crate::data::executor) fn target_write_set(
    targets: &[TargetWrite],
) -> Vec<crate::bridge::envelope::WriteSetEntry> {
    targets
        .iter()
        .map(|target| crate::bridge::envelope::WriteSetEntry {
            surrogate: target.surrogate.as_u32(),
            identity: target.identity.clone(),
            is_delete: false,
            value: target.body.clone(),
            collection: Some(target.collection.clone()),
        })
        .collect()
}

/// Decode one image in the encoding its origin implies.
fn decode(
    core: &CoreLoop,
    ctx: &HookCtx<'_>,
    body: ImageBody<'_>,
) -> crate::Result<Option<serde_json::Value>> {
    match body {
        // Unreadable means "no column to fold", not a failure: the incoming
        // body may legitimately carry no document at all.
        ImageBody::Submitted(bytes) => Ok(doc_format::decode_document(bytes).ok()),
        // Here the collection HAS declared constraints over its columns, so a
        // stored row that will not decode is corruption. Failing is the only
        // outcome that does not silently mis-account the write.
        ImageBody::Stored(bytes) => {
            let format = core.sparse_body_format(
                DatabaseId::new(ctx.database_id),
                TenantId::new(ctx.tid),
                ctx.collection,
            );
            decode_scanned_document(bytes, format.as_format_ref()).map(Some)
        }
    }
}
