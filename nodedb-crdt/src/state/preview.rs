// SPDX-License-Identifier: Apache-2.0

//! Non-mutating, bounded previews of remote CRDT deltas.
//!
//! A preview forks the authoritative document, imports only into that fork,
//! and derives the exact write-set and target post-image from the fork. This
//! lets callers authorize a proposed merge without accepting unbounded bytes,
//! pending causal dependencies, or an unverified sender-declared target.

use std::collections::BTreeMap;

use loro::LoroValue;
use sha2::{Digest, Sha256};

use crate::error::{CrdtError, Result};
use crate::loro_value::loro_to_value;

use super::core::CrdtState;
use super::document_cell::DocumentCell;
use super::import_admission::{CrdtImportLimits, admit_import};
use super::write_set::collect_write_set;

/// Maximum raw CRDT delta bytes accepted by the default authoritative preview.
pub const DEFAULT_MAX_DELTA_BYTES: usize = 1024 * 1024;
/// Maximum operations physically encoded in any previewed delta, including
/// operations already known to the authoritative state.
pub const DEFAULT_MAX_ENCODED_DELTA_OPS: usize = 100_000;
/// Maximum canonical MessagePack post-image bytes emitted by the default preview.
pub const DEFAULT_MAX_POST_IMAGE_BYTES: usize = 1024 * 1024;

/// Explicit resource limits for [`CrdtState::preview_delta`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CrdtDeltaPreviewLimits {
    /// Maximum raw delta bytes accepted before a fork/import is attempted.
    pub max_delta_bytes: usize,
    /// Maximum new operations a delta contributes beyond the fork's existing
    /// oplog version vector.
    pub max_imported_ops: usize,
    /// Maximum rows a non-idempotent delta may write.
    pub max_write_set_entries: usize,
    /// Maximum canonical MessagePack post-image bytes.
    pub max_post_image_bytes: usize,
}

impl Default for CrdtDeltaPreviewLimits {
    fn default() -> Self {
        Self {
            max_delta_bytes: DEFAULT_MAX_DELTA_BYTES,
            max_imported_ops: 10_000,
            max_write_set_entries: 1,
            max_post_image_bytes: DEFAULT_MAX_POST_IMAGE_BYTES,
        }
    }
}

/// Exact, bounded result of importing a delta into an isolated Loro fork.
#[derive(Debug, Clone, PartialEq)]
pub struct CrdtDeltaPreview {
    /// Deterministically sorted rows actually changed by the imported delta.
    pub write_set: Vec<(String, String)>,
    /// Current target-row image after applying the delta, or `None` when absent.
    pub post_image: Option<nodedb_types::Value>,
    /// Deterministic canonical MessagePack representation of [`post_image`](Self::post_image).
    pub post_image_msgpack: Vec<u8>,
    /// SHA-256 over the post-import sorted `(peer, counter)` frontier entries.
    pub resulting_frontier_digest: [u8; 32],
    /// Number of new operations contributed beyond the fork's prior oplog
    /// version vector.
    pub imported_ops: usize,
    /// Number of operations the delta encoded that this document already knew.
    ///
    /// Loro drops these during merge. A resync trims its replay prefix and then
    /// advances (`imported_ops > 0`); a delta that trims entirely
    /// (`imported_ops == 0` with `trimmed_ops > 0`) contributed nothing at all,
    /// which is both what an idempotent replay looks like and what a peer-id
    /// collision looks like. Counting it is what makes the second case visible.
    pub trimmed_ops: usize,
}

impl CrdtState {
    /// Preview one remote delta without mutating this authoritative state.
    ///
    /// A zero-operation, already-known delta is a valid idempotent preview:
    /// its write-set is empty and the post-image is the authoritative target's
    /// current image. Any non-empty write-set must contain only the declared
    /// `(expected_collection, expected_row)` target.
    pub fn preview_delta(
        &self,
        delta: &[u8],
        expected_collection: &str,
        expected_row: &str,
        limits: CrdtDeltaPreviewLimits,
    ) -> Result<CrdtDeltaPreview> {
        // Loro's `fork` enters a barrier and commits a pending auto-commit
        // transaction on its source. Refuse before calling any barrier/frontier
        // API so preview is strictly non-mutating.
        let pending_operations = self.doc.get_pending_txn_len();
        if pending_operations != 0 {
            return Err(CrdtError::PreviewSourceTransactionPending {
                operations: pending_operations,
            });
        }

        // Validate the authenticated Loro envelope and count its *new* oplog
        // contribution before creating a fork. This makes the operation budget
        // a pre-import admission gate, including for deltas that would otherwise
        // be parked for missing causal dependencies.
        let authoritative_oplog = self.doc.oplog_vv();
        let admission = admit_import(
            delta,
            &authoritative_oplog,
            CrdtImportLimits {
                max_bytes: limits.max_delta_bytes,
                max_encoded_operations: DEFAULT_MAX_ENCODED_DELTA_OPS,
                max_new_operations: limits.max_imported_ops,
            },
        )
        .map_err(|error| match error {
            CrdtError::ImportTooLarge { limit, actual } => {
                CrdtError::PreviewDeltaTooLarge { limit, actual }
            }
            CrdtError::ImportMalformed { detail } => CrdtError::PreviewMalformed { detail },
            CrdtError::ImportInvalidOperationRange => CrdtError::PreviewInvalidOperationRange,
            CrdtError::ImportOperationLimitExceeded { limit, actual } => {
                CrdtError::PreviewOperationLimitExceeded { limit, actual }
            }
            _ => error,
        })?;

        // The source is quiescent, so Loro's fork cannot publish source state.
        let fork = self.doc.fork();
        let before_oplog = fork.oplog_vv();
        if before_oplog != authoritative_oplog {
            return Err(CrdtError::PreviewInvalidOperationRange);
        }
        // The write-set comes from the operations the import adds to the fork.
        // It runs after byte and imported-operation caps have bounded the
        // import work. `max_write_set_entries` is semantic cardinality
        // enforcement, not an allocation short-circuit.
        let (imported, write_set) = collect_write_set(&fork, || fork.import(delta));
        let status = imported.map_err(|error| match error {
            loro::LoroError::ImportUpdatesThatDependsOnOutdatedVersion => {
                CrdtError::PreviewPendingDependencies
            }
            error => CrdtError::PreviewMalformed {
                detail: error.to_string(),
            },
        })?;
        if status.pending.is_some() {
            return Err(CrdtError::PreviewPendingDependencies);
        }
        let after_oplog = fork.oplog_vv();
        let imported_ops = positive_oplog_advance(&before_oplog, &after_oplog)?;
        if imported_ops != admission.new_operations {
            return Err(CrdtError::PreviewInvalidOperationRange);
        }
        // A delta that writes outside the root-map-of-row-maps shape is
        // refused with the write-set's own typed error.
        let write_set = write_set?;

        let resulting_frontier = fork.state_frontiers();
        let fork_state = CrdtState {
            doc: DocumentCell::new(fork),
            peer_id: self.peer_id,
            _single_owner: std::marker::PhantomData,
        };
        if write_set.len() > limits.max_write_set_entries {
            return Err(CrdtError::PreviewWriteSetLimitExceeded {
                limit: limits.max_write_set_entries,
                actual: write_set.len(),
            });
        }
        if let Some((actual_collection, actual_row)) = write_set
            .iter()
            .find(|(collection, row)| collection != expected_collection || row != expected_row)
        {
            return Err(CrdtError::PreviewTargetMismatch {
                expected_collection: expected_collection.to_owned(),
                expected_row: expected_row.to_owned(),
                actual_collection: actual_collection.clone(),
                actual_row: actual_row.clone(),
            });
        }

        let raw_post_image = fork_state.read_row(expected_collection, expected_row);
        let post_image = raw_post_image.as_ref().map(loro_to_value);
        let post_image_msgpack =
            zerompk::to_msgpack_vec(&raw_post_image.as_ref().map(CanonicalLoroValue::from))
                .map_err(|error| CrdtError::Loro(format!("preview post-image encode: {error}")))?;
        if post_image_msgpack.len() > limits.max_post_image_bytes {
            return Err(CrdtError::PreviewPostImageTooLarge {
                limit: limits.max_post_image_bytes,
                actual: post_image_msgpack.len(),
            });
        }

        Ok(CrdtDeltaPreview {
            write_set,
            post_image,
            post_image_msgpack,
            resulting_frontier_digest: CrdtState::frontier_digest_from(&resulting_frontier),
            imported_ops,
            trimmed_ops: admission.trimmed_operations(),
        })
    }
}

/// A deterministic, recursively sorted representation for the opaque preview
/// bytes. Loro maps and `Value::Object` use hash maps, so serializing either
/// directly would make a byte-level post-image depend on hash iteration order.
enum CanonicalLoroValue {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    Array(Vec<CanonicalLoroValue>),
    Object(BTreeMap<String, CanonicalLoroValue>),
}

impl zerompk::ToMessagePack for CanonicalLoroValue {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        match self {
            Self::Null => {
                writer.write_array_len(1)?;
                writer.write_u8(0)
            }
            Self::Bool(value) => {
                writer.write_array_len(2)?;
                writer.write_u8(1)?;
                writer.write_boolean(*value)
            }
            Self::Integer(value) => {
                writer.write_array_len(2)?;
                writer.write_u8(2)?;
                writer.write_i64(*value)
            }
            Self::Float(value) => {
                writer.write_array_len(2)?;
                writer.write_u8(3)?;
                writer.write_f64(*value)
            }
            Self::String(value) => {
                writer.write_array_len(2)?;
                writer.write_u8(4)?;
                writer.write_string(value)
            }
            Self::Bytes(value) => {
                writer.write_array_len(2)?;
                writer.write_u8(5)?;
                writer.write_binary(value)
            }
            Self::Array(values) => {
                writer.write_array_len(2)?;
                writer.write_u8(6)?;
                zerompk::ToMessagePack::write(values, writer)
            }
            Self::Object(values) => {
                writer.write_array_len(2)?;
                writer.write_u8(7)?;
                zerompk::ToMessagePack::write(values, writer)
            }
        }
    }
}

impl From<&LoroValue> for CanonicalLoroValue {
    fn from(value: &LoroValue) -> Self {
        match value {
            LoroValue::Null => Self::Null,
            LoroValue::Bool(value) => Self::Bool(*value),
            LoroValue::I64(value) => Self::Integer(*value),
            LoroValue::Double(value) => Self::Float(*value),
            LoroValue::String(value) => Self::String(value.to_string()),
            LoroValue::Binary(value) => Self::Bytes(value.to_vec()),
            LoroValue::List(values) => Self::Array(values.iter().map(Self::from).collect()),
            LoroValue::Map(values) => Self::Object(
                values
                    .iter()
                    .map(|(key, value)| (key.to_string(), Self::from(value)))
                    .collect(),
            ),
            // `loro_to_value` exposes opaque container handles as `Value::Null`.
            // Preserve that exact public post-image semantic in canonical bytes.
            LoroValue::Container(_) => Self::Null,
        }
    }
}

/// Count only positive per-peer oplog advances contributed by one import.
///
/// A snapshot's import status describes ranges carried by its encoded history,
/// including operations the fork already had. Oplog version vectors instead
/// identify the new contribution exactly. Any regression is an invalid preview
/// result: imports must not make a fork forget an operation.
fn positive_oplog_advance(
    before: &loro::VersionVector,
    after: &loro::VersionVector,
) -> Result<usize> {
    for (peer, before_counter) in before.iter() {
        let after_counter = after.get(peer).copied().unwrap_or_default();
        if after_counter < *before_counter {
            return Err(CrdtError::PreviewInvalidOperationRange);
        }
    }
    after
        .iter()
        .try_fold(0usize, |total, (peer, after_counter)| {
            let before_counter = before.get(peer).copied().unwrap_or_default();
            let advance = after_counter
                .checked_sub(before_counter)
                .ok_or(CrdtError::PreviewInvalidOperationRange)?;
            let advance =
                usize::try_from(advance).map_err(|_| CrdtError::PreviewInvalidOperationRange)?;
            total
                .checked_add(advance)
                .ok_or(CrdtError::PreviewInvalidOperationRange)
        })
}

/// Produce a stable digest independent of Loro's frontier-map iteration order.
impl CrdtState {
    /// Digest the current Loro frontier without mutating or committing it.
    pub fn frontier_digest(&self) -> [u8; 32] {
        Self::frontier_digest_from(&self.doc.state_frontiers())
    }

    fn frontier_digest_from(frontier: &loro::Frontiers) -> [u8; 32] {
        let mut entries: Vec<_> = frontier.iter().collect();
        entries.sort_unstable_by_key(|id| (id.peer, id.counter));
        let mut hasher = Sha256::new();
        for id in entries {
            hasher.update(id.peer.to_be_bytes());
            hasher.update(id.counter.to_be_bytes());
        }
        hasher.finalize().into()
    }
}

#[cfg(test)]
mod tests {
    use loro::{ContainerID, ContainerType, LoroValue};

    use super::*;

    fn delta_for(collection: &str, row: &str, value: &str) -> Vec<u8> {
        let source = CrdtState::new(2).expect("source");
        source
            .upsert(
                collection,
                row,
                &[("value", LoroValue::String(value.into()))],
            )
            .expect("write");
        source.export_snapshot().expect("snapshot")
    }

    #[test]
    fn success_is_non_mutating_and_returns_exact_post_image() {
        let state = CrdtState::new(1).expect("state");
        let delta = delta_for("docs", "one", "new");

        // Make the authoritative baseline explicitly committed before any
        // snapshot/frontier observation used by this non-mutation regression.
        state.doc.commit();
        let snapshot_before = state.export_snapshot().expect("before snapshot");
        let frontier_before = state.oplog_version_vector();
        let preview = state
            .preview_delta(&delta, "docs", "one", CrdtDeltaPreviewLimits::default())
            .expect("preview");

        assert_eq!(preview.write_set, vec![("docs".into(), "one".into())]);
        assert!(matches!(
            preview.post_image,
            Some(nodedb_types::Value::Object(_))
        ));
        assert!(!preview.post_image_msgpack.is_empty());
        let decoded: Option<nodedb_types::Value> =
            zerompk::from_msgpack(&preview.post_image_msgpack).expect("decode canonical image");
        assert_eq!(decoded, preview.post_image);
        assert_eq!(
            state.export_snapshot().expect("after snapshot"),
            snapshot_before
        );
        assert_eq!(state.oplog_version_vector(), frontier_before);
    }

    #[test]
    fn rejects_preview_when_source_has_pending_transaction_without_mutating_it() {
        let state = CrdtState::new(1).expect("state");
        state
            .upsert("local", "pending", &[("value", LoroValue::I64(7))])
            .expect("local pending write");
        let pending_before = state.doc.get_pending_txn_len();
        let row_before = state.read_row("local", "pending");
        let delta = delta_for("docs", "one", "remote");

        let error = state
            .preview_delta(&delta, "docs", "one", CrdtDeltaPreviewLimits::default())
            .expect_err("pending source transaction must reject preview");
        assert!(matches!(
            error,
            CrdtError::PreviewSourceTransactionPending { operations } if operations == pending_before
        ));
        assert_eq!(state.doc.get_pending_txn_len(), pending_before);
        assert_eq!(state.read_row("local", "pending"), row_before);
    }

    #[test]
    fn rejects_malformed_delta_before_any_authoritative_mutation() {
        let state = CrdtState::new(1).expect("state");
        let error = state
            .preview_delta(
                b"not loro",
                "docs",
                "one",
                CrdtDeltaPreviewLimits::default(),
            )
            .expect_err("malformed delta must reject");
        assert!(matches!(error, CrdtError::PreviewMalformed { .. }));
        assert!(!state.row_exists("docs", "one"));
    }

    #[test]
    fn rejects_pending_dependencies() {
        let source = CrdtState::new(2).expect("source");
        source
            .upsert(
                "docs",
                "one",
                &[("value", LoroValue::String("base".into()))],
            )
            .expect("base");
        // Loro's auto-commit transaction is finalized only at an explicit
        // commit or an import/export boundary. Commit the prerequisite so
        // the incremental delta below depends on an operation absent from
        // the preview target, rather than exporting both writes together.
        source.doc.commit();
        let version = source.oplog_version_vector();
        source
            .set_fields(
                "docs",
                "one",
                &[("value", LoroValue::String("next".into()))],
            )
            .expect("next");
        let delta = source.export_updates_since(&version).expect("updates");
        let state = CrdtState::new(1).expect("state");

        let error = state
            .preview_delta(&delta, "docs", "one", CrdtDeltaPreviewLimits::default())
            .expect_err("pending dependency must reject");
        assert!(matches!(error, CrdtError::PreviewPendingDependencies));
    }

    #[test]
    fn operation_budget_rejects_pending_incremental_delta_before_import() {
        let source = CrdtState::new(2).expect("source");
        source
            .upsert(
                "docs",
                "one",
                &[("value", LoroValue::String("base".into()))],
            )
            .expect("base");
        source.doc.commit();
        let version = source.oplog_version_vector();
        source
            .set_fields(
                "docs",
                "one",
                &[("value", LoroValue::String("incremental".into()))],
            )
            .expect("incremental write");
        let delta = source.export_updates_since(&version).expect("updates");
        let receiver = CrdtState::new(1).expect("receiver");

        let error = receiver
            .preview_delta(
                &delta,
                "docs",
                "one",
                CrdtDeltaPreviewLimits {
                    max_imported_ops: 0,
                    ..CrdtDeltaPreviewLimits::default()
                },
            )
            .expect_err("budget must reject before missing-dependency import");
        assert!(matches!(
            error,
            CrdtError::PreviewOperationLimitExceeded {
                limit: 0,
                actual: actual_ops
            } if actual_ops > 0
        ));
    }

    #[test]
    fn rejects_wrong_target_and_multi_target() {
        let state = CrdtState::new(1).expect("state");
        let wrong = delta_for("docs", "actual", "value");
        let error = state
            .preview_delta(
                &wrong,
                "docs",
                "expected",
                CrdtDeltaPreviewLimits::default(),
            )
            .expect_err("wrong row");
        assert!(matches!(error, CrdtError::PreviewTargetMismatch { .. }));

        let source = CrdtState::new(2).expect("source");
        source
            .upsert("docs", "one", &[("value", LoroValue::I64(1))])
            .expect("one");
        source
            .upsert("docs", "two", &[("value", LoroValue::I64(2))])
            .expect("two");
        let error = state
            .preview_delta(
                &source.export_snapshot().expect("snapshot"),
                "docs",
                "one",
                CrdtDeltaPreviewLimits::default(),
            )
            .expect_err("multi target");
        assert!(matches!(
            error,
            CrdtError::PreviewWriteSetLimitExceeded { .. }
        ));
    }

    #[test]
    fn rejects_wrong_collection() {
        let state = CrdtState::new(1).expect("state");
        let wrong = delta_for("other", "one", "value");
        let error = state
            .preview_delta(&wrong, "docs", "one", CrdtDeltaPreviewLimits::default())
            .expect_err("wrong collection");
        assert!(matches!(error, CrdtError::PreviewTargetMismatch { .. }));
    }

    #[test]
    fn absent_target_is_distinct_from_loro_null() {
        let state = CrdtState::new(1).expect("state");
        let source = CrdtState::new(2).expect("source");
        let empty_delta = source.export_snapshot().expect("empty snapshot");
        let preview = state
            .preview_delta(
                &empty_delta,
                "docs",
                "missing",
                CrdtDeltaPreviewLimits::default(),
            )
            .expect("zero-op preview");
        assert_eq!(preview.write_set, Vec::<(String, String)>::new());
        assert_eq!(preview.post_image, None);
        assert_eq!(
            preview.post_image_msgpack,
            zerompk::to_msgpack_vec(&Option::<CanonicalLoroValue>::None)
                .expect("canonical absent image")
        );
    }

    #[test]
    fn enforces_every_explicit_limit() {
        let delta = delta_for("docs", "one", "long value");
        let state = CrdtState::new(1).expect("state");
        let limits = CrdtDeltaPreviewLimits {
            max_delta_bytes: 1,
            ..CrdtDeltaPreviewLimits::default()
        };
        assert!(matches!(
            state.preview_delta(&delta, "docs", "one", limits),
            Err(CrdtError::PreviewDeltaTooLarge { .. })
        ));

        let limits = CrdtDeltaPreviewLimits {
            max_imported_ops: 0,
            ..CrdtDeltaPreviewLimits::default()
        };
        assert!(matches!(
            state.preview_delta(&delta, "docs", "one", limits),
            Err(CrdtError::PreviewOperationLimitExceeded { .. })
        ));

        let limits = CrdtDeltaPreviewLimits {
            max_write_set_entries: 0,
            ..CrdtDeltaPreviewLimits::default()
        };
        assert!(matches!(
            state.preview_delta(&delta, "docs", "one", limits),
            Err(CrdtError::PreviewWriteSetLimitExceeded { .. })
        ));

        let limits = CrdtDeltaPreviewLimits {
            max_post_image_bytes: 1,
            ..CrdtDeltaPreviewLimits::default()
        };
        assert!(matches!(
            state.preview_delta(&delta, "docs", "one", limits),
            Err(CrdtError::PreviewPostImageTooLarge { .. })
        ));
    }

    #[test]
    fn already_known_snapshot_does_not_consume_operation_budget() {
        let state = CrdtState::new(1).expect("state");
        let delta = delta_for("docs", "one", "value");
        state.import(&delta).expect("import");

        let preview = state
            .preview_delta(
                &delta,
                "docs",
                "one",
                CrdtDeltaPreviewLimits {
                    max_imported_ops: 0,
                    ..CrdtDeltaPreviewLimits::default()
                },
            )
            .expect("already-known snapshot must be idempotent");
        assert_eq!(preview.imported_ops, 0);
        assert!(preview.write_set.is_empty());
        assert!(
            preview.trimmed_ops > 0,
            "a fully-known delta trimmed every operation it carried"
        );
    }

    #[test]
    fn a_resync_prefix_is_trimmed_while_the_new_suffix_still_imports() {
        // The healthy shape the trim counter must not flag: a client re-pushes
        // history it already sent, followed by writes the receiver has not seen.
        let source = CrdtState::new(2).expect("source");
        source
            .upsert("docs", "known", &[("value", LoroValue::I64(1))])
            .expect("first write");
        let known = source.export_snapshot().expect("known prefix");

        let receiver = CrdtState::new(1).expect("receiver");
        receiver.import(&known).expect("prefix applies");

        source
            .upsert("docs", "fresh", &[("value", LoroValue::I64(2))])
            .expect("second write");
        let full = source.export_snapshot().expect("prefix + suffix");

        let preview = receiver
            .preview_delta(&full, "docs", "fresh", CrdtDeltaPreviewLimits::default())
            .expect("resync preview");
        assert!(
            preview.imported_ops > 0,
            "a resync advances after trimming its replay prefix"
        );
        assert!(
            preview.trimmed_ops > 0,
            "the replay prefix itself is trimmed"
        );
    }

    #[test]
    fn snapshot_history_counts_only_new_oplog_advances() {
        let source = CrdtState::new(2).expect("source");
        source
            .upsert("docs", "existing", &[("value", LoroValue::I64(1))])
            .expect("existing row");
        let initial_snapshot = source.export_snapshot().expect("initial snapshot");

        let receiver = CrdtState::new(1).expect("receiver");
        receiver.import(&initial_snapshot).expect("initial import");
        let receiver_oplog = receiver.oplog_version_vector();

        source
            .upsert("docs", "new", &[("value", LoroValue::I64(2))])
            .expect("new row");
        source.doc.commit();
        let expected_new_ops =
            positive_oplog_advance(&receiver_oplog, &source.oplog_version_vector())
                .expect("monotonic source oplog");
        let full_snapshot = source.export_snapshot().expect("full snapshot");

        let preview = receiver
            .preview_delta(
                &full_snapshot,
                "docs",
                "new",
                CrdtDeltaPreviewLimits::default(),
            )
            .expect("full snapshot carrying known history");
        assert_eq!(preview.imported_ops, expected_new_ops);
        assert!(preview.imported_ops > 0);
        assert_eq!(preview.write_set, vec![("docs".into(), "new".into())]);
    }

    #[test]
    fn canonical_map_encoding_sorts_keys_and_decodes_as_public_value() {
        let mut reverse = BTreeMap::new();
        reverse.insert("z".into(), CanonicalLoroValue::Integer(1));
        reverse.insert("a".into(), CanonicalLoroValue::String("first".into()));
        let canonical = CanonicalLoroValue::Object(reverse);
        let bytes = zerompk::to_msgpack_vec(&canonical).expect("canonical map encoding");
        let decoded: nodedb_types::Value = zerompk::from_msgpack(&bytes).expect("public decode");
        let nodedb_types::Value::Object(fields) = decoded else {
            panic!("canonical map must decode as public object");
        };
        assert_eq!(
            fields.get("a"),
            Some(&nodedb_types::Value::String("first".into()))
        );
        assert_eq!(fields.get("z"), Some(&nodedb_types::Value::Integer(1)));

        let mut forward = BTreeMap::new();
        forward.insert("a".into(), CanonicalLoroValue::String("first".into()));
        forward.insert("z".into(), CanonicalLoroValue::Integer(1));
        assert_eq!(
            bytes,
            zerompk::to_msgpack_vec(&CanonicalLoroValue::Object(forward))
                .expect("stable key ordering")
        );
    }

    #[test]
    fn opaque_loro_container_has_canonical_null_post_image() {
        let raw = LoroValue::Container(ContainerID::Normal {
            peer: 7,
            counter: 11,
            container_type: ContainerType::Map,
        });
        assert_eq!(loro_to_value(&raw), nodedb_types::Value::Null);
        assert!(matches!(
            CanonicalLoroValue::from(&raw),
            CanonicalLoroValue::Null
        ));
        let bytes = zerompk::to_msgpack_vec(&Some(CanonicalLoroValue::from(&raw)))
            .expect("container canonical encoding");
        let decoded: Option<nodedb_types::Value> =
            zerompk::from_msgpack(&bytes).expect("public container decode");
        assert_eq!(decoded, Some(nodedb_types::Value::Null));
        assert_eq!(
            bytes,
            zerompk::to_msgpack_vec(&Some(CanonicalLoroValue::Null))
                .expect("null canonical encoding")
        );
    }

    #[test]
    fn stale_preview_after_receiver_advances_is_idempotent() {
        let source = CrdtState::new(2).expect("source");
        let empty = loro::VersionVector::default();
        source
            .upsert(
                "docs",
                "one",
                &[("value", LoroValue::String("first".into()))],
            )
            .expect("first write");
        let stale = source.export_updates_since(&empty).expect("stale delta");
        source
            .set_fields(
                "docs",
                "one",
                &[("value", LoroValue::String("second".into()))],
            )
            .expect("second write");
        let current = source.export_updates_since(&empty).expect("current delta");

        let receiver = CrdtState::new(1).expect("receiver");
        receiver.import(&current).expect("advance receiver");
        let preview = receiver
            .preview_delta(&stale, "docs", "one", CrdtDeltaPreviewLimits::default())
            .expect("stale preview is idempotent");
        assert_eq!(preview.imported_ops, 0);
        assert!(preview.write_set.is_empty());
    }

    #[test]
    fn known_delta_is_idempotent_and_frontier_digest_is_stable() {
        let state = CrdtState::new(1).expect("state");
        let delta = delta_for("docs", "one", "value");
        state.import(&delta).expect("import");

        let first = state
            .preview_delta(&delta, "docs", "one", CrdtDeltaPreviewLimits::default())
            .expect("idempotent preview");
        let second = state
            .preview_delta(&delta, "docs", "one", CrdtDeltaPreviewLimits::default())
            .expect("repeat preview");
        assert!(first.write_set.is_empty());
        assert!(matches!(
            first.post_image,
            Some(nodedb_types::Value::Object(_))
        ));
        assert_eq!(
            first.resulting_frontier_digest,
            second.resulting_frontier_digest
        );
    }
}
