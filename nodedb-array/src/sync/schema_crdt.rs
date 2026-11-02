// SPDX-License-Identifier: Apache-2.0

//! Loro-backed CRDT schema document for arrays.
//!
//! [`SchemaDoc`] wraps a [`LoroDoc`] to give each array a CRDT-replicated
//! schema. The schema is stored as a MessagePack blob under the key
//! `"content"` in a root Loro map named `"root"`. HLC tracking ensures that
//! schema changes can be causally ordered with cell ops.
//!
//! This is the minimum surface needed for the initial Array sync. ALTER
//! ARRAY support (incremental dimension/attribute add, domain expansion)
//! will build on top of the `replace_schema` path exposed here.

use loro::{LoroDoc, LoroMap, LoroValue};

use crate::error::{ArrayError, ArrayResult};
use crate::schema::array_schema::ArraySchema;
use crate::sync::hlc::{Hlc, HlcGenerator};
use crate::sync::replica_id::ReplicaId;

/// Envelope version for the Loro snapshot bytes produced and consumed by this
/// module.
///
/// Format: `[LORO_FORMAT_VERSION: u8][loro snapshot bytes…]`
///
/// The version is checked on every import. A snapshot of any other version is
/// rejected with a clear error rather than silently corrupting state.
pub const LORO_FORMAT_VERSION: u8 = 1;

/// Maximum enveloped Loro schema snapshot bytes accepted before metadata
/// decoding or import allocation.
pub const MAX_LORO_SCHEMA_SNAPSHOT_BYTES: usize = 16 * 1024 * 1024;
/// Maximum operations physically encoded in an admitted schema snapshot.
pub const MAX_LORO_SCHEMA_SNAPSHOT_OPERATIONS: usize = 100_000;

#[derive(Debug, Clone, Copy)]
struct SchemaSnapshotLimits {
    max_bytes: usize,
    max_operations: usize,
}

impl Default for SchemaSnapshotLimits {
    fn default() -> Self {
        Self {
            max_bytes: MAX_LORO_SCHEMA_SNAPSHOT_BYTES,
            max_operations: MAX_LORO_SCHEMA_SNAPSHOT_OPERATIONS,
        }
    }
}

/// Loro-backed CRDT document tracking a single array's schema.
///
/// The schema is stored as a MessagePack blob at root map key `"content"`.
/// `schema_hlc` is the HLC of the most-recent schema write on this replica;
/// it is compared against the `schema_hlc` embedded in each [`ArrayOp`]
/// header to gate op application.
///
/// `LoroDoc` is not `Clone`, so [`SchemaDoc`] is not `Clone` either.
pub struct SchemaDoc {
    doc: LoroDoc,
    schema_hlc: Hlc,
    replica_id: ReplicaId,
}

impl SchemaDoc {
    /// Create an empty schema doc for `replica_id`.
    ///
    /// `schema_hlc` starts at `Hlc::ZERO`. Call [`SchemaDoc::from_schema`]
    /// or [`SchemaDoc::import_snapshot`] to populate it.
    pub fn new(replica_id: ReplicaId) -> Self {
        Self {
            doc: LoroDoc::new(),
            schema_hlc: Hlc::ZERO,
            replica_id,
        }
    }

    /// Construct a schema doc pre-populated with `schema`.
    ///
    /// The schema is encoded as MessagePack and stored under
    /// `root["content"]`. `generator.next()` is called to assign the initial
    /// `schema_hlc`.
    pub fn from_schema(
        replica_id: ReplicaId,
        schema: &ArraySchema,
        generator: &HlcGenerator,
    ) -> ArrayResult<Self> {
        let mut doc_self = Self::new(replica_id);
        doc_self.write_schema_to_doc(schema)?;
        doc_self.schema_hlc = generator.next()?;
        Ok(doc_self)
    }

    /// Return the current schema HLC.
    pub fn schema_hlc(&self) -> Hlc {
        self.schema_hlc
    }

    /// Return the replica ID of this doc.
    pub fn replica_id(&self) -> ReplicaId {
        self.replica_id
    }

    /// Decode the stored schema from the Loro doc.
    ///
    /// Reads the MessagePack blob at `root["content"]` and decodes it via
    /// zerompk. Errors map to [`ArrayError::SegmentCorruption`].
    pub fn to_schema(&self) -> ArrayResult<ArraySchema> {
        let root = self.doc.get_map("root");
        let bytes = self.read_content_bytes(&root)?;
        zerompk::from_msgpack(&bytes).map_err(|e| ArrayError::SegmentCorruption {
            detail: format!("schema decode failed: {e}"),
        })
    }

    /// Export the full Loro snapshot as an enveloped byte buffer.
    ///
    /// The returned bytes have the format:
    /// `[LORO_FORMAT_VERSION: u8][loro snapshot bytes…]`
    ///
    /// Pass the result to [`SchemaDoc::import_snapshot`] (or
    /// [`SchemaDoc::import_snapshot_replicated`]) on another replica to
    /// converge schema state.
    pub fn export_snapshot(&self) -> ArrayResult<Vec<u8>> {
        let loro_bytes = self.doc.export(loro::ExportMode::Snapshot).map_err(|e| {
            ArrayError::SegmentCorruption {
                detail: format!("loro snapshot export failed: {e}"),
            }
        })?;
        let mut envelope = Vec::with_capacity(1 + loro_bytes.len());
        envelope.push(LORO_FORMAT_VERSION);
        envelope.extend_from_slice(&loro_bytes);
        Ok(envelope)
    }

    /// Import a Loro snapshot from a remote replica.
    ///
    /// After merging the snapshot, `generator.observe(remote_hlc)` is called
    /// so the local generator incorporates the remote clock. A fresh
    /// `schema_hlc` is then generated via `generator.next()` so that any
    /// subsequent local writes have an HLC strictly greater than
    /// `remote_hlc`.
    pub fn import_snapshot(
        &mut self,
        bytes: &[u8],
        remote_hlc: Hlc,
        generator: &HlcGenerator,
    ) -> ArrayResult<()> {
        self.import_snapshot_with_limits(
            bytes,
            remote_hlc,
            generator,
            SchemaSnapshotLimits::default(),
        )
    }

    fn import_snapshot_with_limits(
        &mut self,
        bytes: &[u8],
        remote_hlc: Hlc,
        generator: &HlcGenerator,
        limits: SchemaSnapshotLimits,
    ) -> ArrayResult<()> {
        let loro_bytes = admit_snapshot(bytes, limits)?;
        self.doc
            .import(loro_bytes)
            .map_err(|e| ArrayError::LoroError {
                detail: format!("loro import failed: {e}"),
            })?;
        generator.observe(remote_hlc)?;
        self.schema_hlc = generator.next()?;
        Ok(())
    }

    /// Import a Loro snapshot from a Raft-committed entry.
    ///
    /// Unlike [`import_snapshot`], this method sets `schema_hlc` to exactly
    /// `remote_hlc` rather than bumping past it. This is the correct
    /// behaviour for Raft replication: every replica must converge to the
    /// *same* `schema_hlc` after applying the same log entry so that
    /// schema-gating checks on ops are consistent across the cluster.
    ///
    /// Call this only from the distributed applier (Raft commit path), not
    /// from the CRDT sync path where bumping is required.
    pub fn import_snapshot_replicated(
        &mut self,
        bytes: &[u8],
        committed_hlc: Hlc,
    ) -> ArrayResult<()> {
        self.import_snapshot_replicated_with_limits(
            bytes,
            committed_hlc,
            SchemaSnapshotLimits::default(),
        )
    }

    fn import_snapshot_replicated_with_limits(
        &mut self,
        bytes: &[u8],
        committed_hlc: Hlc,
        limits: SchemaSnapshotLimits,
    ) -> ArrayResult<()> {
        let loro_bytes = admit_snapshot(bytes, limits)?;
        self.doc
            .import(loro_bytes)
            .map_err(|e| ArrayError::LoroError {
                detail: format!("loro import (replicated) failed: {e}"),
            })?;
        self.schema_hlc = committed_hlc;
        Ok(())
    }

    /// Replace the stored schema with `schema`.
    ///
    /// Re-encodes the schema as MessagePack and overwrites `root["content"]`.
    /// Bumps `schema_hlc` via `generator.next()`.
    ///
    /// This is the stub entry point for Phase F ALTER ARRAY support.
    /// Incremental dim/attr add will build on this path.
    pub fn replace_schema(
        &mut self,
        schema: &ArraySchema,
        generator: &HlcGenerator,
    ) -> ArrayResult<()> {
        self.write_schema_to_doc(schema)?;
        self.schema_hlc = generator.next()?;
        Ok(())
    }

    // ─── Internal helpers ────────────────────────────────────────────────────

    fn write_schema_to_doc(&self, schema: &ArraySchema) -> ArrayResult<()> {
        let schema_bytes =
            zerompk::to_msgpack_vec(schema).map_err(|e| ArrayError::SegmentCorruption {
                detail: format!("schema encode failed: {e}"),
            })?;
        let root: LoroMap = self.doc.get_map("root");
        root.insert("content", LoroValue::Binary(schema_bytes.into()))
            .map_err(|e| ArrayError::LoroError {
                detail: format!("loro map insert failed: {e}"),
            })?;
        Ok(())
    }

    fn read_content_bytes(&self, root: &LoroMap) -> ArrayResult<Vec<u8>> {
        match root.get("content") {
            Some(loro::ValueOrContainer::Value(LoroValue::Binary(b))) => Ok(b.to_vec()),
            Some(other) => Err(ArrayError::SegmentCorruption {
                detail: format!("expected Binary at root[\"content\"], got {:?}", other),
            }),
            None => Err(ArrayError::SegmentCorruption {
                detail: "root[\"content\"] not found".into(),
            }),
        }
    }
}

// ─── Envelope helpers ─────────────────────────────────────────────────────────

/// Admit an enveloped Loro schema snapshot before its raw bytes reach Loro.
///
/// The byte and encoded-operation budgets, authenticated metadata parse, and
/// checked operation range all complete before the caller can mutate schema
/// state or observe an HLC.
fn admit_snapshot(bytes: &[u8], limits: SchemaSnapshotLimits) -> ArrayResult<&[u8]> {
    if bytes.len() > limits.max_bytes {
        return Err(ArrayError::LoroSnapshotTooLarge {
            limit: limits.max_bytes,
            actual: bytes.len(),
        });
    }
    let loro_bytes = strip_envelope(bytes)?;
    let metadata = loro::LoroDoc::decode_import_blob_meta(loro_bytes, true).map_err(|error| {
        ArrayError::LoroSnapshotMalformed {
            detail: error.to_string(),
        }
    })?;
    let operation_count =
        encoded_operation_count(&metadata.partial_start_vv, &metadata.partial_end_vv)?;
    if operation_count > limits.max_operations {
        return Err(ArrayError::LoroSnapshotOperationLimitExceeded {
            limit: limits.max_operations,
            actual: operation_count,
        });
    }
    Ok(loro_bytes)
}

fn encoded_operation_count(
    start: &loro::VersionVector,
    end: &loro::VersionVector,
) -> ArrayResult<usize> {
    for (peer, start_counter) in start.iter() {
        if end.get(peer).copied().unwrap_or_default() < *start_counter {
            return Err(ArrayError::LoroSnapshotMalformed {
                detail: "Loro operation range regresses".into(),
            });
        }
    }
    end.iter().try_fold(0usize, |total, (peer, end_counter)| {
        let start_counter = start.get(peer).copied().unwrap_or_default();
        let operations = end_counter.checked_sub(start_counter).ok_or_else(|| {
            ArrayError::LoroSnapshotMalformed {
                detail: "Loro operation range regresses".into(),
            }
        })?;
        let operations =
            usize::try_from(operations).map_err(|_| ArrayError::LoroSnapshotMalformed {
                detail: "Loro operation count does not fit platform usize".into(),
            })?;
        total
            .checked_add(operations)
            .ok_or_else(|| ArrayError::LoroSnapshotMalformed {
                detail: "Loro operation count overflow".into(),
            })
    })
}

fn strip_envelope(bytes: &[u8]) -> ArrayResult<&[u8]> {
    match bytes.first() {
        None => Err(ArrayError::SegmentCorruption {
            detail: "loro snapshot envelope is empty".into(),
        }),
        Some(&v) if v != LORO_FORMAT_VERSION => Err(ArrayError::LoroSnapshotVersionMismatch {
            expected: LORO_FORMAT_VERSION,
            got: v,
        }),
        Some(_) => Ok(&bytes[1..]),
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::array_schema::ArraySchema;
    use crate::schema::attr_spec::{AttrSpec, AttrType};
    use crate::schema::cell_order::{CellOrder, TileOrder};
    use crate::schema::dim_spec::{DimSpec, DimType};
    use crate::sync::hlc::HlcGenerator;
    use crate::sync::replica_id::ReplicaId;
    use crate::types::domain::{Domain, DomainBound};

    fn replica(id: u64) -> ReplicaId {
        ReplicaId::new(id)
    }

    fn generator(id: u64) -> HlcGenerator {
        HlcGenerator::new(replica(id))
    }

    fn simple_schema(name: &str) -> ArraySchema {
        ArraySchema {
            name: name.into(),
            dims: vec![DimSpec::new(
                "x",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(99)),
            )],
            attrs: vec![AttrSpec::new("v", AttrType::Float64, true)],
            tile_extents: vec![10],
            cell_order: CellOrder::RowMajor,
            tile_order: TileOrder::RowMajor,
            georef: None,
        }
    }

    #[test]
    fn from_schema_then_to_schema_roundtrips() {
        let g = generator(1);
        let schema = simple_schema("arr");
        let doc = SchemaDoc::from_schema(replica(1), &schema, &g).unwrap();
        let back = doc.to_schema().unwrap();
        assert_eq!(schema, back);
        assert!(doc.schema_hlc() > Hlc::ZERO);
    }

    #[test]
    fn replace_schema_bumps_hlc() {
        let g = generator(1);
        let schema = simple_schema("arr");
        let mut doc = SchemaDoc::from_schema(replica(1), &schema, &g).unwrap();
        let hlc_before = doc.schema_hlc();

        let schema2 = simple_schema("arr2");
        doc.replace_schema(&schema2, &g).unwrap();
        assert!(doc.schema_hlc() > hlc_before);
        assert_eq!(doc.to_schema().unwrap(), schema2);
    }

    #[test]
    fn export_then_import_converges() {
        let g_a = generator(1);
        let schema = simple_schema("shared");
        let doc_a = SchemaDoc::from_schema(replica(1), &schema, &g_a).unwrap();
        let snapshot = doc_a.export_snapshot().unwrap();

        let g_b = generator(2);
        let mut doc_b = SchemaDoc::new(replica(2));
        doc_b
            .import_snapshot(&snapshot, doc_a.schema_hlc(), &g_b)
            .unwrap();

        assert_eq!(doc_a.to_schema().unwrap(), doc_b.to_schema().unwrap());
    }

    #[test]
    fn import_observes_remote_hlc() {
        let g_a = generator(1);
        let schema = simple_schema("x");
        let doc_a = SchemaDoc::from_schema(replica(1), &schema, &g_a).unwrap();
        let remote_hlc = doc_a.schema_hlc();
        let snapshot = doc_a.export_snapshot().unwrap();

        let g_b = generator(2);
        let mut doc_b = SchemaDoc::new(replica(2));
        doc_b.import_snapshot(&snapshot, remote_hlc, &g_b).unwrap();

        // After import, any new local write must produce hlc > remote_hlc.
        doc_b.replace_schema(&simple_schema("x2"), &g_b).unwrap();
        assert!(doc_b.schema_hlc() > remote_hlc);
    }

    #[test]
    fn import_garbage_errors() {
        let g = generator(1);
        let mut doc = SchemaDoc::new(replica(1));
        // Prefix with a valid version byte so we get past the envelope check
        // and into Loro's own parser — which should reject the payload.
        let mut bad = vec![LORO_FORMAT_VERSION];
        bad.extend_from_slice(b"not valid loro data");
        let result = doc.import_snapshot(&bad, Hlc::ZERO, &g);
        assert!(result.is_err());
    }

    #[test]
    fn export_snapshot_has_version_prefix() {
        let g = generator(1);
        let schema = simple_schema("arr");
        let doc = SchemaDoc::from_schema(replica(1), &schema, &g).unwrap();
        let snapshot = doc.export_snapshot().unwrap();
        assert!(!snapshot.is_empty());
        assert_eq!(snapshot[0], LORO_FORMAT_VERSION);
    }

    #[test]
    fn pre_import_admission_rejections_preserve_schema_and_hlc() {
        let generator = generator(1);
        let baseline = simple_schema("baseline");
        let mut doc = SchemaDoc::from_schema(replica(1), &baseline, &generator).unwrap();
        let before_hlc = doc.schema_hlc();
        let limits = SchemaSnapshotLimits {
            max_bytes: 1,
            max_operations: 1,
        };
        let result = doc.import_snapshot_with_limits(
            &[LORO_FORMAT_VERSION, 0],
            Hlc::ZERO,
            &generator,
            limits,
        );
        assert!(matches!(
            result,
            Err(ArrayError::LoroSnapshotTooLarge { .. })
        ));
        assert_eq!(doc.to_schema().unwrap(), baseline);
        assert_eq!(doc.schema_hlc(), before_hlc);

        let result = doc.import_snapshot_with_limits(
            &[LORO_FORMAT_VERSION, 0xff],
            Hlc::ZERO,
            &generator,
            SchemaSnapshotLimits::default(),
        );
        assert!(matches!(
            result,
            Err(ArrayError::LoroSnapshotMalformed { .. })
        ));
        assert_eq!(doc.to_schema().unwrap(), baseline);
        assert_eq!(doc.schema_hlc(), before_hlc);
    }

    #[test]
    fn operation_admission_rejects_before_mutating_replicated_schema() {
        let source_generator = generator(2);
        let source =
            SchemaDoc::from_schema(replica(2), &simple_schema("source"), &source_generator)
                .unwrap();
        let snapshot = source.export_snapshot().unwrap();

        let target_generator = generator(1);
        let baseline = simple_schema("baseline");
        let mut target = SchemaDoc::from_schema(replica(1), &baseline, &target_generator).unwrap();
        let before_hlc = target.schema_hlc();
        let result = target.import_snapshot_replicated_with_limits(
            &snapshot,
            source.schema_hlc(),
            SchemaSnapshotLimits {
                max_bytes: snapshot.len(),
                max_operations: 0,
            },
        );
        assert!(matches!(
            result,
            Err(ArrayError::LoroSnapshotOperationLimitExceeded { .. })
        ));
        assert_eq!(target.to_schema().unwrap(), baseline);
        assert_eq!(target.schema_hlc(), before_hlc);
    }

    #[test]
    fn import_snapshot_rejects_wrong_version() {
        let g_a = generator(1);
        let schema = simple_schema("v");
        let doc_a = SchemaDoc::from_schema(replica(1), &schema, &g_a).unwrap();
        let mut snapshot = doc_a.export_snapshot().unwrap();

        // Corrupt the version byte.
        snapshot[0] = LORO_FORMAT_VERSION.wrapping_add(1);

        let g_b = generator(2);
        let mut doc_b = SchemaDoc::new(replica(2));
        let err = doc_b
            .import_snapshot(&snapshot, doc_a.schema_hlc(), &g_b)
            .unwrap_err();
        assert!(
            matches!(
                err,
                ArrayError::LoroSnapshotVersionMismatch { expected, got }
                    if expected == LORO_FORMAT_VERSION && got == LORO_FORMAT_VERSION.wrapping_add(1)
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn import_snapshot_replicated_rejects_wrong_version() {
        let g_a = generator(1);
        let schema = simple_schema("v2");
        let doc_a = SchemaDoc::from_schema(replica(1), &schema, &g_a).unwrap();
        let mut snapshot = doc_a.export_snapshot().unwrap();

        snapshot[0] = 0; // version 0 has never existed

        let mut doc_b = SchemaDoc::new(replica(2));
        let err = doc_b
            .import_snapshot_replicated(&snapshot, doc_a.schema_hlc())
            .unwrap_err();
        assert!(
            matches!(
                err,
                ArrayError::LoroSnapshotVersionMismatch { expected, got }
                    if expected == LORO_FORMAT_VERSION && got == 0
            ),
            "unexpected error: {err}"
        );
    }
}
