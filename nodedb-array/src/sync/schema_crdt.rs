//! Loro-backed CRDT schema document for arrays.
//!
//! [`SchemaDoc`] wraps a [`LoroDoc`] to give each array a CRDT-replicated
//! schema. The schema is stored as a MessagePack blob under the key
//! `"content"` in a root Loro map named `"root"`. HLC tracking ensures that
//! schema changes can be causally ordered with cell ops.
//!
//! This is the minimum surface needed by Phase A. ALTER NDARRAY support
//! (incremental dimension/attribute add, domain expansion) is tracked as
//! Phase F work and will build on top of the `replace_schema` path exposed
//! here.

use loro::{LoroDoc, LoroMap, LoroValue};

use crate::error::{ArrayError, ArrayResult};
use crate::schema::array_schema::ArraySchema;
use crate::sync::hlc::{Hlc, HlcGenerator};
use crate::sync::replica_id::ReplicaId;

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

    /// Export the full Loro snapshot as bytes.
    ///
    /// The returned bytes can be passed to [`SchemaDoc::import_snapshot`] on
    /// another replica to converge schema state.
    pub fn export_snapshot(&self) -> ArrayResult<Vec<u8>> {
        self.doc
            .export(loro::ExportMode::Snapshot)
            .map_err(|e| ArrayError::SegmentCorruption {
                detail: format!("loro snapshot export failed: {e}"),
            })
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
        self.doc.import(bytes).map_err(|e| ArrayError::LoroError {
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
        self.doc.import(bytes).map_err(|e| ArrayError::LoroError {
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
    /// This is the stub entry point for Phase F ALTER NDARRAY support.
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
        let result = doc.import_snapshot(b"not valid msgpack or loro data", Hlc::ZERO, &g);
        assert!(result.is_err());
    }
}
