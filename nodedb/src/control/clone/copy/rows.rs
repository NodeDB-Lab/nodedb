// SPDX-License-Identifier: BUSL-1.1

//! One copied row as the redo sub-record that installs it.
//!
//! A copy of a source row into a clone is the same logical version as the
//! source row. Its sub-record carries the source version's system-time
//! ordinal, never a fresh one. Every apply of the record (install, replica,
//! restart replay) writes the same version key, so a snapshot that read the
//! row through the source reads the same row from the clone.
//!
//! Each sub-record carries the clone-copy marker: the apply installs it only
//! where the target holds no version of the row, live or deleted. A write
//! into the clone therefore always wins over a copy. A copy changes no row a
//! clone reader sees, so it names no row change.
//!
//! A row with vectors copies whole. Its node in each vector index installs
//! as a restored vector row (`VectorRestoreRedo`) under the target
//! surrogate, at the node's source start. A vector-primary row's sidecar
//! installs in the same sub-record as its primary node, in its stored
//! tagged encoding, at the source version's stamps. The install keeps the
//! row's payload bitmap entries in step with that sidecar. A document put
//! never carries a sidecar: it decodes the body as plain MessagePack.

use nodedb_types::Surrogate;
use nodedb_types::sync::wire::SyncProvenance;
use nodedb_wal::record::RecordType;

use crate::control::security::catalog::StoredCollection;
use crate::control::surrogate::CarriedIdentity;
use crate::types::{CopiedVectorNodes, DocVersionStamp, KvVersionStamp};
use crate::wal::{
    KvRestoreLive, KvRestoreRedo, RedoSubRecord, VectorRestoreIndex, VectorRestoreLive,
    VectorRestoreRedo, VectorSidecarVersion,
};

/// The primary index of a vector-primary clone collection, as its catalog
/// entry declares it.
#[derive(Debug, Clone)]
pub struct VectorPrimaryCopy {
    pub field_name: String,
    pub dim: u32,
    pub settings: VectorRestoreIndex,
}

impl VectorPrimaryCopy {
    /// The primary index of `coll`. `None` when `coll` is no vector-primary
    /// collection.
    pub fn of(coll: &StoredCollection) -> Option<Self> {
        if !matches!(coll.primary, nodedb_types::PrimaryEngine::Vector) {
            return None;
        }
        coll.vector_primary.as_ref().map(|config| Self {
            field_name: config.vector_field.clone(),
            dim: config.dim,
            settings: VectorRestoreIndex {
                quantization: config.quantization,
                storage_dtype: config.storage_dtype,
                payload_indexes: config.payload_indexes.clone(),
            },
        })
    }
}

/// One document row to copy into a clone.
pub struct DocCopy {
    /// The row's client identity text.
    pub document_id: String,
    /// The primary-key bytes the target surrogate binds under.
    pub pk_bytes: Vec<u8>,
    pub target_surrogate: Surrogate,
    /// The source version's body. For a vector-primary row, its stored
    /// sidecar.
    pub body: Vec<u8>,
    /// The source version's stamps.
    pub stamp: DocVersionStamp,
    /// The row's nodes in each vector index of the collection.
    pub vectors: Vec<CopiedVectorNodes>,
    /// Set when the clone collection is vector-primary.
    pub vector_primary: Option<VectorPrimaryCopy>,
}

/// One KV key to copy into a clone.
pub struct KvCopy {
    pub key: Vec<u8>,
    pub target_surrogate: Surrogate,
    /// The source version's value.
    pub value: Vec<u8>,
    /// The source version's ordinal and absolute expiry.
    pub stamp: KvVersionStamp,
}

/// The pieces one copied row adds to a redo record.
pub(super) struct CopyRecordPart {
    pub ops: Vec<RedoSubRecord>,
    pub identity: CarriedIdentity,
}

/// The sub-record of one restored vector row.
fn vector_op(redo: VectorRestoreRedo) -> crate::Result<RedoSubRecord> {
    Ok(RedoSubRecord {
        record_type: RecordType::VectorPut as u32,
        payload: redo.encode()?,
    })
}

/// The width of the vectors of `nodes`.
fn nodes_dim(nodes: &CopiedVectorNodes) -> crate::Result<u32> {
    let dim = nodes.vectors.first().map_or(0, Vec::len);
    u32::try_from(dim).map_err(|_| crate::Error::Internal {
        detail: format!(
            "clone copy: vector index '{}' has dimension {dim}, which does not fit a u32",
            nodes.field_name
        ),
    })
}

impl DocCopy {
    /// The sub-records that install this row in `stored`, the clone
    /// collection as its engine stores it. `bare` is its catalog name.
    pub(super) fn record_part(self, stored: &str, bare: &str) -> crate::Result<CopyRecordPart> {
        let DocCopy {
            document_id,
            pk_bytes,
            target_surrogate,
            body,
            stamp,
            mut vectors,
            vector_primary,
        } = self;
        let surrogate = target_surrogate.as_u32();
        let mut ops = Vec::with_capacity(1 + vectors.len());
        match &vector_primary {
            Some(primary) => {
                let nodes = vectors
                    .iter()
                    .position(|nodes| nodes.field_name == primary.field_name)
                    .map(|at| vectors.remove(at));
                ops.push(vector_op(VectorRestoreRedo {
                    collection: stored.to_string(),
                    field_name: primary.field_name.clone(),
                    surrogate,
                    dim: primary.dim,
                    multi: nodes.as_ref().is_some_and(|nodes| nodes.multi),
                    ended: Vec::new(),
                    live: nodes.map(|nodes| VectorRestoreLive {
                        vectors: nodes.vectors,
                        sys_from: nodes.sys_from,
                    }),
                    end_live_at: None,
                    vector_primary: true,
                    sidecar: vec![VectorSidecarVersion {
                        sys_from: stamp.sys_from,
                        valid_from_ms: stamp.valid_from_ms,
                        valid_until_ms: stamp.valid_until_ms,
                        body: Some(body),
                    }],
                    primary_index: Some(primary.settings.clone()),
                    clone_copy_at: Some(stamp.sys_from),
                })?);
            }
            None => ops.push(document_put(stored, &document_id, body, surrogate, stamp)?),
        }
        for nodes in vectors {
            let dim = nodes_dim(&nodes)?;
            ops.push(vector_op(VectorRestoreRedo {
                collection: stored.to_string(),
                field_name: nodes.field_name,
                surrogate,
                dim,
                multi: nodes.multi,
                ended: Vec::new(),
                live: Some(VectorRestoreLive {
                    vectors: nodes.vectors,
                    sys_from: nodes.sys_from,
                }),
                end_live_at: None,
                vector_primary: vector_primary.is_some(),
                sidecar: Vec::new(),
                primary_index: None,
                clone_copy_at: Some(stamp.sys_from),
            })?);
        }
        Ok(CopyRecordPart {
            ops,
            identity: CarriedIdentity {
                collection: bare.to_string(),
                pk_bytes,
                surrogate: target_surrogate,
            },
        })
    }
}

/// The stamped document put that installs a copied row's `body`.
fn document_put(
    stored: &str,
    document_id: &str,
    body: Vec<u8>,
    surrogate: u32,
    stamp: DocVersionStamp,
) -> crate::Result<RedoSubRecord> {
    let provenance: Option<SyncProvenance> = None;
    let payload = zerompk::to_msgpack_vec(&(
        stored,
        document_id,
        body,
        provenance,
        surrogate,
        stamp.sys_from,
        stamp.valid_from_ms,
        stamp.valid_until_ms,
        // A clone copy installs only where the row holds no version.
        true,
    ))
    .map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("clone copy: encode document put: {e}"),
    })?;
    Ok(RedoSubRecord {
        record_type: RecordType::Put as u32,
        payload,
    })
}

impl KvCopy {
    /// The restored-key sub-record that installs this key's live row in
    /// `stored` at its source ordinal. `bare` is the catalog name.
    pub(super) fn record_part(self, stored: &str, bare: &str) -> crate::Result<CopyRecordPart> {
        let payload = KvRestoreRedo {
            collection: stored.to_string(),
            key: self.key.clone(),
            surrogate: self.target_surrogate.as_u32(),
            live: Some(KvRestoreLive {
                value: self.value,
                expire_at_ms: self.stamp.expire_at_ms,
                sys_from: self.stamp.sys_from,
            }),
            history: Vec::new(),
            reassert_at: None,
            // A clone copy installs only where the key holds no version.
            clone_copy: true,
        }
        .encode()?;
        Ok(CopyRecordPart {
            ops: vec![RedoSubRecord {
                record_type: RecordType::Put as u32,
                payload,
            }],
            identity: CarriedIdentity {
                collection: bare.to_string(),
                pk_bytes: self.key,
                surrogate: self.target_surrogate,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::DocumentPutRedo;

    /// A copied document row installs at its source version's stamps.
    #[test]
    fn a_document_copy_keeps_the_source_stamp() {
        let stamp = DocVersionStamp {
            sys_from: 1_793_457_000_000_000_007,
            valid_from_ms: 10,
            valid_until_ms: 20,
        };
        let part = DocCopy {
            document_id: "p1".into(),
            pk_bytes: b"p1".to_vec(),
            target_surrogate: Surrogate::new(9),
            body: vec![0x80],
            stamp,
            vectors: Vec::new(),
            vector_primary: None,
        }
        .record_part("db1:products", "products")
        .expect("record part");
        assert_eq!(part.ops.len(), 1);
        let put = DocumentPutRedo::decode(&part.ops[0].payload).expect("a document put");
        let carried = put.stamp.expect("a stamped put");
        assert_eq!(
            (
                carried.sys_from,
                carried.valid_from_ms,
                carried.valid_until_ms
            ),
            (stamp.sys_from, stamp.valid_from_ms, stamp.valid_until_ms)
        );
        assert_eq!(put.surrogate, 9);
        assert!(
            put.clone_copy,
            "a copy installs only where the row holds no version"
        );
        assert_eq!(part.identity.surrogate, Surrogate::new(9));
    }

    fn nodes(field_name: &str, vector: Vec<f32>, sys_from: i64) -> CopiedVectorNodes {
        CopiedVectorNodes {
            field_name: field_name.into(),
            vectors: vec![vector],
            sys_from,
            multi: false,
        }
    }

    /// A document row with a vector index copies as its document put and
    /// its node, both under the target surrogate and gated on the source
    /// version.
    #[test]
    fn a_document_copy_carries_its_vector_nodes() {
        let stamp = DocVersionStamp {
            sys_from: 500,
            valid_from_ms: i64::MIN,
            valid_until_ms: i64::MAX,
        };
        let part = DocCopy {
            document_id: "p1".into(),
            pk_bytes: b"p1".to_vec(),
            target_surrogate: Surrogate::new(9),
            body: vec![0x80],
            stamp,
            vectors: vec![nodes("emb", vec![1.0, 0.0], 400)],
            vector_primary: None,
        }
        .record_part("db1:docs", "docs")
        .expect("record part");
        assert_eq!(part.ops.len(), 2);
        assert!(DocumentPutRedo::decode(&part.ops[0].payload).is_some());
        let row = VectorRestoreRedo::decode(&part.ops[1].payload).expect("a vector row");
        assert_eq!(
            (row.field_name.as_str(), row.surrogate, row.dim),
            ("emb", 9, 2)
        );
        let live = row.live.expect("a live node");
        assert_eq!((live.vectors, live.sys_from), (vec![vec![1.0, 0.0]], 400));
        assert!(row.sidecar.is_empty());
        assert_eq!(row.primary_index, None);
        assert_eq!(row.clone_copy_at, Some(500));
        assert!(!row.vector_primary);
    }

    /// A vector-primary row copies as one vector row: its primary node and
    /// its stored sidecar at the source stamps. No document put carries the
    /// sidecar.
    #[test]
    fn a_vector_primary_copy_carries_its_sidecar_with_its_node() {
        let stamp = DocVersionStamp {
            sys_from: 500,
            valid_from_ms: 10,
            valid_until_ms: 20,
        };
        let settings = VectorRestoreIndex {
            quantization: nodedb_types::VectorQuantization::default(),
            storage_dtype: nodedb_types::VectorStorageDtype::F32,
            payload_indexes: vec![("tag".into(), nodedb_types::PayloadIndexKind::Equality)],
        };
        let sidecar = vec![0x81, 0xa3, b't', b'a', b'g', 0x92, 0x04, 0xa1, b'a'];
        let part = DocCopy {
            document_id: "v1".into(),
            pk_bytes: b"v1".to_vec(),
            target_surrogate: Surrogate::new(3),
            body: sidecar.clone(),
            stamp,
            vectors: vec![nodes("emb", vec![1.0, 0.0, 0.0], 500)],
            vector_primary: Some(VectorPrimaryCopy {
                field_name: "emb".into(),
                dim: 3,
                settings: settings.clone(),
            }),
        }
        .record_part("db1:vp", "vp")
        .expect("record part");
        assert_eq!(part.ops.len(), 1);
        assert_eq!(part.ops[0].record_type, RecordType::VectorPut as u32);
        let row = VectorRestoreRedo::decode(&part.ops[0].payload).expect("a vector row");
        assert!(row.vector_primary);
        assert_eq!(row.primary_index, Some(settings));
        assert_eq!(
            row.sidecar,
            vec![VectorSidecarVersion {
                sys_from: 500,
                valid_from_ms: 10,
                valid_until_ms: 20,
                body: Some(sidecar),
            }]
        );
        assert_eq!(
            row.live.map(|live| live.vectors),
            Some(vec![vec![1.0, 0.0, 0.0]])
        );
        assert_eq!(row.clone_copy_at, Some(500));
    }

    /// A copied KV key installs its live row at its source ordinal, with its
    /// source expiry.
    #[test]
    fn a_kv_copy_keeps_the_source_stamp() {
        let stamp = KvVersionStamp {
            sys_from: 1_793_457_000_000_000_011,
            expire_at_ms: 77,
        };
        let part = KvCopy {
            key: b"p1".to_vec(),
            target_surrogate: Surrogate::new(4),
            value: b"v".to_vec(),
            stamp,
        }
        .record_part("db1:products", "products")
        .expect("record part");
        let restored = KvRestoreRedo::decode(&part.ops[0].payload).expect("a restored key");
        let live = restored.live.expect("a live row");
        assert_eq!((live.sys_from, live.expire_at_ms), (stamp.sys_from, 77));
        assert_eq!(live.value, b"v");
        assert!(restored.history.is_empty());
        assert_eq!(restored.reassert_at, None);
        assert!(
            restored.clone_copy,
            "a copy installs only where the key holds no version"
        );
    }
}
