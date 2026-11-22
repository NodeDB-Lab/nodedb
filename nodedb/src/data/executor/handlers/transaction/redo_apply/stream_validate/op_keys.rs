// SPDX-License-Identifier: BUSL-1.1

//! The fence keys of state a sub-record's install writes beside its
//! collection: the sync high-water mark a sync frame advances, and the
//! graph state of the record's tenant.
//!
//! A stream marker's keys declare both, and its install arms them before
//! the validate. The walk keys each sub-record again, and each slice checks
//! what it walked against what the fence holds. The proposer declares the
//! keys through the same sub-record reads (see
//! `crate::wal::redo::stream_keys`), so the declared keys cover the walk.

use std::collections::BTreeSet;

use crate::data::executor::core_loop::install_fence::FenceKey;
use crate::types::{DatabaseId, TenantId};
use crate::wal::RedoSubRecord;
use crate::wal::redo::stream_keys::{sub_record_provenance, sub_record_writes_graph};

/// Add to `keys` the keys of the state the install of `sub` writes beside
/// its collection. `db` and `tenant` are the installing request's. The
/// install fence keys a whole record's ops with it too.
pub(in crate::data::executor) fn add_sub_record_keys(
    sub: &RedoSubRecord,
    db: DatabaseId,
    tenant: TenantId,
    keys: &mut BTreeSet<FenceKey>,
) {
    keys.extend(
        sub_record_provenance(sub)
            .as_ref()
            .and_then(FenceKey::sync_stream),
    );
    // The graph key joins once. The edge and label decode runs only until
    // a record has joined it.
    let graph = FenceKey::Graph { db, tenant };
    if !keys.contains(&graph) && sub_record_writes_graph(sub) {
        keys.insert(graph);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::server::wal_dispatch::encode_vector_put_payload;
    use nodedb_types::Surrogate;
    use nodedb_types::sync::wire::SyncProvenance;
    use nodedb_wal::record::RecordType;

    fn provenance(producer_id: u64) -> SyncProvenance {
        SyncProvenance {
            producer_id,
            epoch: 1,
            stream_id: 42,
            seq: 1,
        }
    }

    fn vector_put(provenance: Option<&SyncProvenance>) -> RedoSubRecord {
        let payload =
            encode_vector_put_payload("v", &[1.0, 2.0], 2, "", Surrogate::new(9), provenance)
                .expect("encode vector put");
        RedoSubRecord {
            record_type: RecordType::VectorPut as u32,
            payload,
        }
    }

    fn keys_of(sub: &RedoSubRecord) -> BTreeSet<FenceKey> {
        let mut keys = BTreeSet::new();
        add_sub_record_keys(sub, DatabaseId::DEFAULT, TenantId::new(1), &mut keys);
        keys
    }

    #[test]
    fn a_sync_frame_keys_its_stream() {
        let keys = keys_of(&vector_put(Some(&provenance(7))));
        assert_eq!(
            keys.into_iter().collect::<Vec<_>>(),
            vec![FenceKey::SyncStream {
                producer_id: 7,
                stream_id: 42,
            }]
        );
    }

    #[test]
    fn a_frame_of_producer_zero_and_a_plain_write_key_nothing() {
        assert!(keys_of(&vector_put(Some(&provenance(0)))).is_empty());
        assert!(keys_of(&vector_put(None)).is_empty());
    }

    #[test]
    fn an_edge_put_keys_the_graph_of_its_tenant() {
        let edge = RedoSubRecord {
            record_type: RecordType::Put as u32,
            payload: zerompk::to_msgpack_vec(&crate::wal::EdgePutRedo {
                collection: "knows".into(),
                src_id: "a".into(),
                label: "KNOWS".into(),
                dst_id: "b".into(),
                properties: Vec::new(),
                src_surrogate: 1,
                dst_surrogate: 2,
                system_from: Some(5),
                applied: None,
                valid_from_ms: None,
                valid_until_ms: None,
            })
            .expect("encode edge put"),
        };
        assert_eq!(
            keys_of(&edge).into_iter().collect::<Vec<_>>(),
            vec![FenceKey::Graph {
                db: DatabaseId::DEFAULT,
                tenant: TenantId::new(1),
            }]
        );
    }

    #[test]
    fn a_document_put_keys_nothing_beside_its_collection() {
        let body = nodedb_types::json_to_msgpack(&serde_json::json!({"n": 1})).expect("body");
        let put = super::super::super::test_commit::doc_put_sub_record("docs", "d1", &body, 1);
        assert!(keys_of(&put).is_empty());
    }
}
