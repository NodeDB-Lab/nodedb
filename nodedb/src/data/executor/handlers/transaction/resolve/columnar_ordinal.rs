// SPDX-License-Identifier: BUSL-1.1

//! The system-time ordinal each staged columnar row of a resolve takes in a
//! bitemporal collection.
//!
//! A transaction's columnar versions start from the transaction's ordinal,
//! the one its document and edge versions take (`doc_ordinal`): the Calvin
//! transaction's ordinal, else the resolve-time ordinal of the local clock.
//! Each row then rises by the rule every bitemporal write stamps by
//! (`KeyStamps`), over the keys it touches: the base key it replaces or
//! removes, and the key of its image. After a staged TRUNCATE,
//! which closes each current key at the transaction's ordinal raised above
//! its newest version, a key's newest version is that tombstone. The resolve
//! reads only base rows and the overlay, so every replica of the same base
//! raises alike, and the redo carries the stamps.

use std::collections::BTreeMap;

use nodedb_columnar::pk_index::encode_pk;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::bitemporal_time::{KeyStamps, raised_above};
use crate::data::executor::handlers::transaction::overlay::{
    Staged, TxnOverlay, decode_staged_row,
};
use crate::types::{DatabaseId, TenantId};

impl CoreLoop {
    /// The stamp of every row `overlay` staged in the columnar collection
    /// `coll_key`, by surrogate. `None` when the collection is not bitemporal
    /// or has no engine on this core.
    ///
    /// `Err` when a base key or a staged row does not decode, or a stored
    /// version does not read.
    pub(super) fn columnar_txn_stamps(
        &self,
        overlay: &TxnOverlay,
        coll_key: &(DatabaseId, TenantId, String),
        txn_ordinal: i64,
    ) -> crate::Result<Option<BTreeMap<u32, i64>>> {
        let Some(engine) = self.columnar_engines.get(coll_key) else {
            return Ok(None);
        };
        let schema = engine.schema();
        if !schema.is_bitemporal() {
            return Ok(None);
        }
        let collection = coll_key.2.as_str();
        let truncated = overlay.is_truncated(coll_key);
        let staged: BTreeMap<u32, &Staged> = overlay.iter_for_collection(coll_key).collect();
        let mut batch = KeyStamps::default();
        let mut stamps = BTreeMap::new();
        for (surrogate, entry) in staged {
            let mut keys: Vec<Vec<u8>> = Vec::with_capacity(2);
            if !truncated && let Some(prior) = overlay.base_pk(coll_key, surrogate) {
                let pk = nodedb_types::value_from_msgpack(prior).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!(
                            "columnar resolve: base key of '{collection}' (surrogate \
                             {surrogate}) does not decode: {e}"
                        ),
                    }
                })?;
                keys.push(encode_pk(&pk));
            }
            if let Staged::Put(body) = entry {
                let values = decode_staged_row(body).ok_or_else(|| crate::Error::Internal {
                    detail: format!(
                        "columnar resolve: staged row of '{collection}' (surrogate {surrogate}) \
                         does not decode"
                    ),
                })?;
                keys.push(engine.encode_pk_from_row(&values)?);
            }
            let stamp = batch.stamp_row(txn_ordinal, keys, |key| {
                let stored = self.newest_stored_stamp(coll_key, schema, key)?;
                // After a truncate a stored key's newest version is the
                // truncate's tombstone: the install closes it first.
                Ok(if truncated {
                    stored.map(|stored| raised_above(txn_ordinal, Some(stored)))
                } else {
                    stored
                })
            })?;
            stamps.insert(surrogate, stamp);
        }
        Ok(Some(stamps))
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::RowIdentity;
    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
    use nodedb_types::value::Value;

    use super::*;
    use crate::data::executor::core_loop::tests::make_core_with_dir;

    fn schema() -> ColumnarSchema {
        ColumnarSchema::new(vec![
            ColumnDef::required("_ts_system", ColumnType::Int64),
            ColumnDef::required("_ts_valid_from", ColumnType::Int64),
            ColumnDef::required("_ts_valid_until", ColumnType::Int64),
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("v", ColumnType::Int64),
        ])
        .expect("valid")
    }

    fn version(ts: i64, id: i64, v: i64) -> Vec<Value> {
        vec![
            Value::Integer(ts),
            Value::Integer(0),
            Value::Integer(i64::MAX),
            Value::Integer(id),
            Value::Integer(v),
        ]
    }

    fn key_bytes(id: i64) -> Vec<u8> {
        nodedb_types::value_to_msgpack(&Value::Integer(id)).expect("encode key")
    }

    /// Every staged row takes the transaction's ordinal. A row whose key
    /// holds a version at or above it rises one above that version, and a
    /// later row on a key an earlier row stamped rises above that stamp.
    #[test]
    fn each_staged_row_takes_the_txn_ordinal_raised_per_key() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let coll_key = (DatabaseId::DEFAULT, TenantId::new(1), "bt".to_string());
        let txn = 1_000;
        let mut engine = nodedb_columnar::MutationEngine::new("bt".to_string(), schema());
        engine.insert(&version(100, 1, 10)).expect("insert");
        engine.insert(&version(200, 2, 20)).expect("insert");
        engine.insert(&version(txn + 50, 3, 30)).expect("insert");
        core.columnar_engines.insert(coll_key.clone(), engine);

        let mut overlay = TxnOverlay::new();
        // Surrogate 1: an update of id 1.
        overlay.note_base_pk(&coll_key, 1, key_bytes(1));
        let body = nodedb_types::value_to_msgpack(&Value::Array(version(0, 1, 11)))
            .expect("encode staged row");
        overlay.insert_put(coll_key.clone(), 1, &RowIdentity::from_user_key("1"), body);
        // Surrogate 2: a delete of id 2.
        overlay.note_base_pk(&coll_key, 2, key_bytes(2));
        overlay.insert_tombstone(coll_key.clone(), 2, &RowIdentity::from_user_key("2"));
        // Surrogate 3: a delete of id 3, whose version sits above the txn.
        overlay.note_base_pk(&coll_key, 3, key_bytes(3));
        overlay.insert_tombstone(coll_key.clone(), 3, &RowIdentity::from_user_key("3"));
        // Surrogate 4: another staged row on id 1's key.
        let again = nodedb_types::value_to_msgpack(&Value::Array(version(0, 1, 12)))
            .expect("encode staged row");
        overlay.insert_put(
            coll_key.clone(),
            4,
            &RowIdentity::from_user_key("1b"),
            again,
        );

        let stamps = core
            .columnar_txn_stamps(&overlay, &coll_key, txn)
            .expect("stamps")
            .expect("bitemporal");
        assert_eq!(
            stamps.into_iter().collect::<Vec<_>>(),
            vec![(1, txn), (2, txn), (3, txn + 51), (4, txn + 1)]
        );
    }

    /// After a staged truncate, a row on a key with a stored version rises
    /// above the tombstone the truncate closes it with, and a row on a new
    /// key takes the transaction's ordinal.
    #[test]
    fn a_row_after_a_staged_truncate_rises_above_its_keys_tombstone() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let coll_key = (DatabaseId::DEFAULT, TenantId::new(1), "bt".to_string());
        let txn = 1_000;
        let mut engine = nodedb_columnar::MutationEngine::new("bt".to_string(), schema());
        engine.insert(&version(100, 1, 10)).expect("insert");
        engine.insert(&version(txn + 50, 2, 20)).expect("insert");
        core.columnar_engines.insert(coll_key.clone(), engine);

        let mut overlay = TxnOverlay::new();
        overlay.mark_truncated(coll_key.clone());
        for (surrogate, id) in [(1, 1), (2, 2), (3, 3)] {
            let body = nodedb_types::value_to_msgpack(&Value::Array(version(0, id, 7)))
                .expect("encode staged row");
            overlay.insert_put(
                coll_key.clone(),
                surrogate,
                &RowIdentity::from_user_key(id.to_string()),
                body,
            );
        }

        let stamps = core
            .columnar_txn_stamps(&overlay, &coll_key, txn)
            .expect("stamps")
            .expect("bitemporal");
        // id 1's tombstone takes the txn ordinal, id 2's rises above its
        // version, and id 3 has no stored version.
        assert_eq!(
            stamps.into_iter().collect::<Vec<_>>(),
            vec![(1, txn + 1), (2, txn + 52), (3, txn)]
        );
    }
}
