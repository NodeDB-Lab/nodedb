// SPDX-License-Identifier: BUSL-1.1

//! The vector column of a vector-primary row.
//!
//! A vector-primary row keeps its vector on the HNSW node bound to its
//! surrogate and every other column in the sparse-store sidecar. A read of
//! the row renders both: the sidecar columns plus the vector under the
//! collection's `vector_field`. A scan, a point read and a `RETURNING`
//! clause all render the row this way, so `SELECT *`, `RETURNING *` and a
//! copy of the row (`INSERT ... SELECT`, `MERGE`) carry the vector.

use nodedb_types::{StorageKey, Surrogate, Value};

use crate::data::executor::core_loop::CoreLoop;
use crate::types::{DatabaseId, TenantId, TxnId};

use super::transaction::overlay::{Staged, StagedVectorRow};

/// The vector as a row value: an array of floats.
pub(in crate::data::executor) fn vector_value(vector: &[f32]) -> Value {
    Value::Array(vector.iter().map(|x| Value::Float(f64::from(*x))).collect())
}

/// `row`, a msgpack map, with `vector` added under `field`.
pub(in crate::data::executor) fn with_vector_column(
    row: &[u8],
    field: &str,
    vector: &[f32],
) -> crate::Result<Vec<u8>> {
    let mut value = nodedb_types::value_from_msgpack(row).map_err(|e| crate::Error::Codec {
        detail: format!("vector-primary row decode: {e}"),
    })?;
    let Value::Object(map) = &mut value else {
        return Err(crate::Error::Internal {
            detail: "a vector-primary row is not a map".into(),
        });
    };
    map.insert(field.to_string(), vector_value(vector));
    nodedb_types::value_to_msgpack(&value).map_err(|e| crate::Error::Codec {
        detail: format!("vector-primary row encode: {e}"),
    })
}

impl CoreLoop {
    /// The vector field of a vector-primary collection; `None` for every
    /// other collection.
    pub(in crate::data::executor) fn vector_primary_field(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> Option<String> {
        let key = (
            DatabaseId::new(database_id),
            TenantId::new(tid),
            collection.to_string(),
        );
        self.doc_configs
            .get(&key)
            .and_then(|config| config.vector_primary.as_ref())
            .map(|vp| vp.vector_field.clone())
    }

    /// The committed vector of `surrogate` in the vector-primary index of
    /// `collection`.`field`. A sidecar row is written with its node, so a row
    /// without one is an error, never a row with no vector.
    pub(in crate::data::executor) fn committed_primary_vector(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        field: &str,
        surrogate: Surrogate,
    ) -> crate::Result<Vec<f32>> {
        let index_key = CoreLoop::vector_index_key(database_id, tid, collection, field);
        self.vector_collections
            .get(&index_key)
            .and_then(|coll| coll.vector_for_surrogate(surrogate))
            .ok_or_else(|| crate::Error::Internal {
                detail: format!(
                    "vector-primary row {} of '{collection}' has no vector in its index",
                    StorageKey::for_surrogate(surrogate)
                ),
            })
    }

    /// The vector of `surrogate`'s row version visible at system-time
    /// ordinal `at` in the vector-primary index of `collection`.`field`.
    /// `None` reads the committed current vector. A row version visible at
    /// the cut has its node visible there too, so a row without one is an
    /// error.
    pub(in crate::data::executor) fn primary_vector_at(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        field: &str,
        surrogate: Surrogate,
        at: Option<i64>,
    ) -> crate::Result<Vec<f32>> {
        let Some(at) = at else {
            return self.committed_primary_vector(database_id, tid, collection, field, surrogate);
        };
        let index_key = CoreLoop::vector_index_key(database_id, tid, collection, field);
        self.vector_collections
            .get(&index_key)
            .and_then(|coll| coll.vector_for_surrogate_at(surrogate, at))
            .ok_or_else(|| crate::Error::Internal {
                detail: format!(
                    "vector-primary row {} of '{collection}' has no vector visible at \
                     system-time ordinal {at}",
                    StorageKey::for_surrogate(surrogate)
                ),
            })
    }

    /// The vector `surrogate` reads with: the staged one when `txn_id`
    /// staged a put of the row, else the vector of the version visible at
    /// `at` (the committed current one when `at` is `None`). A historical
    /// read passes no `txn_id`.
    pub(in crate::data::executor) fn current_primary_vector(
        &self,
        txn_id: Option<TxnId>,
        coll_key: &(DatabaseId, TenantId, String),
        field: &str,
        surrogate: Surrogate,
        at: Option<i64>,
    ) -> crate::Result<Vec<f32>> {
        if let Some(Staged::Put(body)) = txn_id
            .and_then(|txn| self.txn_overlays.get(&txn))
            .and_then(|overlay| overlay.get(coll_key, surrogate.0))
        {
            return Ok(StagedVectorRow::from_bytes(body)?.vector);
        }
        self.primary_vector_at(
            coll_key.0.as_u64(),
            coll_key.1.as_u64(),
            &coll_key.2,
            field,
            surrogate,
            at,
        )
    }

    /// The vector field and the vector a vector-primary row version held:
    /// the node visible at the version's `system_from`, or, for a delete
    /// version, the node of the version it closes. `None` when the index
    /// keeps no such node: a purge dropped it.
    pub(in crate::data::executor) fn version_primary_vector(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        surrogate: Surrogate,
        system_from: i64,
    ) -> Option<(String, Vec<f32>)> {
        let field = self.vector_primary_field(database_id, tid, collection)?;
        let index_key = CoreLoop::vector_index_key(database_id, tid, collection, &field);
        let coll = self.vector_collections.get(&index_key)?;
        let vector = coll
            .vector_for_surrogate_at(surrogate, system_from)
            .or_else(|| coll.vector_for_surrogate_at(surrogate, system_from.saturating_sub(1)))?;
        Some((field, vector))
    }

    /// Add each normalized sidecar row's vector under `field`: the vector of
    /// the row version visible at system-time ordinal `at`, the committed
    /// current one when `at` is `None`.
    pub(in crate::data::executor) fn attach_primary_vectors(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        field: &str,
        rows: &mut [(StorageKey, Vec<u8>)],
        at: Option<i64>,
    ) -> crate::Result<()> {
        for (key, row) in rows.iter_mut() {
            let vector =
                self.primary_vector_at(database_id, tid, collection, field, key.surrogate(), at)?;
            *row = with_vector_column(row, field, &vector)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn the_vector_joins_the_row_under_its_field() {
        let mut fields = HashMap::new();
        fields.insert("owner".to_string(), Value::String("alice".into()));
        let row = nodedb_types::value_to_msgpack(&Value::Object(fields)).expect("encode");
        let out = with_vector_column(&row, "vec", &[1.0, 0.5]).expect("attach");
        let Value::Object(map) = nodedb_types::value_from_msgpack(&out).expect("decode") else {
            panic!("row is not a map");
        };
        assert_eq!(map.get("owner"), Some(&Value::String("alice".into())));
        assert_eq!(
            map.get("vec"),
            Some(&Value::Array(vec![Value::Float(1.0), Value::Float(0.5)]))
        );
    }

    #[test]
    fn a_row_that_is_not_a_map_is_an_error() {
        let row = nodedb_types::value_to_msgpack(&Value::Integer(3)).expect("encode");
        assert!(with_vector_column(&row, "vec", &[1.0]).is_err());
    }
}
