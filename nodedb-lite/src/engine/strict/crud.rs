//! CRUD operations for strict document collections.

use std::collections::HashMap;

use nodedb_strict::arrow_extract::extract_column_to_arrow;
use nodedb_types::Namespace;
use nodedb_types::columnar::SchemaOps;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::storage::engine::{StorageEngine, WriteOp};

use super::engine::{StrictEngine, strict_err_to_lite};

impl<S: StorageEngine> StrictEngine<S> {
    // -- Write path --

    /// Insert a row into a strict collection.
    ///
    /// Validates schema, encodes as Binary Tuple, writes to storage keyed by PK.
    /// Returns an error if the PK already exists.
    pub async fn insert(&self, collection: &str, values: &[Value]) -> Result<(), LiteError> {
        let state = self.get_state(collection)?;

        // Encode the row.
        let tuple = state.encoder.encode(values).map_err(strict_err_to_lite)?;

        // Build storage key from PK values.
        let key = state.storage_key(collection, values);

        // Check for duplicate PK.
        if self.storage.get(Namespace::Strict, &key).await?.is_some() {
            return Err(LiteError::BadRequest {
                detail: format!("duplicate primary key in collection '{collection}'"),
            });
        }

        self.storage.put(Namespace::Strict, &key, &tuple).await
    }

    /// Insert multiple rows atomically.
    pub async fn insert_batch(
        &self,
        collection: &str,
        rows: &[Vec<Value>],
    ) -> Result<(), LiteError> {
        let state = self.get_state(collection)?;

        let mut ops = Vec::with_capacity(rows.len());
        for values in rows {
            let tuple = state.encoder.encode(values).map_err(strict_err_to_lite)?;
            let key = state.storage_key(collection, values);
            ops.push(WriteOp::Put {
                ns: Namespace::Strict,
                key,
                value: tuple,
            });
        }

        self.storage.batch_write(&ops).await
    }

    /// Update a row by PK. Reads the existing tuple, patches the specified
    /// fields, and writes the modified tuple back.
    ///
    /// `updates` maps column names to new values. Columns not in the map
    /// retain their existing values.
    pub async fn update(
        &self,
        collection: &str,
        pk: &Value,
        updates: &HashMap<String, Value>,
    ) -> Result<bool, LiteError> {
        let state = self.get_state(collection)?;
        let key = state.storage_key_from_pk(collection, pk);

        // Read existing tuple.
        let existing = match self.storage.get(Namespace::Strict, &key).await? {
            Some(bytes) => bytes,
            None => return Ok(false),
        };

        // Extract all current values.
        let mut values = state
            .decoder
            .extract_all(&existing)
            .map_err(strict_err_to_lite)?;

        // Apply updates.
        for (col_name, new_value) in updates {
            let col_idx =
                state
                    .schema
                    .column_index(col_name)
                    .ok_or_else(|| LiteError::BadRequest {
                        detail: format!("unknown column '{col_name}' in collection '{collection}'"),
                    })?;

            // Validate the new value against the column type.
            if !matches!(new_value, Value::Null)
                && !state.schema.columns[col_idx].column_type.accepts(new_value)
            {
                return Err(LiteError::BadRequest {
                    detail: format!(
                        "column '{}': type mismatch",
                        state.schema.columns[col_idx].name
                    ),
                });
            }

            values[col_idx] = new_value.clone();
        }

        // Re-encode and write.
        let new_tuple = state.encoder.encode(&values).map_err(strict_err_to_lite)?;

        // If PK columns were updated, we need to delete the old key and insert the new one.
        let new_key = state.storage_key(collection, &values);
        if new_key != key {
            self.storage
                .batch_write(&[
                    WriteOp::Delete {
                        ns: Namespace::Strict,
                        key,
                    },
                    WriteOp::Put {
                        ns: Namespace::Strict,
                        key: new_key,
                        value: new_tuple,
                    },
                ])
                .await?;
        } else {
            self.storage
                .put(Namespace::Strict, &key, &new_tuple)
                .await?;
        }

        Ok(true)
    }

    /// Update a row by replacing with complete new values (for CRDT adapter).
    pub async fn update_by_values(
        &self,
        collection: &str,
        pk: &Value,
        new_values: &[Value],
    ) -> Result<bool, LiteError> {
        let state = self.get_state(collection)?;
        let key = state.storage_key_from_pk(collection, pk);

        if self.storage.get(Namespace::Strict, &key).await?.is_none() {
            return Ok(false);
        }

        let new_tuple = state
            .encoder
            .encode(new_values)
            .map_err(strict_err_to_lite)?;
        self.storage
            .put(Namespace::Strict, &key, &new_tuple)
            .await?;
        Ok(true)
    }

    /// Delete a row by PK. Returns true if the row existed.
    pub async fn delete(&self, collection: &str, pk: &Value) -> Result<bool, LiteError> {
        let state = self.get_state(collection)?;
        let key = state.storage_key_from_pk(collection, pk);

        let existed = self.storage.get(Namespace::Strict, &key).await?.is_some();
        if existed {
            self.storage.delete(Namespace::Strict, &key).await?;
        }
        Ok(existed)
    }

    // -- Read path --

    /// Point lookup by PK. Returns the row as a Vec<Value>, or None.
    pub async fn get(&self, collection: &str, pk: &Value) -> Result<Option<Vec<Value>>, LiteError> {
        let state = self.get_state(collection)?;
        let key = state.storage_key_from_pk(collection, pk);

        match self.storage.get(Namespace::Strict, &key).await? {
            Some(bytes) => {
                // Check tuple schema version for multi-version reads.
                let tuple_version = state
                    .decoder
                    .schema_version(&bytes)
                    .map_err(strict_err_to_lite)?;
                let current_version = state.schema.version;

                if tuple_version < current_version {
                    // Old tuple — it was encoded with fewer columns. Build a
                    // temporary decoder from the old schema to read it, then
                    // pad with Null for columns added after that version.
                    let old_col_count = state
                        .version_column_counts
                        .get(&tuple_version)
                        .copied()
                        .unwrap_or(state.schema.columns.len());

                    let old_schema = nodedb_types::columnar::StrictSchema {
                        columns: state.schema.columns[..old_col_count].to_vec(),
                        version: tuple_version,
                    };
                    let old_decoder = nodedb_strict::TupleDecoder::new(&old_schema);
                    let mut values = old_decoder
                        .extract_all(&bytes)
                        .map_err(strict_err_to_lite)?;
                    values.resize(state.schema.columns.len(), Value::Null);
                    Ok(Some(values))
                } else {
                    let values = state
                        .decoder
                        .extract_all(&bytes)
                        .map_err(strict_err_to_lite)?;
                    Ok(Some(values))
                }
            }
            None => Ok(None),
        }
    }

    /// Point lookup with column projection. Only decodes the requested columns.
    pub async fn get_projected(
        &self,
        collection: &str,
        pk: &Value,
        columns: &[&str],
    ) -> Result<Option<Vec<Value>>, LiteError> {
        let state = self.get_state(collection)?;
        let key = state.storage_key_from_pk(collection, pk);

        match self.storage.get(Namespace::Strict, &key).await? {
            Some(bytes) => {
                let mut values = Vec::with_capacity(columns.len());
                for col_name in columns {
                    let val = state
                        .decoder
                        .extract_by_name(&bytes, col_name)
                        .map_err(strict_err_to_lite)?;
                    values.push(val);
                }
                Ok(Some(values))
            }
            None => Ok(None),
        }
    }

    /// Scan all rows in a collection. Returns raw tuple bytes for Arrow extraction.
    pub async fn scan_raw(&self, collection: &str) -> Result<Vec<Vec<u8>>, LiteError> {
        let _state = self.get_state(collection)?;
        let prefix = format!("{collection}:");
        let entries = self
            .storage
            .scan_prefix(Namespace::Strict, prefix.as_bytes())
            .await?;
        Ok(entries.into_iter().map(|(_, v)| v).collect())
    }

    /// Scan all rows and extract a single column into an Arrow array.
    pub async fn scan_column_to_arrow(
        &self,
        collection: &str,
        col_idx: usize,
    ) -> Result<arrow::array::ArrayRef, LiteError> {
        let state = self.get_state(collection)?;
        let tuples = self.scan_raw(collection).await?;
        let refs: Vec<&[u8]> = tuples.iter().map(|t| t.as_slice()).collect();

        extract_column_to_arrow(&state.schema, &state.decoder, &refs, col_idx)
            .map_err(strict_err_to_lite)
    }

    /// Scan all rows and extract multiple columns into Arrow arrays.
    pub async fn scan_columns_to_arrow(
        &self,
        collection: &str,
        col_indices: &[usize],
    ) -> Result<Vec<arrow::array::ArrayRef>, LiteError> {
        let state = self.get_state(collection)?;
        let tuples = self.scan_raw(collection).await?;
        let refs: Vec<&[u8]> = tuples.iter().map(|t| t.as_slice()).collect();

        let mut arrays = Vec::with_capacity(col_indices.len());
        for &idx in col_indices {
            let arr = extract_column_to_arrow(&state.schema, &state.decoder, &refs, idx)
                .map_err(strict_err_to_lite)?;
            arrays.push(arr);
        }
        Ok(arrays)
    }

    /// Count the number of rows in a collection.
    pub async fn count(&self, collection: &str) -> Result<usize, LiteError> {
        let _state = self.get_state(collection)?;
        let prefix = format!("{collection}:");
        let entries = self
            .storage
            .scan_prefix(Namespace::Strict, prefix.as_bytes())
            .await?;
        Ok(entries.len())
    }
}
