// SPDX-License-Identifier: BUSL-1.1

//! Reinstating index registrations and their content from a checkpoint.
//!
//! These are the counterparts of the `register_*` calls, and they differ in one
//! deliberate way: they install the exported content instead of deriving it from
//! the collection's rows. Deriving is not equivalent. An index registered with
//! `backfill=false` deliberately omits every row that predates it, so its
//! content is a function of the write history and cannot be recomputed from the
//! rows alone — a restore that re-derived it would silently promote a partial
//! index to a full one, and start answering queries with rows the index was
//! never meant to contain.
//!
//! Because the content is installed rather than derived, the rows must be
//! restored FIRST, while the collection still has zero registrations: the write
//! path's zero-index fast path then leaves the tables alone, and no PUT-driven
//! maintenance can add entries alongside the ones being reinstalled here.

use super::super::engine_helpers::table_key;
use super::super::index::IndexCoverage;
use super::super::sorted_index::manager::SortedIndexDef;
use super::KvEngine;

/// Parameters for [`KvEngine::restore_field_index`].
pub struct RestoreFieldIndexParams<'a> {
    pub database_id: u64,
    pub tenant_id: u64,
    pub collection: &'a str,
    pub field: &'a str,
    pub field_position: usize,
    /// Which rows the exported index held.
    pub coverage: IndexCoverage,
    /// `(field_value, primary_key)` pairs, exactly as exported.
    pub entries: &'a [(Vec<u8>, Vec<u8>)],
}

/// Parameters for [`KvEngine::restore_composite_index`].
pub struct RestoreCompositeIndexParams<'a> {
    pub database_id: u64,
    pub tenant_id: u64,
    pub collection: &'a str,
    pub fields: &'a [String],
    pub field_positions: &'a [usize],
    /// Which rows the exported index held.
    pub coverage: IndexCoverage,
    /// `(composite_key, primary_key)` pairs, exactly as exported — the key is
    /// the already-built one, never re-derived from split-apart field values.
    pub entries: &'a [(Vec<u8>, Vec<u8>)],
}

impl KvEngine {
    /// Reinstate a single-field secondary index and its content.
    pub fn restore_field_index(&mut self, params: RestoreFieldIndexParams<'_>) {
        let RestoreFieldIndexParams {
            database_id,
            tenant_id,
            collection,
            field,
            field_position,
            coverage,
            entries,
        } = params;
        let tkey = self.name_collection(database_id, tenant_id, collection);

        let idx_set = self.indexes.entry(tkey).or_default();
        idx_set.add_index(field, field_position, coverage);
        // `add_index` reports `false` for an already-registered field. That is
        // not a failure here: the registration is what had to exist, and
        // refilling the index it returned is what this call is for.
        let Some(index) = idx_set.get_index_mut(field) else {
            return;
        };
        for (value, primary_key) in entries {
            index.insert(value.clone(), primary_key.clone());
        }
    }

    /// Reinstate a composite secondary index and its content.
    pub fn restore_composite_index(&mut self, params: RestoreCompositeIndexParams<'_>) {
        let RestoreCompositeIndexParams {
            database_id,
            tenant_id,
            collection,
            fields,
            field_positions,
            coverage,
            entries,
        } = params;
        let tkey = self.name_collection(database_id, tenant_id, collection);

        let idx_set = self.indexes.entry(tkey).or_default();
        idx_set.add_composite_index(fields.to_vec(), field_positions.to_vec(), coverage);
        let Some(index) = idx_set.get_composite_index_mut(fields) else {
            return;
        };
        for (composite_key, primary_key) in entries {
            index.insert_raw(composite_key.clone(), primary_key.clone());
        }
    }

    /// Reinstate a sorted index and its content.
    pub fn restore_sorted_index(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        def: SortedIndexDef,
        entries: &[(Vec<u8>, Vec<u8>)],
    ) {
        self.name_collection(database_id, tenant_id, &def.collection);
        self.sorted_indexes
            .restore(database_id, tenant_id, def, entries);
    }

    /// Record a collection's identity against its hashed table key and return
    /// that key.
    ///
    /// A restored collection may have no rows at all — the checkpoint carries
    /// index-only collections — and the reverse maps are what let the NEXT
    /// checkpoint find and name it again.
    fn name_collection(&mut self, database_id: u64, tenant_id: u64, collection: &str) -> u64 {
        let tkey = table_key(database_id, tenant_id, collection);
        self.register_table_identity(tkey, database_id, tenant_id, collection);
        tkey
    }
}
