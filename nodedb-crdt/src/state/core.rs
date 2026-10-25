// SPDX-License-Identifier: Apache-2.0

//! CrdtState core: document handle, row CRUD, uniqueness probes.

use std::cell::Cell;
use std::collections::HashSet;
use std::marker::PhantomData;

use loro::{LoroDoc, LoroMap, LoroValue, ValueOrContainer};

use crate::error::{CrdtError, Result};
use crate::row_lookup::RowLookup;
use crate::validator::bitemporal::{VALID_UNTIL, VALID_UNTIL_OPEN};

use super::document_cell::DocumentCell;

/// A row is live when its `_ts_valid_until` field is absent, null, or the
/// open sentinel (`i64::MAX`). Rows with any finite `_ts_valid_until` are
/// treated as superseded, independent of wall-clock time — the write path
/// sets finite `_ts_valid_until` only when explicitly terminating a version.
fn row_is_live(row: &LoroMap) -> bool {
    match row.get(VALID_UNTIL) {
        None => true,
        Some(ValueOrContainer::Value(LoroValue::Null)) => true,
        Some(ValueOrContainer::Value(LoroValue::I64(n))) => n == VALID_UNTIL_OPEN,
        _ => true,
    }
}

/// True when `key` currently holds a container (map/list/text/etc.) value
/// on `row`, rather than a plain scalar. Shared by `upsert`'s delete-set
/// filter and its scalar-write guard so both use one definition of
/// "container-valued key".
fn key_is_container(row: &LoroMap, key: &str) -> bool {
    matches!(row.get(key), Some(ValueOrContainer::Container(_)))
}

/// A CRDT state for a single collection — owns one `LoroDoc`.
///
/// Container naming inside the doc still uses `doc.get_map(collection)` so the
/// on-the-wire container layout matches across Origin and Lite and a raw Loro
/// `import` of a peer's delta merges into the same container.
pub struct CrdtState {
    /// Kept private to this state module so callers cannot clone or share a
    /// raw Loro handle around the preview's pending-check/fork critical section.
    pub(in crate::state) doc: DocumentCell,
    pub(super) peer_id: u64,
    /// Loro auto-commit state must stay single-owner. This makes accidental
    /// cross-thread sharing of a `CrdtState` a compile error.
    pub(super) _single_owner: PhantomData<Cell<()>>,
}

impl CrdtState {
    /// Create a new empty state for the given peer.
    pub fn new(peer_id: u64) -> Result<Self> {
        Ok(Self {
            doc: DocumentCell::new(Self::new_doc(peer_id)?),
            peer_id,
            _single_owner: PhantomData,
        })
    }

    /// Load a state from an encoded document this process produced itself — a
    /// snapshot read back from durable storage, or a pre-image exported moments
    /// earlier for transaction rollback.
    ///
    /// Pairs `new` with [`CrdtState::import_local`], because those two steps
    /// belong together: a caller that assembles them by hand has to know that
    /// its own bytes must not go through the peer ceilings, and a caller that
    /// gets that wrong writes a document it can no longer open. See
    /// `import_local` for why the ceilings do not apply here.
    pub fn from_local_snapshot(peer_id: u64, snapshot: &[u8]) -> Result<Self> {
        let state = Self::new(peer_id)?;
        state.import_local(snapshot)?;
        Ok(state)
    }

    /// A fresh Loro document bound to `peer_id`.
    ///
    /// Shared by `new` and by the compaction paths, which need the same
    /// peer-bound document before loading a shallow snapshot into it.
    pub(in crate::state) fn new_doc(peer_id: u64) -> Result<LoroDoc> {
        let doc = LoroDoc::new();
        doc.set_peer_id(peer_id)
            .map_err(|e| CrdtError::Loro(format!("failed to set peer_id {peer_id}: {e}")))?;
        Ok(doc)
    }

    /// Fetch a row's existing `LoroMap` container, or create one if absent.
    /// Shared by `upsert` and `set_fields` — both need the same row handle
    /// before diverging on prune-vs-preserve semantics.
    fn row_container(doc: &LoroDoc, collection: &str, row_id: &str) -> Result<LoroMap> {
        let coll = doc.get_map(collection);
        match coll.get(row_id) {
            Some(ValueOrContainer::Container(loro::Container::Map(m))) => Ok(m),
            _ => coll
                .insert_container(row_id, LoroMap::new())
                .map_err(|e| CrdtError::Loro(e.to_string())),
        }
    }

    /// Write `fields` onto `row_container` as scalar LWW inserts, rejecting
    /// any key that currently holds a container value. Shared by `upsert`
    /// and `set_fields` — both write the same way, only the prune step
    /// (upsert-only) differs.
    fn write_scalar_fields(
        row_container: &LoroMap,
        collection: &str,
        row_id: &str,
        fields: &[(&str, LoroValue)],
    ) -> Result<()> {
        for (field, value) in fields {
            // A container-valued key can never legitimately appear in the
            // incoming scalar projection. Overwriting one would destroy the
            // nested container; skipping it would silently discard the
            // caller's write. Reject instead of doing either.
            if key_is_container(row_container, field) {
                return Err(CrdtError::ScalarFieldShadowsContainer {
                    collection: collection.to_string(),
                    row_id: row_id.to_string(),
                    field: (*field).to_string(),
                });
            }
            row_container
                .insert(field, value.clone())
                .map_err(|e| CrdtError::Loro(e.to_string()))?;
        }
        Ok(())
    }

    /// The body of [`CrdtState::upsert`], run inside an open mutation.
    ///
    /// `upsert_versioned` calls it in the same mutation as its archive
    /// write, so the archive entry and the new row commit together.
    pub(in crate::state) fn replace_row_fields(
        doc: &LoroDoc,
        collection: &str,
        row_id: &str,
        fields: &[(&str, LoroValue)],
    ) -> Result<()> {
        let row_container = Self::row_container(doc, collection, row_id)?;

        let incoming_keys: HashSet<&str> = fields.iter().map(|(field, _)| *field).collect();

        // Full-projection replace, computed from the row's current live
        // keys on every call — never assumed from caller discipline.
        // Container-valued keys are excluded: they are never part of the
        // scalar projection callers pass, so deleting them here would
        // silently discard nested CRDT state (e.g. a row's block list).
        let keys_to_delete: Vec<String> = row_container
            .keys()
            .filter(|key| {
                !incoming_keys.contains(key.as_ref()) && !key_is_container(&row_container, key)
            })
            .map(|key| key.to_string())
            .collect();
        for key in &keys_to_delete {
            row_container
                .delete(key)
                .map_err(|e| CrdtError::Loro(e.to_string()))?;
        }

        Self::write_scalar_fields(&row_container, collection, row_id, fields)
    }

    /// Insert or update a row in a collection.
    ///
    /// This is a REPLACE for scalar fields — every caller passes the
    /// complete scalar projection, and any current scalar key absent from
    /// `fields` is deleted. It reuses the row's existing `LoroMap` rather
    /// than destroying and recreating it, because container-valued keys
    /// (e.g. the Notion-style block list in `list_ops.rs`, stored as a
    /// container-valued key inside this same row map) cannot be expressed in
    /// `fields: &[(&str, LoroValue)]` at all — they are structurally out of
    /// scope for this replace and must survive across every call.
    ///
    /// The write commits on return, on success and on error.
    pub fn upsert(
        &self,
        collection: &str,
        row_id: &str,
        fields: &[(&str, LoroValue)],
    ) -> Result<()> {
        self.doc
            .mutate(|doc| Self::replace_row_fields(doc, collection, row_id, fields))
    }

    /// Partial-merge write: set exactly the provided scalar `fields` on a row
    /// (LWW-per-field), creating the row if absent, leaving every untouched
    /// key intact. This is `upsert` WITHOUT the full-projection prune step —
    /// the UPDATE-SET semantic for `CrdtOp::DocUpsert { partial: true }`.
    ///
    /// The write commits on return, on success and on error.
    pub fn set_fields(
        &self,
        collection: &str,
        row_id: &str,
        fields: &[(&str, LoroValue)],
    ) -> Result<()> {
        self.doc.mutate(|doc| {
            let row_container = Self::row_container(doc, collection, row_id)?;
            Self::write_scalar_fields(&row_container, collection, row_id, fields)
        })
    }

    /// Delete a row from a collection. The delete commits on return.
    pub fn delete(&self, collection: &str, row_id: &str) -> Result<()> {
        self.doc.mutate(|doc| {
            doc.get_map(collection)
                .delete(row_id)
                .map_err(|e| CrdtError::Loro(e.to_string()))
        })
    }

    /// Insert a block-map into one row's movable list and populate scalar fields.
    /// The raw document handle never leaves this state object. The insert
    /// commits on return, on success and on error.
    pub fn list_insert_fields(
        &self,
        collection: &str,
        row_id: &str,
        list_path: &str,
        index: usize,
        fields: &[(String, LoroValue)],
    ) -> Result<()> {
        self.doc.mutate(|doc| {
            let block =
                crate::list_ops::list_insert_container(doc, collection, row_id, list_path, index)?;
            for (key, value) in fields {
                block
                    .insert(key.as_str(), value.clone())
                    .map_err(|error| CrdtError::Loro(error.to_string()))?;
            }
            Ok(())
        })
    }

    /// Delete one block from a row-owned movable list. The delete commits on
    /// return.
    pub fn list_delete(
        &self,
        collection: &str,
        row_id: &str,
        list_path: &str,
        index: usize,
    ) -> Result<()> {
        self.doc
            .mutate(|doc| crate::list_ops::list_delete(doc, collection, row_id, list_path, index))
    }

    /// Move one block within a row-owned movable list. The move commits on
    /// return.
    pub fn list_move(
        &self,
        collection: &str,
        row_id: &str,
        list_path: &str,
        from_index: usize,
        to_index: usize,
    ) -> Result<()> {
        self.doc.mutate(|doc| {
            crate::list_ops::list_move(doc, collection, row_id, list_path, from_index, to_index)
        })
    }

    /// Return one row-owned movable list's length.
    pub fn list_length(&self, collection: &str, row_id: &str, list_path: &str) -> Result<usize> {
        crate::list_ops::list_length(&self.doc, collection, row_id, list_path)
    }

    /// Read one value from a row-owned movable list without exposing its doc.
    pub fn list_get(
        &self,
        collection: &str,
        row_id: &str,
        list_path: &str,
        index: usize,
    ) -> Result<Option<LoroValue>> {
        crate::list_ops::list_get(&self.doc, collection, row_id, list_path, index)
    }

    /// Delete all rows in a collection. Returns the number of rows deleted.
    /// The deletes commit on return, on success and on error.
    pub fn clear_collection(&self, collection: &str) -> Result<usize> {
        self.doc.mutate(|doc| {
            let coll = doc.get_map(collection);
            let keys: Vec<String> = coll.keys().map(|k| k.to_string()).collect();
            for key in &keys {
                coll.delete(key)
                    .map_err(|e| CrdtError::Loro(e.to_string()))?;
            }
            Ok(keys.len())
        })
    }

    /// Read a single row's fields as a `LoroValue::Map`.
    ///
    /// Navigates via `LoroMap::get()` to avoid the expensive recursive
    /// `get_deep_value()` clone on the entire row container.
    pub fn read_row(&self, collection: &str, row_id: &str) -> Option<LoroValue> {
        let coll = self.doc.get_map(collection);
        match coll.get(row_id)? {
            ValueOrContainer::Container(loro::Container::Map(m)) => Some(m.get_value()),
            ValueOrContainer::Container(loro::Container::List(l)) => Some(l.get_value()),
            ValueOrContainer::Container(_) => Some(LoroValue::Null),
            ValueOrContainer::Value(v) => Some(v),
        }
    }

    /// Read a single field from a row without cloning the entire row.
    ///
    /// This is the fast path for KV-style access where only one field
    /// is needed. Avoids allocating a full Map for single-field reads.
    ///
    /// Shares the same `doc.get_map(collection).get(row_id)` lookup pattern
    /// as `read_row`, but returns a single field value instead of the whole
    /// row map — different return granularity, intentionally kept separate.
    pub fn read_field(&self, collection: &str, row_id: &str, field: &str) -> Option<LoroValue> {
        let coll = self.doc.get_map(collection);
        let row_map = match coll.get(row_id)? {
            ValueOrContainer::Container(loro::Container::Map(m)) => m,
            ValueOrContainer::Value(v) => return Some(v),
            _ => return None,
        };
        match row_map.get(field)? {
            ValueOrContainer::Value(v) => Some(v),
            ValueOrContainer::Container(loro::Container::Map(m)) => Some(m.get_value()),
            ValueOrContainer::Container(loro::Container::List(l)) => Some(l.get_value()),
            ValueOrContainer::Container(_) => Some(LoroValue::Null),
        }
    }

    /// Check if a row exists in this collection's Loro document.
    pub fn row_exists(&self, collection: &str, row_id: &str) -> bool {
        let coll = self.doc.get_map(collection);
        coll.get(row_id).is_some()
    }

    /// List all collection names (top-level map keys in the Loro doc).
    ///
    /// Reads the shallow value: the keys are at the top level, and
    /// `get_deep_value` would materialise every row and field of every
    /// collection to reach them — O(document) for a list whose size is the
    /// number of collections. Name resolution calls this per query.
    pub fn collection_names(&self) -> Vec<String> {
        match self.doc.get_value() {
            LoroValue::Map(map) => map.keys().map(|k| k.to_string()).collect(),
            _ => Vec::new(),
        }
    }

    /// Get all row IDs in a collection.
    pub fn row_ids(&self, collection: &str) -> Vec<String> {
        let coll = self.doc.get_map(collection);
        coll.keys().map(|k| k.to_string()).collect()
    }

    /// Check if a value exists for the given field across all rows in a collection.
    /// Used for UNIQUE constraint checking.
    ///
    /// When `exclude_row_id` is `Some`, the row with that id is skipped so a row
    /// does not collide with its own already-committed version. `None` scans
    /// every row.
    pub fn field_value_exists(
        &self,
        collection: &str,
        field: &str,
        value: &LoroValue,
        exclude_row_id: Option<&str>,
    ) -> bool {
        let coll = self.doc.get_map(collection);
        for key in coll.keys() {
            if exclude_row_id == Some(key.as_ref()) {
                continue;
            }
            // Reached through the row container rather than a built path.
            // `get_by_str_path` formats a `collection/row/field` string and
            // re-resolves it from the document root for every row, which turns
            // one UNIQUE probe into a per-row allocation plus a path parse and
            // walk — the dominant cost of validating a single row against a
            // large collection. The containers here are already in hand.
            let Some(ValueOrContainer::Container(loro::Container::Map(row))) = coll.get(&key)
            else {
                continue;
            };
            if let Some(ValueOrContainer::Value(field_val)) = row.get(field)
                && &field_val == value
            {
                return true;
            }
        }
        false
    }

    /// Bitemporal variant of [`field_value_exists`]: only considers rows
    /// whose `_ts_valid_until` is open (absent or `i64::MAX`).
    ///
    /// A UNIQUE collision between a superseded version and a new live row
    /// is not a violation — both may share the same value because they
    /// represent the same logical entity at different valid-times.
    pub fn field_value_exists_live(
        &self,
        collection: &str,
        field: &str,
        value: &LoroValue,
        exclude_row_id: Option<&str>,
    ) -> bool {
        let coll = self.doc.get_map(collection);
        for key in coll.keys() {
            if exclude_row_id == Some(key.as_ref()) {
                continue;
            }
            let row_map = match coll.get(&key) {
                Some(ValueOrContainer::Container(loro::Container::Map(m))) => m,
                _ => continue,
            };
            if !row_is_live(&row_map) {
                continue;
            }
            let field_val = match row_map.get(field) {
                Some(ValueOrContainer::Value(v)) => v,
                _ => continue,
            };
            if &field_val == value {
                return true;
            }
        }
        false
    }

    /// Return row IDs currently "live" in a bitemporal collection
    /// (rows whose `_ts_valid_until` is open). For non-bitemporal
    /// collections every row is returned.
    pub fn live_row_ids(&self, collection: &str) -> Vec<String> {
        let coll = self.doc.get_map(collection);
        let mut out = Vec::new();
        for key in coll.keys() {
            let row_map = match coll.get(&key) {
                Some(ValueOrContainer::Container(loro::Container::Map(m))) => m,
                _ => continue,
            };
            if row_is_live(&row_map) {
                out.push(key.to_string());
            }
        }
        out
    }

    /// Peer ID of this state.
    pub fn peer_id(&self) -> u64 {
        self.peer_id
    }

    /// Mint every later operation under `peer_id`. Operations already in the
    /// document keep the ids they were minted with.
    pub fn set_peer_id(&mut self, peer_id: u64) -> Result<()> {
        self.doc
            .set_peer_id(peer_id)
            .map_err(|e| CrdtError::Loro(format!("failed to set peer_id {peer_id}: {e}")))?;
        self.peer_id = peer_id;
        Ok(())
    }
}

impl RowLookup for CrdtState {
    fn row_exists(&self, collection: &str, row_id: &str) -> bool {
        self.row_exists(collection, row_id)
    }

    fn field_value_exists(
        &self,
        collection: &str,
        field: &str,
        value: &LoroValue,
        exclude_row_id: Option<&str>,
    ) -> bool {
        self.field_value_exists(collection, field, value, exclude_row_id)
    }

    fn field_value_exists_live(
        &self,
        collection: &str,
        field: &str,
        value: &LoroValue,
        exclude_row_id: Option<&str>,
    ) -> bool {
        self.field_value_exists_live(collection, field, value, exclude_row_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const COLL: &str = "c";
    const ROW: &str = "r";

    #[test]
    fn set_fields_preserves_untouched_keys_and_upsert_prunes() {
        let state = CrdtState::new(0).expect("state");

        // Full projection {a:1, b:2}.
        state
            .upsert(
                COLL,
                ROW,
                &[("a", LoroValue::I64(1)), ("b", LoroValue::I64(2))],
            )
            .expect("upsert");

        // Partial-merge {b:9}: `a` must survive untouched, `b` overwritten.
        state
            .set_fields(COLL, ROW, &[("b", LoroValue::I64(9))])
            .expect("set_fields");
        assert_eq!(
            state.read_field(COLL, ROW, "a"),
            Some(LoroValue::I64(1)),
            "set_fields must leave the untouched key `a` intact"
        );
        assert_eq!(
            state.read_field(COLL, ROW, "b"),
            Some(LoroValue::I64(9)),
            "set_fields must overwrite `b` to 9"
        );

        // Full-projection replace {a:5}: absent key `b` must be pruned.
        state
            .upsert(COLL, ROW, &[("a", LoroValue::I64(5))])
            .expect("upsert replace");
        assert_eq!(
            state.read_field(COLL, ROW, "a"),
            Some(LoroValue::I64(5)),
            "upsert must set `a` to 5"
        );
        assert_eq!(
            state.read_field(COLL, ROW, "b"),
            None,
            "upsert must prune key `b` absent from the projection"
        );
    }

    #[test]
    fn upsert_and_check_existence() {
        let state = CrdtState::new(1).unwrap();
        state
            .upsert(
                "users",
                "user-1",
                &[
                    ("name", LoroValue::String("Alice".into())),
                    ("email", LoroValue::String("alice@example.com".into())),
                ],
            )
            .unwrap();

        assert!(state.row_exists("users", "user-1"));
        assert!(!state.row_exists("users", "user-2"));
    }

    #[test]
    fn delete_row() {
        let state = CrdtState::new(1).unwrap();
        state
            .upsert(
                "users",
                "user-1",
                &[("name", LoroValue::String("Alice".into()))],
            )
            .unwrap();

        assert!(state.row_exists("users", "user-1"));
        state.delete("users", "user-1").unwrap();
        assert!(!state.row_exists("users", "user-1"));
    }

    #[test]
    fn row_ids_listing() {
        let state = CrdtState::new(1).unwrap();
        state
            .upsert("users", "a", &[("x", LoroValue::I64(1))])
            .unwrap();
        state
            .upsert("users", "b", &[("x", LoroValue::I64(2))])
            .unwrap();

        let mut ids = state.row_ids("users");
        ids.sort();
        assert_eq!(ids, vec!["a", "b"]);
    }

    #[test]
    fn field_value_uniqueness_check() {
        let state = CrdtState::new(1).unwrap();
        state
            .upsert(
                "users",
                "u1",
                &[("email", LoroValue::String("alice@example.com".into()))],
            )
            .unwrap();

        assert!(state.field_value_exists(
            "users",
            "email",
            &LoroValue::String("alice@example.com".into()),
            None,
        ));
        assert!(!state.field_value_exists(
            "users",
            "email",
            &LoroValue::String("bob@example.com".into()),
            None,
        ));
    }

    #[test]
    fn field_value_exists_self_exclusion() {
        let state = CrdtState::new(1).unwrap();
        state
            .upsert(
                "users",
                "u1",
                &[("email", LoroValue::String("a@example.com".into()))],
            )
            .unwrap();

        let value = LoroValue::String("a@example.com".into());

        // Re-validating the very row that holds the value, while excluding it,
        // must NOT report a collision — a committed row cannot conflict with its
        // own just-written version.
        assert!(!state.field_value_exists("users", "email", &value, Some("u1")));

        // A DIFFERENT row carrying the same value (excluding only itself) still
        // collides with the existing u1.
        assert!(state.field_value_exists("users", "email", &value, Some("u2")));

        // With no exclusion the value is plainly present.
        assert!(state.field_value_exists("users", "email", &value, None));
    }

    #[test]
    fn field_value_exists_live_self_exclusion() {
        let state = CrdtState::new(1).unwrap();
        // Live rows (no `_ts_valid_until`) — the live probe treats them as active.
        state
            .upsert(
                "users",
                "u1",
                &[("email", LoroValue::String("a@example.com".into()))],
            )
            .unwrap();

        let value = LoroValue::String("a@example.com".into());

        // Excluding the holder row → no self-collision.
        assert!(!state.field_value_exists_live("users", "email", &value, Some("u1")));

        // A distinct row with the same value → collision against the live u1.
        assert!(state.field_value_exists_live("users", "email", &value, Some("u2")));

        assert!(state.field_value_exists_live("users", "email", &value, None));
    }

    #[test]
    fn compact_history_preserves_state() {
        let mut state = CrdtState::new(1).unwrap();
        // Create some state with history.
        state
            .upsert(
                "users",
                "u1",
                &[("name", LoroValue::String("Alice".into()))],
            )
            .unwrap();
        state
            .upsert("users", "u2", &[("name", LoroValue::String("Bob".into()))])
            .unwrap();
        // Update to create more history.
        state
            .upsert(
                "users",
                "u1",
                &[("name", LoroValue::String("Alice Updated".into()))],
            )
            .unwrap();

        // Compact.
        state.compact_history().unwrap();

        // State should be preserved after compaction.
        assert!(state.row_exists("users", "u1"));
        assert!(state.row_exists("users", "u2"));

        // New operations should still work.
        state
            .upsert(
                "users",
                "u3",
                &[("name", LoroValue::String("Carol".into()))],
            )
            .unwrap();
        assert!(state.row_exists("users", "u3"));
    }

    #[test]
    fn estimated_memory_grows_with_data() {
        let state = CrdtState::new(1).unwrap();
        let before = state.estimated_memory_bytes();

        for i in 0..100 {
            state
                .upsert(
                    "items",
                    &format!("item-{i}"),
                    &[("value", LoroValue::I64(i))],
                )
                .unwrap();
        }

        let after = state.estimated_memory_bytes();
        assert!(
            after > before,
            "memory should grow: before={before}, after={after}"
        );
    }

    #[test]
    fn estimated_memory_follows_compaction() {
        let mut state = CrdtState::new(1).unwrap();
        // History, not breadth: compaction discards the operations, so the same
        // row rewritten many times is what shrinks when it runs.
        for i in 0..512 {
            state
                .upsert("items", "hot", &[("value", LoroValue::I64(i))])
                .unwrap();
        }
        let before = state.estimated_memory_bytes();

        state.compact_history().unwrap();
        let after = state.estimated_memory_bytes();

        assert_eq!(
            after,
            state.export_snapshot().unwrap().len(),
            "the estimate must describe the document that exists now, not the one \
             compaction replaced; a shallow snapshot keeps the version vector, so a \
             measurement keyed on the version alone still looks current after the \
             bytes it measured are gone"
        );
        assert!(
            after < before,
            "compaction dropped 512 operations and the estimate did not move: \
             before={before}, after={after} — a caller polling this to decide when \
             to compact would compact forever"
        );
    }

    #[test]
    fn estimated_memory_does_not_re_encode_on_every_write() {
        let state = CrdtState::new(1).unwrap();
        for i in 0..2_000 {
            state
                .upsert("items", &format!("i-{i}"), &[("v", LoroValue::I64(i))])
                .unwrap();
            state.estimated_memory_bytes();
        }

        // The estimate is called after every write by the memory governor. Paying
        // a full document re-encode per call is what made a large store take
        // ~0.5 s per record; the calibrated ratio has to make that logarithmic in
        // the number of writes, not linear.
        let exports = state.export_count_for_test();
        assert!(
            exports < 32,
            "{exports} full exports for 2000 writes — the estimate is re-encoding \
             the document per write again"
        );
    }

    #[test]
    fn estimated_memory_stays_close_to_the_real_encoded_size() {
        let state = CrdtState::new(1).unwrap();
        for i in 0..2_000 {
            state
                .upsert(
                    "items",
                    &format!("i-{i}"),
                    &[("v", LoroValue::String("payload".repeat(4).into()))],
                )
                .unwrap();
            state.estimated_memory_bytes();
        }

        // Interpolating between calibrations trades exactness for cost. It is a
        // pressure signal, so it may drift — but it has to stay the same order as
        // the truth, or the thresholds built on it mean nothing.
        let estimate = state.estimated_memory_bytes() as f64;
        let actual = state.export_snapshot().unwrap().len() as f64;
        let ratio = estimate / actual;
        assert!(
            (0.5..=2.0).contains(&ratio),
            "estimate {estimate} vs actual {actual} (ratio {ratio:.2}) — drifted \
             beyond what a pressure signal can carry"
        );
    }

    #[test]
    fn oplog_version_counts_uncommitted_operations() {
        let state = CrdtState::new(1).unwrap();
        // Every mutator commits on return, so the raw handle is the only way
        // to hold an operation in an open transaction.
        state
            .doc
            .get_map("items")
            .insert("a", LoroValue::I64(1))
            .unwrap();
        assert_ne!(state.doc.get_pending_txn_len(), 0);

        // The memory estimate is cached against this version vector. That is only
        // sound because Loro advances it when the operation is written rather than
        // when its transaction commits — otherwise a write could land while the
        // key stood still, and the estimate would answer from before it. If this
        // assertion ever fails, the cache in `document_cell` is what breaks.
        assert!(
            state.oplog_version_vector().get(&1).copied().unwrap_or(0) > 0,
            "an operation that has not committed yet must still move the version"
        );
    }

    #[test]
    fn restore_to_version_produces_forward_delta() {
        let state = CrdtState::new(1).unwrap();
        state
            .upsert("docs", "doc1", &[("body", LoroValue::String("v1".into()))])
            .unwrap();
        let vv1 = state.oplog_version_vector();
        state
            .upsert("docs", "doc1", &[("body", LoroValue::String("v2".into()))])
            .unwrap();

        // Snapshot the pre-restore state: a forward delta exported from `vv_before`
        // carries only the ops after it, so it is meaningful solely to a peer that
        // already holds everything up to that point. Replay works the same way —
        // it imports every delta in order from empty.
        let pre_restore = state.export_snapshot().unwrap();

        let delta = state.restore_to_version("docs", "doc1", &vv1).unwrap();
        assert!(
            !delta.is_empty(),
            "restoring to a genuinely earlier version must produce a non-empty forward delta"
        );

        // The live document carries the restored value.
        assert_eq!(
            state.read_field("docs", "doc1", "body"),
            Some(LoroValue::String("v1".into()))
        );

        // A peer caught up to the pre-restore state converges on the restored value
        // once it imports the delta.
        let peer = CrdtState::new(2).unwrap();
        peer.import(&pre_restore).unwrap();
        peer.import(&delta).unwrap();
        assert_eq!(
            peer.read_field("docs", "doc1", "body"),
            Some(LoroValue::String("v1".into()))
        );
    }

    #[test]
    fn restore_to_current_version_is_empty() {
        let state = CrdtState::new(1).unwrap();
        state
            .upsert("docs", "doc1", &[("body", LoroValue::String("v1".into()))])
            .unwrap();
        let current = state.oplog_version_vector();

        let delta = state.restore_to_version("docs", "doc1", &current).unwrap();
        assert!(
            delta.is_empty(),
            "restoring to the version a document is already at must return a genuinely empty delta, \
             not a header-only export"
        );
    }

    /// Attach a nested `blocks` `LoroMovableList` directly onto an existing row,
    /// mirroring how `list_ops.rs` stores a Notion-style block list as a
    /// container-valued key inside the row `LoroMap`. Returns the single block's
    /// id so callers can assert it survived.
    fn attach_nested_block_list(state: &CrdtState, collection: &str, row_id: &str) {
        state
            .list_insert_fields(
                collection,
                row_id,
                "blocks",
                0,
                &[("id".into(), LoroValue::String("blk-0".into()))],
            )
            .unwrap();
    }

    #[test]
    fn upsert_preserves_nested_movable_list_across_disjoint_scalar_upsert() {
        let state = CrdtState::new(1).unwrap();
        state
            .upsert(
                "pages",
                "doc-1",
                &[("title", LoroValue::String("Draft".into()))],
            )
            .unwrap();

        attach_nested_block_list(&state, "pages", "doc-1");

        // A later upsert with a completely disjoint scalar field set must not
        // destroy the nested "blocks" container.
        state
            .upsert(
                "pages",
                "doc-1",
                &[("status", LoroValue::String("published".into()))],
            )
            .unwrap();

        let len = state.list_length("pages", "doc-1", "blocks").unwrap();
        assert_eq!(len, 1, "nested block list must survive an unrelated upsert");

        let val = state
            .list_get("pages", "doc-1", "blocks", 0)
            .unwrap()
            .unwrap();
        if let LoroValue::Map(map) = val {
            assert_eq!(map.get("id"), Some(&LoroValue::String("blk-0".into())));
        } else {
            panic!("expected block map with fields intact, got {val:?}");
        }
    }

    #[test]
    fn upsert_still_replaces_absent_scalar_fields() {
        let state = CrdtState::new(1).unwrap();
        state
            .upsert(
                "users",
                "u1",
                &[
                    ("name", LoroValue::String("Alice".into())),
                    ("email", LoroValue::String("alice@example.com".into())),
                ],
            )
            .unwrap();

        // Second upsert omits "email" — the row must end up WITHOUT it. This
        // pins that reusing the row's LoroMap (instead of destroying and
        // recreating it) did not turn `upsert` into a merge.
        state
            .upsert(
                "users",
                "u1",
                &[("name", LoroValue::String("Alice Updated".into()))],
            )
            .unwrap();

        assert_eq!(
            state.read_field("users", "u1", "name"),
            Some(LoroValue::String("Alice Updated".into()))
        );
        assert_eq!(
            state.read_field("users", "u1", "email"),
            None,
            "a field absent from the latest upsert must be gone, not merged"
        );
    }

    #[test]
    fn restore_to_version_preserves_nested_movable_list_as_live_container() {
        let state = CrdtState::new(1).unwrap();
        state
            .upsert(
                "pages",
                "doc-1",
                &[("title", LoroValue::String("v1".into()))],
            )
            .unwrap();

        attach_nested_block_list(&state, "pages", "doc-1");
        let vv_with_blocks = state.oplog_version_vector();

        // Move the row forward — Fix 1 keeps "blocks" alive across this upsert.
        state
            .upsert(
                "pages",
                "doc-1",
                &[("title", LoroValue::String("v2".into()))],
            )
            .unwrap();

        let delta = state
            .restore_to_version("pages", "doc-1", &vv_with_blocks)
            .unwrap();
        assert!(
            !delta.is_empty(),
            "restoring to a genuinely earlier version must produce a forward delta"
        );

        assert_eq!(
            state.read_field("pages", "doc-1", "title"),
            Some(LoroValue::String("v1".into()))
        );

        // The restored block list must be a live, queryable CRDT container —
        // not a flattened/dangling value produced by routing the historical
        // container through the scalar `insert` path.
        let len = state.list_length("pages", "doc-1", "blocks").unwrap();
        assert_eq!(len, 1);
        let val = state
            .list_get("pages", "doc-1", "blocks", 0)
            .unwrap()
            .unwrap();
        if let LoroValue::Map(map) = val {
            assert_eq!(map.get("id"), Some(&LoroValue::String("blk-0".into())));
        } else {
            panic!("expected block map with container identity preserved, got {val:?}");
        }
    }

    #[test]
    fn snapshot_roundtrip() {
        let state1 = CrdtState::new(1).unwrap();
        state1
            .upsert("users", "u1", &[("name", LoroValue::String("Bob".into()))])
            .unwrap();

        let snapshot = state1.export_snapshot().unwrap();

        let state2 = CrdtState::new(2).unwrap();
        state2.import(&snapshot).unwrap();

        assert!(state2.row_exists("users", "u1"));
    }

    #[test]
    fn collection_names_lists_top_level_keys_only() {
        let state = CrdtState::new(1).unwrap();
        state
            .upsert("users", "u1", &[("name", LoroValue::String("Bob".into()))])
            .unwrap();
        state
            .upsert("orders", "o1", &[("total", LoroValue::I64(42))])
            .unwrap();

        let mut names = state.collection_names();
        names.sort();
        assert_eq!(
            names,
            vec!["orders".to_string(), "users".to_string()],
            "the collections are the top-level keys; row ids and fields belong to \
             the collections, not to this list"
        );
    }

    /// The operation count this state has authored, after checking that the
    /// write named `what` left no open Loro transaction and authored at
    /// least one operation past `before`.
    fn committed(state: &CrdtState, before: i32, what: &str) -> i32 {
        assert_eq!(
            state.doc.get_pending_txn_len(),
            0,
            "{what} left its operations in an open Loro transaction"
        );
        let after = state.local_op_counter();
        assert!(after > before, "{what} authored no operation");
        after
    }

    #[test]
    fn every_mutator_commits_before_it_returns() {
        let state = CrdtState::new(1).unwrap();
        let mut ops = state.local_op_counter();

        state
            .upsert(
                "docs",
                "r",
                &[("a", LoroValue::I64(1)), ("b", LoroValue::I64(2))],
            )
            .unwrap();
        ops = committed(&state, ops, "upsert");
        state
            .set_fields("docs", "r", &[("b", LoroValue::I64(3))])
            .unwrap();
        ops = committed(&state, ops, "set_fields");
        for (index, id) in ["b0", "b1"].into_iter().enumerate() {
            state
                .list_insert_fields(
                    "docs",
                    "r",
                    "blocks",
                    index,
                    &[("id".into(), LoroValue::String(id.into()))],
                )
                .unwrap();
            ops = committed(&state, ops, "list_insert_fields");
        }
        state.list_move("docs", "r", "blocks", 0, 1).unwrap();
        ops = committed(&state, ops, "list_move");
        state.list_delete("docs", "r", "blocks", 0).unwrap();
        ops = committed(&state, ops, "list_delete");
        assert_eq!(state.remove_fields("docs", "r", &["a"]).unwrap(), 1);
        ops = committed(&state, ops, "remove_fields");
        state
            .restore_row_image("docs", "r", &crate::state::RowImage::Absent)
            .unwrap();
        ops = committed(&state, ops, "restore_row_image");

        for sys_ms in [10, 20] {
            state
                .upsert_versioned(
                    "hist",
                    "h",
                    &[
                        ("_ts_system", LoroValue::I64(sys_ms)),
                        ("v", LoroValue::I64(sys_ms)),
                    ],
                )
                .unwrap();
            ops = committed(&state, ops, "upsert_versioned");
        }
        assert_eq!(state.archive_version_count("hist", "h"), 1);
        assert_eq!(state.purge_history_before("hist", 15).unwrap(), 1);
        ops = committed(&state, ops, "purge_history_before");

        state
            .upsert("docs", "r2", &[("v", LoroValue::I64(1))])
            .unwrap();
        ops = committed(&state, ops, "upsert");
        let version = state.oplog_version_vector();
        state
            .upsert("docs", "r2", &[("v", LoroValue::I64(2))])
            .unwrap();
        ops = committed(&state, ops, "upsert");
        let delta = state.restore_to_version("docs", "r2", &version).unwrap();
        assert!(!delta.is_empty());
        ops = committed(&state, ops, "restore_to_version");
        state.delete("docs", "r2").unwrap();
        ops = committed(&state, ops, "delete");
        assert_eq!(state.clear_collection("hist").unwrap(), 1);
        committed(&state, ops, "clear_collection");
    }

    #[test]
    fn a_version_taken_after_a_write_restores_through_preview() {
        let state = CrdtState::new(1).unwrap();
        state
            .upsert("docs", "doc", &[("title", LoroValue::String("v1".into()))])
            .unwrap();
        // A checkpoint reads the version right after the acknowledged write.
        let checkpoint = state.oplog_version_vector();
        assert!(
            state
                .preview_restore_to_version("docs", "doc", &checkpoint)
                .expect("a restore preview right after a write must not be refused")
                .is_empty(),
            "the document is already at the checkpoint"
        );

        state
            .set_fields("docs", "doc", &[("title", LoroValue::String("v2".into()))])
            .unwrap();
        let delta = state
            .preview_restore_to_version("docs", "doc", &checkpoint)
            .expect("preview after a later write");
        assert!(!delta.is_empty());
        state.import(&delta).unwrap();
        assert_eq!(
            state.read_field("docs", "doc", "title"),
            Some(LoroValue::String("v1".into())),
            "the checkpoint must hold the write acknowledged before it"
        );
    }

    #[test]
    fn a_scalar_write_refused_part_way_commits_and_its_row_image_undoes_it() {
        let state = CrdtState::new(1).unwrap();
        state
            .upsert("docs", "r", &[("a", LoroValue::I64(1))])
            .unwrap();
        attach_nested_block_list(&state, "docs", "r");
        let image = state.row_image("docs", "r").unwrap();

        // `a` is written before `blocks` is refused, for both scalar writes.
        let partial = [("a", LoroValue::I64(2)), ("blocks", LoroValue::I64(3))];
        for partial_merge in [true, false] {
            let refusal = if partial_merge {
                state.set_fields("docs", "r", &partial)
            } else {
                state.upsert("docs", "r", &partial)
            };
            assert!(matches!(
                refusal,
                Err(CrdtError::ScalarFieldShadowsContainer { ref field, .. }) if field == "blocks"
            ));
            assert_eq!(
                state.doc.get_pending_txn_len(),
                0,
                "a refused write must not leave its partial operations open"
            );
            assert_eq!(state.read_field("docs", "r", "a"), Some(LoroValue::I64(2)));

            state.restore_row_image("docs", "r", &image).unwrap();
            assert_eq!(state.doc.get_pending_txn_len(), 0);
            assert_eq!(state.read_field("docs", "r", "a"), Some(LoroValue::I64(1)));
            assert_eq!(state.list_length("docs", "r", "blocks").unwrap(), 1);
        }
    }
}
