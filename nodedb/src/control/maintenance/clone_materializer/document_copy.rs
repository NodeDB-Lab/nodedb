// SPDX-License-Identifier: BUSL-1.1

//! Row copy of one non-chained Document clone collection.
//!
//! Each source page is filtered, bound to target surrogates in one batch, and
//! inserted row by row with `PointInsert { if_absent: true }`. A progress
//! checkpoint follows each page.

use std::collections::HashSet;

use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
use nodedb_types::{DatabaseId, Lsn, Surrogate, TenantId};

use super::dispatch::dispatch_to_owner;
use super::document::{SourcePage, scan_page};
use super::status::{check_bound_surrogates, checkpoint_progress};
use crate::control::security::catalog::{StoredCollection, SystemCatalog};
use crate::control::state::SharedState;

/// One source row to copy, with the pk bytes it keys under.
struct PendingRow {
    doc_id_hex: String,
    pk_bytes: Vec<u8>,
    value_bytes: Vec<u8>,
}

/// The row copy of one non-chained Document clone collection.
pub(super) struct RowCopy<'a> {
    pub state: &'a SharedState,
    pub catalog: &'a SystemCatalog,
    pub db_id: DatabaseId,
    pub coll: &'a StoredCollection,
    pub tenant_id: TenantId,
    pub source_db_id: DatabaseId,
    pub source_collection: &'a str,
    pub source_qualified: String,
    pub target_qualified: &'a str,
    pub tombstoned: &'a HashSet<u32>,
    pub system_as_of_ms: Option<i64>,
    pub as_of_lsn: Lsn,
}

impl RowCopy<'_> {
    /// Copy every source page, with a progress checkpoint after each one.
    pub(super) async fn run(&self) -> crate::Result<()> {
        let mut cursor: Vec<u8> = Vec::new();
        let mut copied: u64 = 0;
        let mut total_seen: u64 = 0;
        loop {
            // The copy reads bodies as stored. A scan that adds `id`
            // hands a strict target a field its schema lacks, and a
            // declared-key target a second identity. `insert_row` writes the
            // source identity back only where the target keeps it in `id`.
            let page = SourcePage {
                tenant_id: self.tenant_id,
                source_db_id: self.source_db_id,
                source_qualified: &self.source_qualified,
                cursor: &cursor,
                system_time: super::document::clone_scope(self.system_as_of_ms),
                raw_bodies: true,
            };
            let (entries, next_cursor) = scan_page(self.state, page, None).await?;
            total_seen += entries.len() as u64;
            let pending = self.pending_rows(entries)?;
            copied += self.copy_rows(pending).await?;
            checkpoint_progress(self.state, self.coll, self.as_of_lsn, copied, total_seen).await?;
            if next_cursor.is_empty() {
                break;
            }
            cursor = next_cursor;
        }
        tracing::info!(
            db_id = self.db_id.as_u64(),
            collection = %self.coll.name,
            copied,
            skipped_tombstoned = self.tombstoned.len(),
            source_total = total_seen,
            "document materialize: source rows copied to target",
        );
        Ok(())
    }

    /// The rows of one page still to copy. A row deleted from the clone (CoW
    /// tombstone) or already copied up by the CoW write path is skipped.
    fn pending_rows(&self, entries: Vec<(String, u32, Vec<u8>)>) -> crate::Result<Vec<PendingRow>> {
        let mut pending = Vec::with_capacity(entries.len());
        for (doc_id_hex, source_surrogate, value_bytes) in entries {
            if self.tombstoned.contains(&source_surrogate)
                || self
                    .catalog
                    .get_clone_copyup(self.target_qualified, source_surrogate)?
                    .is_some()
            {
                continue;
            }
            let pk_bytes = self.source_pk_bytes(&doc_id_hex, source_surrogate)?;
            pending.push(PendingRow {
                doc_id_hex,
                pk_bytes,
                value_bytes,
            });
        }
        Ok(pending)
    }

    /// Recover the user-visible PK bytes from the catalog, so the surrogate
    /// assigner produces the same surrogate the write path allocates for this
    /// (collection, pk) pair.
    fn source_pk_bytes(&self, doc_id_hex: &str, source_surrogate: u32) -> crate::Result<Vec<u8>> {
        let pk_bytes = self
            .catalog
            .get_pk_for_surrogate(
                nodedb_types::CollectionKey::from_bare(self.source_db_id, self.source_collection),
                self.tenant_id,
                Surrogate::new(source_surrogate),
            )
            .map_err(|e| crate::Error::Storage {
                engine: "clone_materializer".into(),
                detail: format!(
                    "get_pk_for_surrogate failed for surrogate {source_surrogate} \
                     in '{}': {e}",
                    self.source_qualified
                ),
            })?;
        // No PK binding (e.g. very old row): the hex doc_id is the key bytes.
        // It is deterministic but can differ from the write path.
        Ok(pk_bytes.unwrap_or_else(|| doc_id_hex.as_bytes().to_vec()))
    }

    /// Bind target surrogates for `pending` and insert each row into the
    /// target. Returns the rows copied.
    async fn copy_rows(&self, pending: Vec<PendingRow>) -> crate::Result<u64> {
        // Target surrogates for the whole page in one batch at the target
        // collection's home, under the same (collection, pk_bytes) keys the
        // normal INSERT path uses.
        let pks: Vec<&[u8]> = pending.iter().map(|row| row.pk_bytes.as_slice()).collect();
        let target_surrogates =
            crate::control::server::surrogate_exchange::assign_surrogates_routed(
                self.state,
                nodedb_types::CollectionKey::from_bare(self.db_id, &self.coll.name),
                self.tenant_id,
                &pks,
                crate::types::TraceId::ZERO,
            )
            .await
            .map_err(|e| crate::Error::Storage {
                engine: "clone_materializer".into(),
                detail: format!(
                    "surrogate assign failed for a page of '{}': {e}",
                    self.target_qualified
                ),
            })?;
        check_bound_surrogates(
            self.target_qualified,
            target_surrogates.len(),
            pending.len(),
        )?;
        let mut copied = 0;
        for (row, target_surrogate) in pending.into_iter().zip(target_surrogates) {
            self.insert_row(row, target_surrogate).await?;
            copied += 1;
        }
        Ok(copied)
    }

    /// Insert one row into the target under `target_surrogate`, unless the
    /// target already holds it.
    async fn insert_row(&self, row: PendingRow, target_surrogate: Surrogate) -> crate::Result<()> {
        let PendingRow {
            doc_id_hex,
            pk_bytes,
            value_bytes,
        } = row;
        // The copy keeps the row's client identity under its new surrogate,
        // so a clone read matches it to its source row by primary key.
        let value_bytes = match nodedb_types::StorageKey::parse(&doc_id_hex) {
            Some(storage_key) => {
                let identity = nodedb_types::RowIdentity::of_stored_row(
                    &value_bytes,
                    self.coll.declared_primary_key.as_deref(),
                    storage_key,
                );
                crate::control::clone::identity::carry_identity(
                    self.coll,
                    value_bytes,
                    identity.as_str(),
                )
            }
            None => value_bytes,
        };
        let plan = PhysicalPlan::Document(DocumentOp::PointInsert {
            collection: nodedb_types::QualifiedCollection::new(self.db_id, &self.coll.name),
            document_id: String::from_utf8_lossy(&pk_bytes).into_owned(),
            value: value_bytes,
            if_absent: true,
            surrogate: target_surrogate,
            // A materializer copy answers no client, so it projects
            // nothing and needs no read gate.
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        });
        dispatch_to_owner(
            self.state,
            self.tenant_id,
            self.db_id,
            self.target_qualified,
            plan,
        )
        .await?;
        Ok(())
    }
}
