// SPDX-License-Identifier: BUSL-1.1

//! Row copy of one non-chained Document clone collection.
//!
//! Each source page is filtered, bound to target surrogates in one batch, and
//! installed as copies at the source versions' own stamps. A progress
//! checkpoint follows each page.
//!
//! A re-run after a crash copies the same versions at the same stamps, so a
//! row already copied is written again in place.

use std::collections::HashSet;

use nodedb_types::{DatabaseId, Lsn, Surrogate, TenantId};

use super::document::{SourcePage, SourceRow, scan_page};
use super::status::{check_bound_surrogates, checkpoint_progress};
use crate::control::clone::copy::{CopyTarget, DocCopy, commit_doc_copies};
use crate::control::security::catalog::{StoredCollection, SystemCatalog};
use crate::control::state::SharedState;
use crate::types::DocVersionStamp;

/// One source row to copy, with the pk bytes it keys under.
struct PendingRow {
    doc_id_hex: String,
    pk_bytes: Vec<u8>,
    value_bytes: Vec<u8>,
    stamp: DocVersionStamp,
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
            // declared-key target a second identity. `copy_of` writes the
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
    /// tombstone) or already copied up by the CoW write path is skipped. The
    /// catalog is read per row, so a tombstone recorded while the copy runs
    /// counts.
    fn pending_rows(&self, entries: Vec<SourceRow>) -> crate::Result<Vec<PendingRow>> {
        let mut pending = Vec::with_capacity(entries.len());
        for row in entries {
            let SourceRow {
                doc_id_hex,
                source_surrogate,
                body,
                stamp,
            } = row;
            if self.tombstoned.contains(&source_surrogate)
                || self
                    .catalog
                    .is_clone_tombstoned(self.target_qualified, source_surrogate)?
                || self
                    .catalog
                    .get_clone_copyup(self.target_qualified, source_surrogate)?
                    .is_some()
            {
                continue;
            }
            // An autocommit scan reads committed versions only, each stamped.
            let stamp = stamp.ok_or_else(|| crate::Error::Storage {
                engine: "clone_materializer".into(),
                detail: format!(
                    "source row {doc_id_hex} of '{}' carries no version stamp",
                    self.source_qualified
                ),
            })?;
            let pk_bytes = self.source_pk_bytes(&doc_id_hex, source_surrogate)?;
            pending.push(PendingRow {
                doc_id_hex,
                pk_bytes,
                value_bytes: body,
                stamp,
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

    /// Bind target surrogates for `pending` and install each row into the
    /// target at its source stamps. Returns the rows copied.
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
        let copies: Vec<DocCopy> = pending
            .into_iter()
            .zip(target_surrogates)
            .map(|(row, target_surrogate)| self.copy_of(row, target_surrogate))
            .collect();
        let copied = copies.len() as u64;
        commit_doc_copies(
            self.state,
            CopyTarget {
                tenant_id: self.tenant_id,
                database_id: self.db_id,
                collection: &self.coll.name,
            },
            copies,
        )
        .await?;
        Ok(copied)
    }

    /// The copy of one row under `target_surrogate`.
    fn copy_of(&self, row: PendingRow, target_surrogate: Surrogate) -> DocCopy {
        let PendingRow {
            doc_id_hex,
            pk_bytes,
            value_bytes,
            stamp,
        } = row;
        // The copy keeps the row's client identity under its new surrogate,
        // so a clone read matches it to its source row by primary key.
        let body = match nodedb_types::StorageKey::parse(&doc_id_hex) {
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
        DocCopy {
            document_id: String::from_utf8_lossy(&pk_bytes).into_owned(),
            pk_bytes,
            target_surrogate,
            body,
            stamp,
        }
    }
}
