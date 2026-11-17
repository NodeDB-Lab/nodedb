// SPDX-License-Identifier: BUSL-1.1

//! Derive one document's text again from what a database view holds.
//!
//! The cutover runs it inside its write transaction for each document
//! written during the last pass. A catch-up round runs it off the core
//! against its fresh snapshot for each document written during the pass
//! before. Both derive a document the same way: every stored version at
//! its own system time, or every stored sync frame when the view holds no
//! version. A document with neither holds no text. Its whole text history
//! then matches a full pass over the view.

use redb::{ReadTransaction, WriteTransaction};

use nodedb_types::{StorageKey, Surrogate};

use super::replay::{ReplayTarget, TextVersion, replay_frame_into, replay_version_into};
use crate::engine::sparse::btree_versioned::version_walk::{self, VersionVisit};
use crate::engine::sparse::inverted::{
    IndexDocScope, SyncedDocText, for_each_doc_frame_in_snapshot, for_each_doc_frame_in_txn,
};
use crate::types::TenantId;

/// A view a re-derive reads one document's stored versions and frames from.
pub(in crate::data::executor) trait TextSource {
    /// Visit every stored version of `doc`, in ascending system time.
    fn row_versions(&self, doc: DocRef<'_>, visit: &mut VersionVisit<'_>) -> crate::Result<()>;

    /// Visit every stored frame version of `doc`, in ordinal order.
    fn doc_frames(
        &self,
        doc: DocRef<'_>,
        visit: &mut dyn FnMut(SyncedDocText) -> crate::Result<()>,
    ) -> crate::Result<()>;
}

/// The document one re-derive covers.
#[derive(Clone, Copy)]
pub(in crate::data::executor) struct DocRef<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub surrogate: Surrogate,
}

impl TextSource for WriteTransaction {
    fn row_versions(&self, doc: DocRef<'_>, visit: &mut VersionVisit<'_>) -> crate::Result<()> {
        version_walk::for_each_row_version_in_txn(
            self,
            doc.database_id,
            doc.tid,
            doc.collection,
            &StorageKey::for_surrogate(doc.surrogate),
            visit,
        )
    }

    fn doc_frames(
        &self,
        doc: DocRef<'_>,
        visit: &mut dyn FnMut(SyncedDocText) -> crate::Result<()>,
    ) -> crate::Result<()> {
        for_each_doc_frame_in_txn(
            self,
            (doc.database_id, doc.tid),
            doc.collection,
            doc.surrogate,
            visit,
        )
    }
}

impl TextSource for ReadTransaction {
    fn row_versions(&self, doc: DocRef<'_>, visit: &mut VersionVisit<'_>) -> crate::Result<()> {
        let documents = version_walk::open_documents(self)?;
        version_walk::for_each_row_version(
            &documents,
            doc.database_id,
            doc.tid,
            doc.collection,
            &StorageKey::for_surrogate(doc.surrogate),
            visit,
        )
    }

    fn doc_frames(
        &self,
        doc: DocRef<'_>,
        visit: &mut dyn FnMut(SyncedDocText) -> crate::Result<()>,
    ) -> crate::Result<()> {
        for_each_doc_frame_in_snapshot(
            self,
            (doc.database_id, doc.tid),
            doc.collection,
            doc.surrogate,
            visit,
        )
    }
}

/// Derive `doc` into `target`'s index inside `txn` from what `source`
/// holds. `target` holds no row of `doc`: the caller left them out.
/// Returns the versions and frames replayed.
pub(in crate::data::executor) fn rederive_doc(
    source: &impl TextSource,
    target: ReplayTarget<'_>,
    txn: &WriteTransaction,
    doc: DocRef<'_>,
) -> crate::Result<u64> {
    let DocRef {
        database_id,
        tid,
        collection,
        surrogate,
    } = doc;
    let mut versions = 0u64;
    source.row_versions(doc, &mut |_, sys_from, body| {
        versions += 1;
        replay_version_into(
            target,
            txn,
            TextVersion {
                database_id,
                tid,
                collection,
                surrogate,
                ordinal: sys_from,
                body,
            },
        )
    })?;
    if versions > 0 {
        return Ok(versions);
    }
    let mut frames = 0u64;
    source.doc_frames(doc, &mut |synced| {
        frames += 1;
        replay_frame_into(
            target,
            txn,
            IndexDocScope {
                database_id,
                tid: TenantId::new(tid),
                collection,
                surrogate,
                ordinal: synced.ordinal,
            },
            &synced.fields,
        )
    })?;
    Ok(frames)
}
