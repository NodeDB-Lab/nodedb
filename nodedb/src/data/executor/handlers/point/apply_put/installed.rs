// SPDX-License-Identifier: BUSL-1.1

//! Exact accepted document installation and postcommit head publication.

use super::{
    installed_schema::{check_image, compatible, invalid},
    materialized::MaterializedDocumentPut,
    types::{InstalledDocumentRow, InstalledHeadReceipt, PointPutOutcome},
};
use crate::{
    data::executor::{core_loop::CoreLoop, enforcement::unique::UniqueJudge},
    types::{DatabaseId, SharedInstalledDocument, TenantId},
};
use redb::WriteTransaction;

impl CoreLoop {
    /// Install exact source bytes and indexes without deriving another document.
    pub(in crate::data::executor) fn apply_installed_document(
        &mut self,
        txn: &WriteTransaction,
        row: InstalledDocumentRow<'_>,
        image: &SharedInstalledDocument,
    ) -> crate::Result<(PointPutOutcome, Option<InstalledHeadReceipt>)> {
        self.check_installed_row(&row, image)?;
        let receipt = self.persist_installed_head(txn, &row, image)?;
        let outcome = self.apply_checked_image(txn, row, image.clone())?;
        Ok((outcome, receipt))
    }

    /// Apply an already-admitted UPDATE image inside its statement transaction.
    pub(in crate::data::executor) fn apply_prepared_update(
        &mut self,
        txn: &WriteTransaction,
        row: InstalledDocumentRow<'_>,
        image: SharedInstalledDocument,
    ) -> crate::Result<PointPutOutcome> {
        self.check_installed_row(&row, &image)?;
        self.apply_checked_image(txn, row, image)
    }

    fn check_installed_row(
        &self,
        row: &InstalledDocumentRow<'_>,
        image: &SharedInstalledDocument,
    ) -> crate::Result<()> {
        if row.storage_key.surrogate() != row.surrogate {
            return Err(invalid("storage key contradicts its captured surrogate"));
        }
        check_image(image)?;
        let key = (
            DatabaseId::new(row.database_id),
            TenantId::new(row.tid),
            row.collection.to_string(),
        );
        if let Some(config) = self.doc_configs.get(&key) {
            compatible(&image.schema, &config.storage_mode, &image.stored)?;
        }
        Ok(())
    }

    fn apply_checked_image(
        &mut self,
        txn: &WriteTransaction,
        row: InstalledDocumentRow<'_>,
        image: SharedInstalledDocument,
    ) -> crate::Result<PointPutOutcome> {
        let database_id = row.database_id;
        let tid = row.tid;
        let collection = row.collection;
        let storage_key = row.storage_key;
        let outcome = self.apply_materialized_document(
            txn,
            MaterializedDocumentPut {
                row,
                installed: image,
                index_text: true,
                user_roles: &[],
                enforce: false,
                unique: UniqueJudge::Unit,
                resolved_targets: &[],
                check_history: true,
            },
        );
        outcome.inspect_err(|_| {
            self.doc_cache
                .invalidate(database_id, tid, collection, &storage_key);
        })
    }

    fn persist_installed_head(
        &self,
        txn: &WriteTransaction,
        row: &InstalledDocumentRow<'_>,
        image: &SharedInstalledDocument,
    ) -> crate::Result<Option<InstalledHeadReceipt>> {
        let Some(carried) = &image.chain_head else {
            return Ok(None);
        };
        let key = (
            DatabaseId::new(row.database_id),
            TenantId::new(row.tid),
            row.collection.to_string(),
        );
        let committed = self
            .sparse
            .get_chain_head(row.database_id, row.tid, row.collection)?;
        if self.chain.heads.get(&key) != committed.as_ref() {
            return Err(invalid("cached chain head contradicts committed storage"));
        }
        let prior =
            self.sparse
                .get_chain_head_in_txn(txn, row.database_id, row.tid, row.collection)?;
        let installed = match &prior {
            Some(head) if head.seq > carried.seq => head.clone(),
            Some(head) if head.seq == carried.seq => {
                if head.hash != carried.hash {
                    return Err(invalid(
                        "chain heads have equal positions and different hashes",
                    ));
                }
                head.clone()
            }
            _ => carried.clone(),
        };
        let changed = prior.as_ref() != Some(&installed);
        if changed {
            self.sparse.put_chain_head_in_txn(
                txn,
                row.database_id,
                row.tid,
                row.collection,
                &installed,
            )?;
        }
        Ok(Some(InstalledHeadReceipt {
            collection: row.collection.into(),
            prior,
            installed,
            changed,
        }))
    }

    /// Publish a checked head receipt after the caller commits its transaction.
    pub(in crate::data::executor) fn publish_installed_head(
        &mut self,
        database_id: u64,
        tid: u64,
        receipt: &InstalledHeadReceipt,
    ) {
        let key = (
            DatabaseId::new(database_id),
            TenantId::new(tid),
            receipt.collection.clone(),
        );
        if self
            .chain
            .heads
            .get(&key)
            .is_none_or(|head| head.seq < receipt.installed.seq)
        {
            self.chain.heads.insert(key, receipt.installed.clone());
        }
    }
}
