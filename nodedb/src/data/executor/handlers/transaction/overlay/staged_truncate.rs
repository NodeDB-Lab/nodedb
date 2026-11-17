// SPDX-License-Identifier: BUSL-1.1

//! Staging a TRUNCATE in the overlay.
//!
//! A truncate tombstones every row the transaction staged a put for, then
//! sets a marker that hides every base row with no newer overlay entry. The
//! staged rows are read in batches of [`TRUNCATE_BATCH`] bindings, each scan
//! resuming after the last binding the batch read, so a truncate after a
//! spilled insert holds one batch in RAM.

use nodedb_types::RowIdentity;

use super::staged::{CollKey, Staged, TxnOverlay};
use super::staged_journal::JournalEntry;
use crate::data::executor::handlers::spill::tiered::ScanBounds;
use crate::data::executor::handlers::spill::tiered::map::encode_key;

/// Bindings one truncate batch reads.
const TRUNCATE_BATCH: usize = 4_096;

/// One batch of a truncate's scan of the staged bindings.
struct PutBatch {
    /// Each row in the batch that holds a staged put, with its surrogate.
    puts: Vec<(RowIdentity, u32)>,
    /// The encoded key of the last binding read, or `None` when the scan
    /// reached the end.
    resume: Option<Vec<u8>>,
}

impl TxnOverlay {
    /// Stage a TRUNCATE of `coll_key`. Every row already staged in the
    /// collection is tombstoned through the normal undo path, so a savepoint
    /// rollback restores it; the marker then hides every base row that has
    /// no newer overlay entry.
    pub fn mark_truncated(&mut self, coll_key: CollKey) -> crate::Result<()> {
        self.guard()?;
        let mut after: Option<Vec<u8>> = None;
        loop {
            let batch = self.staged_put_batch(&coll_key, after.take())?;
            for (doc_id, surrogate) in &batch.puts {
                self.insert_tombstone(coll_key.clone(), *surrogate, doc_id)?;
            }
            match batch.resume {
                Some(last) => after = Some(last),
                None => break,
            }
        }
        let prev = self.truncated.get(&coll_key).copied();
        let position = self.journal.push(JournalEntry::Truncated {
            coll_key: coll_key.clone(),
            prev,
        });
        self.truncated.insert(coll_key, position);
        Ok(())
    }

    /// The next [`TRUNCATE_BATCH`] bindings after `after`.
    fn staged_put_batch(
        &self,
        coll_key: &CollKey,
        after: Option<Vec<u8>>,
    ) -> crate::Result<PutBatch> {
        let Some(overlay) = self.collections.get(coll_key) else {
            return Ok(PutBatch {
                puts: Vec::new(),
                resume: None,
            });
        };
        let reader = self.space.reader();
        let mut batch = Vec::new();
        let mut read = 0usize;
        let mut last: Option<RowIdentity> = None;
        for binding in overlay.doc_id_to_surrogate.scan(
            ScanBounds {
                after,
                prefix: None,
            },
            reader,
        ) {
            let (doc_id, surrogate) = binding?;
            let surrogate = *surrogate;
            if matches!(
                overlay.by_surrogate.get(&surrogate, reader)?.as_deref(),
                Some(Staged::Put(_))
            ) {
                batch.push((doc_id.clone(), surrogate));
            }
            last = Some(doc_id);
            read += 1;
            if read == TRUNCATE_BATCH {
                break;
            }
        }
        let resume = match (read == TRUNCATE_BATCH, last) {
            (true, Some(last)) => Some(encode_key(&last)?),
            _ => None,
        };
        Ok(PutBatch {
            puts: batch,
            resume,
        })
    }
}
