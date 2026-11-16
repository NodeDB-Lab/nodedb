// SPDX-License-Identifier: BUSL-1.1

//! The resolve cursors of committing transactions on one core.
//!
//! A cursor reads the parts of one resolved redo in order. A resolve of one
//! part holds it in memory. A longer one is read from the transaction's
//! resolve spill file, one row per part. Either way the cursor holds one
//! part at a time. It closes with its last part, which removes any file,
//! when its transaction's overlay drops, and when the lease reaper finds it
//! unread past the lease.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::data::executor::handlers::join::FrameStreamReader;
use crate::data::executor::handlers::spill::cleanup::{SpillKind, remove_spill_file};
use crate::types::TxnId;

/// The file name of a transaction's resolve spill file, in its spill
/// directory.
pub(in crate::data::executor) const RESOLVE_FILE: &str = "resolve";

/// Where a resolve's parts are.
pub(in crate::data::executor) enum PartSource {
    /// The resolve's only part, `None` for a resolve of no part. No file
    /// exists.
    Memory(Option<Vec<u8>>),
    /// The resolve spill file at this path, one row per part.
    File(PathBuf),
}

/// Where an open cursor reads its parts.
enum CursorSource {
    Memory(Option<Vec<u8>>),
    File {
        reader: FrameStreamReader,
        path: PathBuf,
    },
}

impl CursorSource {
    /// The path of the cursor's file, if it reads one.
    fn into_path(self) -> Option<PathBuf> {
        match self {
            Self::Memory(_) => None,
            Self::File { reader, path } => {
                drop(reader);
                Some(path)
            }
        }
    }
}

/// The unread parts of one resolved redo.
struct ResolveCursor {
    source: CursorSource,
    /// Parts the source holds that no read returned yet.
    remaining: u64,
    /// The part the next request reads.
    next: u32,
    /// The core clock ordinal at the last read.
    touched: i64,
}

/// The open resolve cursors of one core, keyed by transaction.
#[derive(Default)]
pub(in crate::data::executor) struct ResolveCursors {
    open: HashMap<TxnId, ResolveCursor>,
}

impl ResolveCursors {
    /// Open the cursor of `txn_id` over the `parts` parts in `source`. The
    /// caller closed any open one.
    pub(in crate::data::executor) fn open(
        &mut self,
        txn_id: TxnId,
        source: PartSource,
        parts: u64,
        now: i64,
    ) -> crate::Result<()> {
        let source = match source {
            PartSource::Memory(part) => CursorSource::Memory(part),
            PartSource::File(path) => CursorSource::File {
                reader: FrameStreamReader::open(&path)?,
                path,
            },
        };
        self.open.insert(
            txn_id,
            ResolveCursor {
                source,
                remaining: parts,
                next: 0,
                touched: now,
            },
        );
        Ok(())
    }

    /// Read part `part` of `txn_id`'s cursor: its bytes, and whether it is
    /// the last part. Reading the last part closes the cursor and removes
    /// its file. A part other than the next one is refused.
    pub(in crate::data::executor) fn read(
        &mut self,
        txn_id: TxnId,
        part: u32,
        now: i64,
    ) -> crate::Result<(Vec<u8>, bool)> {
        let Some(cursor) = self.open.get_mut(&txn_id) else {
            return Err(crate::Error::Internal {
                detail: format!(
                    "{txn_id} asks for resolve part {part}, but this core holds no resolve \
                     cursor for it; the commit resolves again from part 0"
                ),
            });
        };
        if cursor.next != part {
            return Err(crate::Error::Internal {
                detail: format!(
                    "{txn_id} asks for resolve part {part}, but its cursor reads part {} next; \
                     the commit resolves again from part 0",
                    cursor.next
                ),
            });
        }
        let bytes = if cursor.remaining == 0 {
            Vec::new()
        } else {
            let next = match &mut cursor.source {
                CursorSource::Memory(slot) => slot.take(),
                CursorSource::File { reader, .. } => reader.next_row()?,
            };
            let Some(bytes) = next else {
                let held = match &cursor.source {
                    CursorSource::Memory(_) => "in-memory resolve part".to_string(),
                    CursorSource::File { path, .. } => {
                        format!("resolve spill file {}", path.display())
                    }
                };
                return Err(crate::Error::Internal {
                    detail: format!("the {held} of {txn_id} ends before its part {part}"),
                });
            };
            cursor.remaining -= 1;
            bytes
        };
        cursor.next += 1;
        cursor.touched = now;
        let done = cursor.remaining == 0;
        if done && let Some(cursor) = self.open.remove(&txn_id) {
            remove_resolve_file(cursor)?;
        }
        Ok((bytes, done))
    }

    /// Close the cursor of `txn_id`, if one is open. Returns the path of its
    /// file, which stays in place, when it reads one.
    pub(in crate::data::executor) fn close(&mut self, txn_id: TxnId) -> Option<PathBuf> {
        self.open
            .remove(&txn_id)
            .and_then(|cursor| cursor.source.into_path())
    }

    /// At most `limit` transactions whose cursor was last read before
    /// `threshold` and that `live` does not name.
    pub(in crate::data::executor) fn expired(
        &self,
        threshold: i64,
        limit: usize,
        live: impl Fn(&TxnId) -> bool,
    ) -> Vec<TxnId> {
        self.open
            .iter()
            .filter(|(txn_id, cursor)| !live(txn_id) && cursor.touched < threshold)
            .map(|(txn_id, _)| *txn_id)
            .take(limit)
            .collect()
    }

    /// Whether `txn_id` holds an open cursor.
    #[cfg(test)]
    pub(in crate::data::executor) fn is_open(&self, txn_id: TxnId) -> bool {
        self.open.contains_key(&txn_id)
    }
}

/// Remove the file, if any, of a cursor that read its last part.
fn remove_resolve_file(cursor: ResolveCursor) -> crate::Result<()> {
    match cursor.source.into_path() {
        Some(path) => remove_resolve_path(&path),
        None => Ok(()),
    }
}

/// Remove a resolve spill file. A file that cannot be removed files a
/// faultbox report at the removal site and is an error.
pub(in crate::data::executor) fn remove_resolve_path(path: &Path) -> crate::Result<()> {
    remove_spill_file(SpillKind::Txn, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::handlers::join::SpillPartitionWriter;

    /// A resolve file of `n` parts of four bytes each.
    fn file(dir: &Path, n: u8) -> PathBuf {
        let path = dir.join(RESOLVE_FILE);
        let mut writer = SpillPartitionWriter::create(&path).expect("create");
        for i in 0..n {
            writer.append_row(&[i; 4]).expect("part");
        }
        writer.finish().expect("finish")
    }

    #[test]
    fn parts_read_in_order_and_the_last_closes_the_cursor_and_its_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = file(dir.path(), 3);
        let mut cursors = ResolveCursors::default();
        let txn = TxnId::new(9);
        cursors
            .open(txn, PartSource::File(path.clone()), 3, 0)
            .expect("open");
        assert_eq!(
            cursors.read(txn, 0, 1).expect("part 0"),
            (vec![0; 4], false)
        );
        assert!(
            cursors.read(txn, 2, 1).is_err(),
            "a skipped part is refused"
        );
        assert_eq!(
            cursors.read(txn, 1, 1).expect("part 1"),
            (vec![1; 4], false)
        );
        assert_eq!(cursors.read(txn, 2, 1).expect("part 2"), (vec![2; 4], true));
        assert!(!cursors.is_open(txn));
        assert!(!path.exists(), "the last part removes the file");
        assert!(cursors.read(txn, 3, 1).is_err());
    }

    #[test]
    fn an_empty_resolve_answers_one_empty_last_part() {
        let mut cursors = ResolveCursors::default();
        let txn = TxnId::new(3);
        cursors
            .open(txn, PartSource::Memory(None), 0, 0)
            .expect("open");
        assert_eq!(cursors.read(txn, 0, 0).expect("part 0"), (Vec::new(), true));
        assert!(!cursors.is_open(txn));
    }

    /// A one-part resolve held in memory answers its part as the last one.
    #[test]
    fn a_part_in_memory_answers_as_the_last_part() {
        let mut cursors = ResolveCursors::default();
        let txn = TxnId::new(4);
        cursors
            .open(txn, PartSource::Memory(Some(vec![7; 4])), 1, 0)
            .expect("open");
        assert_eq!(cursors.read(txn, 0, 0).expect("part 0"), (vec![7; 4], true));
        assert!(!cursors.is_open(txn));
        assert_eq!(cursors.close(txn), None);
    }

    #[test]
    fn only_unread_cursors_of_dead_transactions_expire() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cursors = ResolveCursors::default();
        let (live, stale, fresh) = (TxnId::new(1), TxnId::new(2), TxnId::new(3));
        for (txn, touched) in [(live, 0), (stale, 0), (fresh, 50)] {
            let sub = dir.path().join(txn.as_u64().to_string());
            std::fs::create_dir_all(&sub).expect("dir");
            cursors
                .open(txn, PartSource::File(file(&sub, 2)), 2, touched)
                .expect("open");
        }
        assert_eq!(cursors.expired(10, 8, |txn| *txn == live), vec![stale]);
        assert!(
            cursors.expired(10, 0, |_| false).is_empty(),
            "the limit bounds the list"
        );
    }
}
