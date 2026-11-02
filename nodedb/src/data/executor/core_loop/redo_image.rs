// SPDX-License-Identifier: BUSL-1.1

//! The write-set entries a document write reports for the rows it stored.
//!
//! A write-set entry carries a row's post-image in the form the write path
//! encodes into storage: canonical MessagePack. Replay hands the body to that
//! same encoder, so a strict collection's Binary Tuple is decoded back to
//! MessagePack here, before it is journalled. Every entry carries the
//! version key its row landed at.

use crate::bridge::envelope::{RowVersion, WriteSetEntry};
use crate::data::executor::strict_format::undecodable_strict_row;
use crate::engine::document::store::RowIdentity;

use super::CoreLoop;
use super::event_image::StoredImage;
use super::fail_stop::FailStopCause;

impl CoreLoop {
    /// The write-set entry of a row a write stored as `stored`.
    /// `sys_from` is the version's system-time ordinal. The version is valid
    /// for all time.
    ///
    /// The redo body is MessagePack: a strict collection's Binary Tuple is
    /// decoded, other bytes are journalled as stored. A strict row that does
    /// not decode is an error: replay would encode raw tuple bytes as a
    /// MessagePack body and store a different row. The write already landed
    /// and its write set cannot be journalled, so the core fail-stops.
    pub(in crate::data::executor) fn stored_row_image(
        &mut self,
        row: WriteSetRow<'_>,
        stored: &[u8],
        sys_from: i64,
    ) -> crate::Result<WriteSetEntry> {
        let body =
            match self.resolve_event_payload(row.database_id, row.tid, row.collection, stored) {
                StoredImage::Converted(converted) => converted,
                StoredImage::AsStored => stored.to_vec(),
                StoredImage::Undecodable => {
                    let error = undecodable_strict_row(row.collection, row.identity.as_str());
                    self.fail_stop_core(FailStopCause::WriteSetUnpersisted, &error.to_string());
                    return Err(error);
                }
            };
        Ok(WriteSetEntry::put(
            row.surrogate,
            row.identity,
            body,
            RowVersion::open(sys_from),
        ))
    }
}

/// The row a write-set entry names.
pub(in crate::data::executor) struct WriteSetRow<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub surrogate: u32,
    pub identity: RowIdentity,
}

/// The write-set entry of a row removed by a write, with its tombstone's
/// system time.
pub(in crate::data::executor) fn removed_row_image(
    surrogate: u32,
    identity: RowIdentity,
    sys_from: i64,
) -> WriteSetEntry {
    WriteSetEntry::delete(surrogate, identity, sys_from)
}

/// The write-set entry of a row a write stored from `value`, the MessagePack
/// body it handed to the write path.
pub(in crate::data::executor) fn submitted_row_image(
    surrogate: u32,
    identity: RowIdentity,
    value: Vec<u8>,
    sys_from: i64,
) -> WriteSetEntry {
    WriteSetEntry::put(surrogate, identity, value, RowVersion::open(sys_from))
}

/// The entries a point write owes beyond its pre-dispatch record.
///
/// The pre-dispatch record carries the row unstamped. The apply decides the
/// version key, so the write journals its stamped `image` and cancels the
/// unstamped record.
pub(in crate::data::executor) fn versioned_point_images(
    image: WriteSetEntry,
) -> Vec<WriteSetEntry> {
    let cancel = WriteSetEntry::cancel_forward(image.surrogate, image.identity.clone());
    vec![image, cancel]
}
