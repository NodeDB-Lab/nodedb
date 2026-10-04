// SPDX-License-Identifier: BUSL-1.1

//! The write-set entries a document write reports for the rows it stored.
//!
//! A write-set entry carries a row's post-image in the form the write path
//! encodes into storage: canonical MessagePack. Replay hands the body to that
//! same encoder, so a strict collection's Binary Tuple is decoded back to
//! MessagePack here, before it is journalled. A row on a `bitemporal=true`
//! collection carries the version key it landed at.

use crate::bridge::envelope::{RowVersion, WriteSetEntry};
use crate::data::executor::strict_format::undecodable_strict_row;
use crate::engine::document::store::RowIdentity;

use super::CoreLoop;
use super::event_image::StoredImage;
use super::fail_stop::FailStopCause;

impl CoreLoop {
    /// The write-set entry of a row a write stored as `stored`.
    /// `sys_from_ms` is the version's system time on a `bitemporal=true`
    /// collection, whose version is valid for all time.
    ///
    /// The redo body is MessagePack: a strict collection's Binary Tuple is
    /// decoded, other bytes are journalled as stored. A strict row that does
    /// not decode is an error: replay would encode raw tuple bytes as a
    /// MessagePack body and store a different row. The write already landed
    /// and its write set cannot be journalled, so the core fail-stops.
    pub(in crate::data::executor) fn stored_row_image(
        &mut self,
        row: StoredRow<'_>,
        stored: &[u8],
        sys_from_ms: Option<i64>,
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
        Ok(WriteSetEntry::put(row.surrogate, row.identity, body)
            .versioned(sys_from_ms.map(RowVersion::open)))
    }
}

/// The row a write-set entry names.
pub(in crate::data::executor) struct StoredRow<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub surrogate: u32,
    pub identity: RowIdentity,
}

/// The write-set entry of a row removed by a write, with its tombstone's
/// system time on a `bitemporal=true` collection.
pub(in crate::data::executor) fn removed_row_image(
    surrogate: u32,
    identity: RowIdentity,
    sys_from_ms: Option<i64>,
) -> WriteSetEntry {
    WriteSetEntry::delete(surrogate, identity).versioned(sys_from_ms.map(RowVersion::open))
}

/// The write-set entry of a row a write stored from `value`, the MessagePack
/// body it handed to the write path.
pub(in crate::data::executor) fn submitted_row_image(
    surrogate: u32,
    identity: RowIdentity,
    value: Vec<u8>,
    sys_from_ms: Option<i64>,
) -> WriteSetEntry {
    WriteSetEntry::put(surrogate, identity, value).versioned(sys_from_ms.map(RowVersion::open))
}

/// The entries a point write on a `bitemporal=true` collection owes beyond
/// its pre-dispatch record.
///
/// The pre-dispatch record carries an unversioned row exactly. On a versioned
/// collection the apply decides the version key, so the write journals its
/// image stamped at `sys_from_ms` and cancels the unstamped record.
pub(in crate::data::executor) fn versioned_point_images(
    image: WriteSetEntry,
    sys_from_ms: i64,
) -> Vec<WriteSetEntry> {
    let cancel = WriteSetEntry::cancel_forward(image.surrogate, image.identity.clone());
    vec![image.versioned(Some(RowVersion::open(sys_from_ms))), cancel]
}
