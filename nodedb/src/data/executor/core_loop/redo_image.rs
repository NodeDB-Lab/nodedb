// SPDX-License-Identifier: BUSL-1.1

//! Row identities and control markers for document write-set entries.

use crate::bridge::envelope::WriteSetEntry;
use crate::engine::document::store::RowIdentity;

/// The row a write-set entry names.
pub(in crate::data::executor) struct WriteSetRow<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
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

/// The entries a point write owes beyond its pre-dispatch record.
///
/// The pre-dispatch record carries the row unstamped. The apply decides the
/// version key, so the write journals its exact installed `image` and cancels
/// the unstamped record.
pub(in crate::data::executor) fn versioned_point_images(
    image: WriteSetEntry,
) -> Vec<WriteSetEntry> {
    let cancel = WriteSetEntry::cancel_forward(image.surrogate, image.identity.clone());
    vec![image, cancel]
}
