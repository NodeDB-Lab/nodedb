// SPDX-License-Identifier: BUSL-1.1

//! Events as a consumer reads them, for tests that compare two event
//! streams.

use crate::event::WriteEvent;

/// An event image as the Event Plane reads it: the decoded document when
/// it decodes as one, else its bytes. A document image encodes its keys in
/// hash order, so two installs of one row write the same map in different
/// byte orders.
pub(in crate::data::executor) type ReadImage =
    Result<serde_json::Map<String, serde_json::Value>, Vec<u8>>;

pub(in crate::data::executor) fn read_image(image: Option<&[u8]>) -> Option<ReadImage> {
    image.map(|bytes| crate::event::deserialize_event_payload(bytes).ok_or_else(|| bytes.to_vec()))
}

/// `event` as a consumer reads it: every field, with each image decoded.
pub(in crate::data::executor) fn as_read(
    event: &WriteEvent,
) -> (String, Option<ReadImage>, Option<ReadImage>) {
    let mut fields = event.clone();
    fields.new_value = None;
    fields.old_value = None;
    (
        format!("{fields:?}"),
        read_image(event.new_value.as_deref()),
        read_image(event.old_value.as_deref()),
    )
}

pub(in crate::data::executor) fn as_read_all(
    events: &[WriteEvent],
) -> Vec<(String, Option<ReadImage>, Option<ReadImage>)> {
    events.iter().map(as_read).collect()
}
