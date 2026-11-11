// SPDX-License-Identifier: BUSL-1.1

//! The row image limit every stored document row and edge passes before it
//! installs.
//!
//! A write-set part journals each installed row in one WAL record, so every
//! image of a row is held to [`MAX_ROW_IMAGE_BYTES`]. The bound that makes
//! one row fit one record is asserted beside the part planner.

use nodedb_types::{MAX_OLD_EVENT_IMAGE_BYTES, MAX_ROW_IMAGE_BYTES};

use crate::bridge::envelope::ErrorCode;
use crate::types::document_event::DocumentPutEvent;

/// How many characters of a row id a refusal names.
const ROW_ID_SHOWN: usize = 64;

/// The refusal of a row image of `bytes` bytes, over the limit.
pub(crate) fn row_image_too_large(collection: &str, row_id: &str, bytes: usize) -> ErrorCode {
    ErrorCode::ProgramLimitExceeded {
        detail: format!(
            "row '{}' of '{collection}' is {bytes} bytes, over the row limit of \
             {MAX_ROW_IMAGE_BYTES} bytes; store the large value in smaller rows or as a blob",
            shown_id(row_id)
        ),
    }
}

/// The refusal of a row id of `bytes` bytes, over the limit.
fn row_id_too_large(collection: &str, row_id: &str, bytes: usize) -> ErrorCode {
    ErrorCode::ProgramLimitExceeded {
        detail: format!(
            "the id of row '{}' of '{collection}' is {bytes} bytes, over the row limit of \
             {MAX_ROW_IMAGE_BYTES} bytes; use a shorter key",
            shown_id(row_id)
        ),
    }
}

/// Refuse a row image of `bytes` bytes over [`MAX_ROW_IMAGE_BYTES`].
pub(crate) fn check_row_image(collection: &str, row_id: &str, bytes: usize) -> crate::Result<()> {
    if bytes > MAX_ROW_IMAGE_BYTES {
        return Err(crate::Error::DataPlane(row_image_too_large(
            collection, row_id, bytes,
        )));
    }
    Ok(())
}

/// Refuse a row whose id or any image in `images` is over
/// [`MAX_ROW_IMAGE_BYTES`].
pub(crate) fn check_row_images(
    collection: &str,
    row_id: &str,
    images: &[&[u8]],
) -> crate::Result<()> {
    if row_id.len() > MAX_ROW_IMAGE_BYTES {
        return Err(crate::Error::DataPlane(row_id_too_large(
            collection,
            row_id,
            row_id.len(),
        )));
    }
    for image in images {
        check_row_image(collection, row_id, image.len())?;
    }
    Ok(())
}

/// Refuse an installed document row whose id, accepted body, stored body or
/// new public event image is over [`MAX_ROW_IMAGE_BYTES`], or whose prior
/// event image is over [`MAX_OLD_EVENT_IMAGE_BYTES`].
///
/// The prior image of a row stored before `ALTER ADD COLUMN` renders the
/// added columns too. The catalog bounds what they add, so a row stored
/// within the limit always updates.
pub(crate) fn check_installed_row(
    collection: &str,
    row_id: &str,
    canonical: &[u8],
    stored: &[u8],
    event: &DocumentPutEvent,
) -> crate::Result<()> {
    check_row_images(
        collection,
        row_id,
        &[canonical, stored, event.new_value.as_slice()],
    )?;
    if let Some(old_value) = &event.old_value
        && old_value.len() > MAX_OLD_EVENT_IMAGE_BYTES
    {
        return Err(crate::Error::DataPlane(ErrorCode::ProgramLimitExceeded {
            detail: format!(
                "the prior image of row '{}' of '{collection}' is {} bytes, over the limit of \
                 {MAX_OLD_EVENT_IMAGE_BYTES} bytes; store the large value in smaller rows or \
                 as a blob",
                shown_id(row_id),
                old_value.len()
            ),
        }));
    }
    Ok(())
}

/// Refuse an edge whose properties, endpoints or label are over
/// [`MAX_ROW_IMAGE_BYTES`]. The refusal names the first part over the limit.
pub(crate) fn check_edge(
    collection: &str,
    src_id: &str,
    label: &str,
    dst_id: &str,
    properties: &[u8],
) -> crate::Result<()> {
    let parts = [
        ("src", src_id.len()),
        ("label", label.len()),
        ("dst", dst_id.len()),
        ("properties", properties.len()),
    ];
    let Some((part, bytes)) = parts
        .into_iter()
        .find(|(_, bytes)| *bytes > MAX_ROW_IMAGE_BYTES)
    else {
        return Ok(());
    };
    Err(crate::Error::DataPlane(ErrorCode::ProgramLimitExceeded {
        detail: format!(
            "the {part} of edge ({})-[{}]->({}) of '{collection}' is {bytes} bytes, over the \
             row limit of {MAX_ROW_IMAGE_BYTES} bytes; store the large value in smaller rows or \
             as a blob",
            shown_id(src_id),
            shown_id(label),
            shown_id(dst_id)
        ),
    }))
}

/// A row id or name as a refusal shows it: at most [`ROW_ID_SHOWN`]
/// characters.
pub(crate) fn shown_id(row_id: &str) -> String {
    match row_id.char_indices().nth(ROW_ID_SHOWN) {
        Some((end, _)) => format!("{}...", &row_id[..end]),
        None => row_id.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn detail(error: crate::Error) -> String {
        match error {
            crate::Error::DataPlane(ErrorCode::ProgramLimitExceeded { detail }) => detail,
            other => panic!("expected a program limit refusal, got {other:?}"),
        }
    }

    #[test]
    fn an_image_at_the_limit_passes() {
        let image = vec![0u8; MAX_ROW_IMAGE_BYTES];
        check_row_images("orders", "o1", &[image.as_slice()]).expect("an image at the limit");
    }

    #[test]
    fn an_image_over_the_limit_names_the_row_and_the_next_action() {
        let image = vec![0u8; MAX_ROW_IMAGE_BYTES + 1];
        let detail =
            detail(check_row_images("orders", "o1", &[image.as_slice()]).expect_err("refused"));
        assert!(detail.contains("'o1'"), "{detail}");
        assert!(detail.contains("'orders'"), "{detail}");
        assert!(
            detail.contains(&(MAX_ROW_IMAGE_BYTES + 1).to_string()),
            "{detail}"
        );
        assert!(
            detail.contains(&MAX_ROW_IMAGE_BYTES.to_string()),
            "{detail}"
        );
        assert!(detail.contains("smaller rows or as a blob"), "{detail}");
    }

    #[test]
    fn an_old_event_image_over_the_limit_refuses() {
        let event = DocumentPutEvent {
            new_value: vec![1],
            old_value: Some(vec![0u8; MAX_OLD_EVENT_IMAGE_BYTES + 1]),
        };
        check_installed_row("orders", "o1", &[1], &[1], &event).expect_err("refused");
    }

    /// A prior image that columns added since its row was stored grow past
    /// the row limit still updates.
    #[test]
    fn a_prior_image_grown_by_added_columns_updates() {
        let event = DocumentPutEvent {
            new_value: vec![1],
            old_value: Some(vec![0u8; MAX_ROW_IMAGE_BYTES + 1]),
        };
        check_installed_row("orders", "o1", &[1], &[1], &event).expect("within the prior limit");
    }

    #[test]
    fn an_edge_refusal_names_the_part_over_the_limit() {
        let over = "x".repeat(MAX_ROW_IMAGE_BYTES + 1);
        let properties = vec![0u8; MAX_ROW_IMAGE_BYTES + 1];
        for (part, refusal) in [
            ("src", check_edge("people", &over, "knows", "b", &[])),
            ("label", check_edge("people", "a", &over, "b", &[])),
            ("dst", check_edge("people", "a", "knows", &over, &[])),
            (
                "properties",
                check_edge("people", "a", "knows", "b", &properties),
            ),
        ] {
            let detail = detail(refusal.expect_err("refused"));
            assert!(
                detail.starts_with(&format!("the {part} of edge")),
                "{detail}"
            );
            assert!(
                detail.contains(&(MAX_ROW_IMAGE_BYTES + 1).to_string()),
                "{detail}"
            );
        }
        check_edge("people", "a", "knows", "b", &[0u8; 16]).expect("a small edge");
    }

    #[test]
    fn a_long_row_id_is_cut_in_the_refusal() {
        let id = "k".repeat(MAX_ROW_IMAGE_BYTES + 1);
        let detail = detail(check_row_images("orders", &id, &[]).expect_err("refused"));
        assert!(detail.len() < 512, "{detail}");
        assert!(detail.contains("shorter key"), "{detail}");
    }
}
