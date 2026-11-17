// SPDX-License-Identifier: Apache-2.0

//! `MetaOp::ReadRedoStreamPage`: one page of a committed chunked redo's
//! content, read back from the node's WAL.

/// Which records of a chunked redo's content a page carries.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum RedoStreamSection {
    /// The header record and every identity record. The section ends at the
    /// content's first resolved-list record.
    Identities,
    /// Every row-change record.
    RowChanges,
}

/// The answer of `MetaOp::ReadRedoStreamPage`.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct RedoStreamPage {
    /// Whole framed records of the asked section, in content order.
    pub frames: Vec<u8>,
    /// The content byte offset the next page starts at. Always a record
    /// boundary past the page's start.
    pub next: u64,
    /// Whether the section ends with this page.
    pub done: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_page_round_trips() {
        let page = RedoStreamPage {
            frames: vec![1, 2, 3],
            next: 4096,
            done: false,
        };
        let bytes = zerompk::to_msgpack_vec(&page).expect("encode");
        let back: RedoStreamPage = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(back, page);
        let section = RedoStreamSection::RowChanges;
        let bytes = zerompk::to_msgpack_vec(&section).expect("encode");
        let back: RedoStreamSection = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(back, section);
    }
}
