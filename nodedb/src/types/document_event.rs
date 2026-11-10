// SPDX-License-Identifier: BUSL-1.1

//! Public document event images prepared before source commit.

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct DocumentPutEvent {
    pub new_value: Vec<u8>,
    pub old_value: Option<Vec<u8>>,
}
