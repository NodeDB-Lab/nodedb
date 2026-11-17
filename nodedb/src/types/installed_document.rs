// SPDX-License-Identifier: BUSL-1.1

//! Exact document effects selected by an accepted source operation.

use crate::{bridge::envelope::RowVersion, types::hash_chain::ChainHead};
use nodedb_physical::physical_plan::StorageMode;
use std::sync::Arc;

#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct InstalledDocument<S> {
    pub canonical: Vec<u8>,
    pub stored: Vec<u8>,
    pub version: RowVersion,
    pub schema: S,
    pub chain_head: Option<ChainHead>,
}
pub type SharedInstalledDocument = InstalledDocument<Arc<StorageMode>>;
pub type CapturedInstalledDocument = InstalledDocument<u32>;
