// SPDX-License-Identifier: BUSL-1.1

//! The HASH_CHAIN state a core holds: every chain head, and the link each
//! in-flight write asks for.

use std::collections::HashMap;

use nodedb_types::DatabaseId;

use crate::data::executor::enforcement::chain_guard::ChainIntent;
use crate::types::TenantId;
use crate::types::hash_chain::ChainHead;

/// `(database, tenant, collection)`.
type CollectionKey = (DatabaseId, TenantId, String);

/// Chain heads and in-flight chain links of every HASH_CHAIN collection.
pub(in crate::data::executor) struct HashChainState {
    /// Per-collection chain head: the last link's install-order position and
    /// hash.
    pub(in crate::data::executor) heads: HashMap<CollectionKey, ChainHead>,
    /// The chain link each in-flight write of a HASH_CHAIN row asks
    /// `build_stored_body` to write, keyed by collection and row surrogate.
    /// `ChainGuard` sets an entry before the write and clears it after.
    pub(in crate::data::executor) intents: HashMap<(CollectionKey, u32), ChainIntent>,
}

impl HashChainState {
    /// State holding `heads` and no in-flight link.
    pub(in crate::data::executor) fn new(heads: HashMap<CollectionKey, ChainHead>) -> Self {
        Self {
            heads,
            intents: HashMap::new(),
        }
    }
}
