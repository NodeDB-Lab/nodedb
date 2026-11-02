// SPDX-License-Identifier: BUSL-1.1

//! Reads of the newest ordinal of a collection slice or a vShard, through
//! the cache.

use crate::types::{DatabaseId, TenantId, VShardId};

use super::super::CoreLoop;
use super::cache::SliceKey;

impl CoreLoop {
    /// The newest ordinal the slice of `collection` on `vshard` holds: the
    /// cached value, else the stored one, which the cache then keeps. `Err`
    /// when a store does not read.
    pub(in crate::data::executor) fn slice_newest(
        &mut self,
        vshard: VShardId,
        db: DatabaseId,
        tenant: TenantId,
        collection: &str,
    ) -> crate::Result<Option<i64>> {
        let key = SliceKey {
            vshard,
            db,
            tenant,
            collection: Box::from(collection),
        };
        if let Some(newest) = self.newest_ordinals.slice(&key) {
            return Ok(newest);
        }
        let newest = self.stored_slice_newest(&key)?;
        self.newest_ordinals.set_slice(key, newest);
        Ok(newest)
    }

    /// The newest ordinal any slice of `vshard` holds: the cached value,
    /// else the stored one, which the cache then keeps. `Err` when a store
    /// does not read.
    pub(in crate::data::executor) fn vshard_newest(
        &mut self,
        vshard: VShardId,
    ) -> crate::Result<Option<i64>> {
        if let Some(newest) = self.newest_ordinals.vshard(vshard) {
            return Ok(newest);
        }
        let newest = self.stored_vshard_newest(vshard)?;
        self.newest_ordinals.set_vshard(vshard, newest);
        Ok(newest)
    }
}
