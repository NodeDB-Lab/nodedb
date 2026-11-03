// SPDX-License-Identifier: BUSL-1.1

//! The surrogate bitmap of an array slice, read from every shard that owns
//! the array's tiles.
//!
//! A vector search with an `ARRAY_SLICE` prefilter runs on the core that
//! owns its collection. That core holds only its own buckets of the array,
//! so the prefilter is read here across every primary vShard and handed to
//! the search as its candidate bitmap.

use std::sync::Arc;

use nodedb_cluster::distributed_array::coordinator::ArrayCoordinator;
use nodedb_cluster::distributed_array::wire::ArrayShardSurrogateBitmapReq;
use nodedb_types::{SurrogateBitmap, SystemTimeScope};

use crate::types::TxnId;

use crate::control::cluster::array_cluster_helpers::{cluster_err, coord_err, encode_err};

use super::executor::{ARRAY_RPC_TIMEOUT_MS, ClusterArrayExecutor, note_array_reads};

impl ClusterArrayExecutor {
    /// The union of every shard's surrogate bitmap for the cells of
    /// `array_id` inside `slice_msgpack`.
    ///
    /// Every primary vShard answers. vShards that share one Data Plane
    /// report the same surrogates, which the union keeps once.
    ///
    /// Every shard reads at `system_time`, so the union holds one cut.
    /// Every answering vShard is noted for the running request's
    /// transaction read set.
    /// `txn_id` lets a current or transaction snapshot read on each shard
    /// merge the cells the transaction staged there.
    pub async fn surrogate_bitmap(
        &self,
        array_id: &nodedb_array::types::ArrayId,
        slice_msgpack: &[u8],
        prefix_bits: u8,
        system_time: SystemTimeScope,
        txn_id: Option<TxnId>,
    ) -> crate::Result<SurrogateBitmap> {
        let coordinator = ArrayCoordinator::for_slice(
            self.source_node,
            ARRAY_RPC_TIMEOUT_MS,
            &[],
            prefix_bits,
            self.total_shards,
            Arc::clone(&self.dispatch),
            Arc::clone(&self.circuit_breaker),
        )
        .map_err(cluster_err)?;
        let req = ArrayShardSurrogateBitmapReq {
            array_id_msgpack: zerompk::to_msgpack_vec(array_id).map_err(encode_err)?,
            slice_msgpack: slice_msgpack.to_vec(),
            system_time,
            txn_id: txn_id.map(TxnId::as_u64),
        };
        let resps = coordinator
            .coord_surrogate_bitmap_scan(req)
            .await
            .map_err(coord_err)?;
        // Each answering vShard read the cells its tiles hold. The note
        // joins them to the transaction read set as `ARRAY_SLICE` does, so a
        // concurrent cell write to any of them fails the block's COMMIT.
        let read_vshards: Vec<u32> = resps.iter().map(|resp| resp.shard_id).collect();
        note_array_reads(array_id, &read_vshards);
        let mut union = SurrogateBitmap::new();
        for resp in resps {
            let bitmap: SurrogateBitmap =
                zerompk::from_msgpack(&resp.bitmap_msgpack).map_err(|e| crate::Error::Codec {
                    detail: format!(
                        "array surrogate bitmap from vShard {} decode: {e}",
                        resp.shard_id
                    ),
                })?;
            union.union_in_place(&bitmap);
        }
        Ok(union)
    }
}
