// SPDX-License-Identifier: BUSL-1.1

//! Shared backup and restore steps for the wire backup tests.

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use nodedb_types::id::{DatabaseId, VShardId};

/// Take a backup of `tenant` over `client` and return the envelope bytes.
pub async fn drain_backup(client: &tokio_postgres::Client, tenant: u64) -> Result<Vec<u8>, String> {
    let stream = client
        .copy_out(&format!("COPY (BACKUP TENANT {tenant}) TO STDOUT"))
        .await
        .map_err(|e| format!("copy_out: {e:?}"))?;
    let mut bytes = Vec::new();
    let mut stream = Box::pin(stream);
    while let Some(chunk) = stream.next().await {
        bytes.extend_from_slice(&chunk.map_err(|e| format!("copy_out chunk: {e:?}"))?);
    }
    Ok(bytes)
}

/// Restore `envelope` into `tenant` over `client`. The error carries the
/// server's full error text.
pub async fn push_restore(
    client: &tokio_postgres::Client,
    tenant: u64,
    envelope: Vec<u8>,
) -> Result<(), String> {
    let sink = client
        .copy_in::<_, Bytes>(&format!("COPY tenant_restore({tenant}) FROM STDIN"))
        .await
        .map_err(|e| format!("copy_in: {e:?}"))?;
    let mut sink = Box::pin(sink);
    sink.as_mut()
        .send(Bytes::from(envelope))
        .await
        .map_err(|e| format!("send: {e:?}"))?;
    sink.as_mut()
        .finish()
        .await
        .map(|_| ())
        .map_err(|e| format!("{e:?}"))
}

/// Two collection names, `<prefix>_a_<n>` and `<prefix>_b_<n>`, that home on
/// different vShards. A transaction that writes both commits through the
/// Calvin scheduler.
pub fn names_on_two_vshards(prefix: &str) -> (String, String) {
    let first = format!("{prefix}_a");
    let first_vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &first).as_u32();
    let second = (0..512u32)
        .map(|i| format!("{prefix}_b_{i}"))
        .find(|name| {
            VShardId::from_collection_in_database(DatabaseId::DEFAULT, name).as_u32()
                != first_vshard
        })
        .unwrap_or_else(|| panic!("no {prefix}_b name on another vShard in 512 tries"));
    (first, second)
}
