// SPDX-License-Identifier: Apache-2.0

use super::super::quote::quote_ident;
use nodedb_types::dropped_collection::DroppedCollection;
use nodedb_types::error::{NodeDbError, NodeDbResult};

use super::super::trait_def::NodeDb;

pub(in crate::traits::core) async fn undrop_collection_default<T: NodeDb + ?Sized>(
    this: &T,
    name: &str,
) -> NodeDbResult<()> {
    let sql = format!("UNDROP COLLECTION {}", quote_ident(name));
    this.execute_sql(&sql, &[]).await?;
    Ok(())
}

pub(in crate::traits::core) async fn drop_collection_purge_default<T: NodeDb + ?Sized>(
    this: &T,
    name: &str,
) -> NodeDbResult<()> {
    let sql = format!("DROP COLLECTION {} PURGE", quote_ident(name));
    this.execute_sql(&sql, &[]).await?;
    Ok(())
}

pub(in crate::traits::core) async fn list_dropped_collections_default<T: NodeDb + ?Sized>(
    this: &T,
) -> NodeDbResult<Vec<DroppedCollection>> {
    let sql = "SELECT tenant_id, name, owner, engine_type, \
               deactivated_at_ns, retention_expires_at_ns \
               FROM _system.dropped_collections";
    let result = this.execute_sql(sql, &[]).await?;
    crate::row_decode::parse_dropped_collection_rows(&result.rows)
}

pub(in crate::traits::core) fn on_collection_purged_default() -> NodeDbResult<()> {
    Err(NodeDbError::storage(
        "on_collection_purged is not supported on this client — \
         requires a push-capable sync connection (NodeDbLite or a \
         sync-enabled remote client)",
    ))
}
