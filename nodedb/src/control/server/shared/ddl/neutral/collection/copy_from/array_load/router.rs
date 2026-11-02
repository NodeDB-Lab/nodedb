// SPDX-License-Identifier: BUSL-1.1

//! Picks the chunk bucket of each row of an array bulk load.

use nodedb_array::schema::ArraySchema;
use nodedb_array::tile::TilePrefixMemo;
use nodedb_sql::types_array::ArrayCoordLiteral;

use crate::control::planner::sql_plan_convert::array_convert::cell_vshard;
use crate::control::server::shared::ddl::result::DdlError;

use super::dispatch::ArrayLoadCtx;

/// Picks each row's chunk bucket: its tile's vShard, so each chunk reaches
/// one shard.
pub(super) struct CellRouter {
    schema: Box<ArraySchema>,
    prefix_bits: u8,
    /// The last tile key, so a run of cells in one tile encodes once.
    memo: TilePrefixMemo,
}

impl CellRouter {
    pub(super) fn for_array(ctx: &ArrayLoadCtx<'_>, array: &str) -> Result<Self, DdlError> {
        let entry = {
            let catalog = ctx
                .state
                .array_catalog
                .read()
                .map_err(|_| DdlError::internal("array catalog lock poisoned"))?;
            catalog
                .lookup_by_name_in_database(ctx.identity.tenant_id, ctx.database_id, array)
                .ok_or_else(|| {
                    DdlError::new("42P01", format!("COPY: array '{array}' does not exist"))
                })?
        };
        let schema: ArraySchema = zerompk::from_msgpack(&entry.schema_msgpack)
            .map_err(|e| DdlError::internal(format!("COPY: array '{array}' schema decode: {e}")))?;
        Ok(CellRouter {
            schema: Box::new(schema),
            prefix_bits: entry.prefix_bits,
            memo: TilePrefixMemo::new(),
        })
    }

    pub(super) fn vshard(&mut self, coords: &[ArrayCoordLiteral]) -> crate::Result<u32> {
        cell_vshard(coords, &self.schema, self.prefix_bits, &mut self.memo)
    }
}
