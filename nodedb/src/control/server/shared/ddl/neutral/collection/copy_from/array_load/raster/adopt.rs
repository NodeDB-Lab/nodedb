// SPDX-License-Identifier: BUSL-1.1

//! `ADOPT_METADATA true`: write the attr metadata the files declare into
//! the array schema before the first row loads.
//!
//! One catalog entry carries every attr's change, built from the committed
//! schema under the DDL preparation lease, as `ALTER ARRAY ... ALTER ATTR`
//! builds its own. Metadata sits outside the schema content, so
//! `schema_hash` and every segment stay valid. A value the attr type cannot
//! hold is refused (`22000`) before anything loads. The change is DDL, so a
//! COPY inside a transaction block cannot make it (`25001`).

use nodedb_sql::types_array::ArrayAttrMetaAlter;

use crate::control::array_catalog::ArrayCatalogEntry;
use crate::control::array_catalog::attr_meta::remeta_schema;
use crate::control::array_catalog::ddl::propose_array_entries;
use crate::control::array_catalog::schema_recode::check_same_content;
use crate::control::catalog_entry::CatalogEntry;
use crate::control::server::shared::ddl::result::DdlError;
use crate::control::trigger::statement_txn::in_block;

use super::super::dispatch::ArrayLoadCtx;

/// Apply `changes` to `array`'s attr metadata in one catalog entry.
pub(super) async fn adopt_attr_metadata(
    ctx: &ArrayLoadCtx<'_>,
    array: &str,
    changes: &[ArrayAttrMetaAlter],
) -> Result<(), DdlError> {
    if changes.is_empty() {
        return Ok(());
    }
    if in_block(ctx.txn_ctx) {
        return Err(DdlError::new(
            "25001",
            format!(
                "COPY {array}: ADOPT_METADATA changes the array schema; run the COPY outside a \
                 transaction block"
            ),
        ));
    }
    let tenant_id = ctx.identity.tenant_id;
    let database_id = ctx.database_id;
    let statement = format!("COPY {array} WITH (ADOPT_METADATA true)");
    propose_array_entries(ctx.state, |catalog| {
        let current = catalog
            .get_array_in_database(tenant_id, database_id, array)?
            .ok_or_else(|| crate::Error::PlanError {
                detail: format!("{statement}: array not found"),
            })?;
        let mut schema = current.schema_msgpack.clone();
        for change in changes {
            schema = remeta_schema(&statement, &schema, change)?;
        }
        check_same_content(&statement, &current.schema_msgpack, &schema)?;
        Ok(vec![CatalogEntry::PutArray(Box::new(ArrayCatalogEntry {
            schema_msgpack: schema,
            ..current
        }))])
    })
    .await
    .map_err(DdlError::from)
}
