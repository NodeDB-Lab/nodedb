// SPDX-License-Identifier: BUSL-1.1

//! Refusal of a generated column as the join column of a materialized sum.
//!
//! The Control Plane names the target row of a cross-shard sum from the join
//! value in the write body. A generated column is absent from that body, so
//! the Control Plane resolves no target row for the value the Data Plane
//! stores. Every DDL that pairs a generated column with a bound join name
//! refuses it with SQLSTATE `0A000`: the bind, `CREATE COLLECTION` of a bound
//! source, `ADD COLUMN` and `RENAME COLUMN`.

use nodedb_types::DatabaseId;

use crate::control::security::catalog::SystemCatalog;
use crate::control::server::shared::ddl::result::DdlError;

/// Refuse when a join column in `join_columns` names a column in `generated`.
///
/// Column names compare without regard to ASCII letter case.
pub(super) fn refuse_generated_join_column(
    source: &str,
    generated: &[String],
    join_columns: &[String],
) -> Result<(), DdlError> {
    let Some(join_column) = join_columns.iter().find(|join| {
        generated
            .iter()
            .any(|g| g.eq_ignore_ascii_case(join.as_str()))
    }) else {
        return Ok(());
    };
    Err(DdlError::new(
        "0A000",
        format!(
            "materialized sum join column '{source}.{join_column}' is a generated column; \
             bind the sum on a stored column"
        ),
    ))
}

/// Refuse when a sum bound to `source` names a column in `generated` as its
/// join column.
///
/// Reads the bindings of every collection of the tenant. An empty `generated`
/// reads nothing.
pub(super) fn refuse_bound_generated_join_column(
    catalog: &SystemCatalog,
    database_id: DatabaseId,
    tenant_id: u64,
    source: &str,
    generated: &[String],
) -> Result<(), DdlError> {
    if generated.is_empty() {
        return Ok(());
    }
    let join_columns: Vec<String> = catalog
        .load_collections_for_tenant(database_id, tenant_id)
        .map_err(|e| DdlError::from_error(&e))?
        .into_iter()
        .flat_map(|target| target.materialized_sums)
        .filter(|def| def.source_collection == source)
        .map(|def| def.join_column)
        .collect();
    refuse_generated_join_column(source, generated, &join_columns)
}
