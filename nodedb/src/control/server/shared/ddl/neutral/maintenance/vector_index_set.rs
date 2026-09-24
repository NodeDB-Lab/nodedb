// SPDX-License-Identifier: BUSL-1.1

//! `ALTER VECTOR INDEX ON collection.column SET (...)`.
//!
//! The statement changes replicated catalog state, so it proposes the whole
//! post-ALTER `StoredVectorIndexParams` row through the same helper `CREATE
//! VECTOR INDEX` uses. Apply writes that row verbatim on every node, and each
//! node's post-apply lane appends its own redo record and brings its cores to
//! the row. A node-local dispatch alone leaves every other node on the
//! CREATE-time parameters, which it then rebuilds from at its next boot.
//!
//! A row must therefore hold the complete post-ALTER state. The statement
//! carries only the fields it names, so the handler reads the stored row and
//! overlays those fields onto it.

use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive;
use nodedb_types::StoredVectorIndexParams;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::support::ddl_err;
use super::vector_index::parse_collection_column;

/// The fields one SET clause names. Zero / `None` means the statement left the
/// field alone and the stored value stands.
#[derive(Default)]
struct ParamOverlay {
    m: usize,
    ef_construction: usize,
    index_type: Option<String>,
    pq_m: usize,
    ivf_cells: usize,
    ivf_nprobe: usize,
}

/// Handle `ALTER VECTOR INDEX ON collection.column SET (...)`.
///
/// Supported keys: `m`, `ef_construction`, `index_type`, `pq_m`, `ivf_cells`,
/// `ivf_nprobe`. The statement never redeclares the dimension or the metric —
/// both keep whatever CREATE declared.
pub async fn handle_alter_vector_index_set(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let (collection, field_name) = parse_collection_column(sql, " ON ")?;
    let overlay = parse_set_clause(sql)?;
    let tenant_id = identity.tenant_id;

    let current = state
        .credentials
        .catalog()
        .get_vector_index_params(
            database_id.as_u64(),
            tenant_id.as_u64(),
            &collection,
            &field_name,
        )
        .map_err(|e| ddl_err("XX000", format!("read vector index params: {e}")))?
        .ok_or_else(|| {
            ddl_err(
                "42704",
                format!(
                    "no vector index exists on '{collection}'{}; \
                     use CREATE VECTOR INDEX to create one",
                    describe_column(&field_name)
                ),
            )
        })?;

    let merged = merge(current, &overlay);
    validate(&merged, &overlay)?;
    let outcome = super::super::vector_replicate::propose_put_params(state, &merged)?;

    // Single node: no applier runs, so post-apply never fires. Run the
    // per-node install the post-apply lane runs everywhere else.
    if outcome.needs_local_apply() {
        let shared = state
            .self_arc()
            .map_err(|e| ddl_err("XX000", format!("install vector index params: {e}")))?;
        crate::control::catalog_entry::post_apply::install_vector_index_params(merged, shared)
            .await;
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!(
            "altered vector index on '{collection}'{}",
            describe_column(&field_name)
        ),
    );

    Ok(vec![DdlResult::Status {
        command: "ALTER VECTOR INDEX".to_string(),
        rows_affected: None,
    }])
}

fn describe_column(field_name: &str) -> String {
    if field_name.is_empty() {
        String::new()
    } else {
        format!(" column '{field_name}'")
    }
}

/// Overlay the named fields onto the stored row, producing the complete
/// post-ALTER state every node writes.
///
/// The dimension, the metric, and the identity fields come from the stored row
/// alone: ALTER names none of them, and a zero written into `dim` clears an
/// enforced width for every node that seeds from the row.
fn merge(stored: StoredVectorIndexParams, overlay: &ParamOverlay) -> StoredVectorIndexParams {
    let index_type = overlay
        .index_type
        .clone()
        .unwrap_or_else(|| stored.index_type.clone());
    StoredVectorIndexParams {
        m: if overlay.m > 0 { overlay.m } else { stored.m },
        ef_construction: if overlay.ef_construction > 0 {
            overlay.ef_construction
        } else {
            stored.ef_construction
        },
        index_type,
        pq_m: if overlay.pq_m > 0 {
            overlay.pq_m
        } else {
            stored.pq_m
        },
        ivf_cells: if overlay.ivf_cells > 0 {
            overlay.ivf_cells
        } else {
            stored.ivf_cells
        },
        ivf_nprobe: if overlay.ivf_nprobe > 0 {
            overlay.ivf_nprobe
        } else {
            stored.ivf_nprobe
        },
        ..stored
    }
}

/// Refuse a merged row the engine cannot build, before it replicates.
///
/// Apply writes the row on every node and cannot reject, so a row that fails
/// these rules would reach every node and break each one's build. CREATE
/// enforces the same three rules on its own row.
///
/// The first rule reads the overlay, not the merged row: an index moving from
/// a quantized type back to plain HNSW carries quantization values the new
/// type ignores, and that is not the statement asking for them.
fn validate(row: &StoredVectorIndexParams, overlay: &ParamOverlay) -> Result<(), DdlError> {
    let uses_pq = matches!(row.index_type.as_str(), "hnsw_pq" | "ivf_pq");
    let names_quantization = overlay.pq_m > 0 || overlay.ivf_cells > 0 || overlay.ivf_nprobe > 0;

    if names_quantization && !uses_pq {
        return Err(ddl_err(
            "42601",
            format!(
                "pq_m / ivf_cells / ivf_nprobe require index_type hnsw_pq or ivf_pq, \
                 and the index is '{}'",
                row.index_type
            ),
        ));
    }

    if uses_pq && row.pq_m > 0 && !row.dim.is_multiple_of(row.pq_m) {
        return Err(ddl_err(
            "22023",
            format!("pq_m ({}) must divide dim ({}) evenly", row.pq_m, row.dim),
        ));
    }

    if row.index_type == "ivf_pq"
        && row.ivf_nprobe > 0
        && row.ivf_cells > 0
        && row.ivf_nprobe > row.ivf_cells
    {
        return Err(ddl_err(
            "22023",
            format!(
                "ivf_nprobe ({}) must not exceed ivf_cells ({})",
                row.ivf_nprobe, row.ivf_cells
            ),
        ));
    }

    Ok(())
}

/// Parse the `SET (...)` list into the fields it names.
fn parse_set_clause(sql: &str) -> Result<ParamOverlay, DdlError> {
    let set_pos = find_ascii_case_insensitive(sql, " SET ").ok_or_else(|| {
        ddl_err(
            "42601",
            "ALTER VECTOR INDEX ... SET (...) requires SET clause",
        )
    })?;
    let params_str = &sql[set_pos + 5..];

    // Strip surrounding parens.
    let inner = params_str
        .trim()
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .unwrap_or(params_str.trim());

    if inner.trim().is_empty() {
        return Err(ddl_err(
            "42601",
            "SET clause must specify at least one parameter (m, ef_construction, \
             index_type, pq_m, ivf_cells, ivf_nprobe)",
        ));
    }

    let mut overlay = ParamOverlay::default();

    for pair in inner.split(',') {
        let pair = pair.trim();
        // A list item with no `=` must not be skipped — silently dropping a
        // typo'd item would report success for the ones around it.
        let Some((key, val)) = pair.split_once('=') else {
            return Err(ddl_err(
                "42601",
                format!("malformed SET item '{pair}'; each item must be <parameter> = <value>"),
            ));
        };
        let key = key.trim().to_lowercase();
        let val = val.trim().trim_matches('\'').trim_matches('"');
        match key.as_str() {
            "m" => overlay.m = uint(val, "m")?,
            "ef_construction" => overlay.ef_construction = uint(val, "ef_construction")?,
            "index_type" => {
                let lower = val.to_lowercase();
                if !matches!(lower.as_str(), "hnsw" | "hnsw_pq" | "ivf_pq") {
                    return Err(ddl_err(
                        "42601",
                        format!("unknown index_type '{val}'; supported: hnsw, hnsw_pq, ivf_pq"),
                    ));
                }
                overlay.index_type = Some(lower);
            }
            "pq_m" => overlay.pq_m = uint(val, "pq_m")?,
            "ivf_cells" => overlay.ivf_cells = uint(val, "ivf_cells")?,
            "ivf_nprobe" => overlay.ivf_nprobe = uint(val, "ivf_nprobe")?,
            // `m0` is derived as `2 * m` by every path that installs an index —
            // CREATE, the boot seed, and WAL replay alike. A row cannot carry a
            // different ratio, so honouring one here would hold only until the
            // next restart, on the one node that ran the statement.
            "m0" => {
                return Err(ddl_err(
                    "42601",
                    "m0 is derived as 2 * m and cannot be set; set m instead",
                ));
            }
            other => {
                return Err(ddl_err(
                    "42601",
                    format!(
                        "unknown parameter '{other}'; supported: m, ef_construction, \
                         index_type, pq_m, ivf_cells, ivf_nprobe"
                    ),
                ));
            }
        }
    }

    Ok(overlay)
}

/// Parse one positive-integer option value.
///
/// Zero reaches the merge as "unspecified" and keeps the stored value, which is
/// not what a statement that named the parameter asked for.
fn uint(value: &str, name: &str) -> Result<usize, DdlError> {
    match value.parse::<usize>() {
        Ok(0) => Err(ddl_err(
            "22023",
            format!("{name} must be greater than zero"),
        )),
        Ok(parsed) => Ok(parsed),
        Err(_) => Err(ddl_err(
            "22023",
            format!("invalid value for {name}: {value}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stored() -> StoredVectorIndexParams {
        StoredVectorIndexParams {
            database_id: 7,
            tenant_id: 3,
            collection: "documents".to_string(),
            field_name: "embedding".to_string(),
            dim: 384,
            metric: "cosine".to_string(),
            m: 16,
            ef_construction: 200,
            index_type: "hnsw".to_string(),
            pq_m: 0,
            ivf_cells: 0,
            ivf_nprobe: 0,
        }
    }

    fn merged(clause: &str) -> StoredVectorIndexParams {
        let overlay = parse_set_clause(&format!(
            "ALTER VECTOR INDEX ON documents.embedding SET ({clause})"
        ))
        .expect("clause parses");
        merge(stored(), &overlay)
    }

    /// A partial SET keeps every field it does not name. A field reset to its
    /// default here is a silent downgrade on every node that seeds from the row.
    #[test]
    fn an_unnamed_field_keeps_its_stored_value() {
        let row = merged("m = 32");
        assert_eq!(row.m, 32);
        assert_eq!(row.ef_construction, 200);
        assert_eq!(row.metric, "cosine");
        assert_eq!(row.index_type, "hnsw");
        assert_eq!(row.pq_m, 0);
        assert_eq!(row.ivf_cells, 0);
        assert_eq!(row.ivf_nprobe, 0);
    }

    /// The dimension is never redeclared: a zero written here clears the width
    /// every node enforces.
    #[test]
    fn the_dimension_survives_every_clause() {
        assert_eq!(merged("m = 32").dim, 384);
        assert_eq!(merged("index_type = 'ivf_pq', ivf_cells = 64").dim, 384);
    }

    /// The row keys the catalog write, so a merge that moved it would land the
    /// altered parameters on a different index.
    #[test]
    fn the_identity_fields_survive_every_clause() {
        let row = merged("ef_construction = 400");
        assert_eq!(row.database_id, 7);
        assert_eq!(row.tenant_id, 3);
        assert_eq!(row.collection, "documents");
        assert_eq!(row.field_name, "embedding");
    }

    #[test]
    fn every_named_field_reaches_the_row() {
        let row = merged(
            "m = 48, ef_construction = 500, index_type = 'ivf_pq', \
             pq_m = 8, ivf_cells = 128, ivf_nprobe = 16",
        );
        assert_eq!(row.m, 48);
        assert_eq!(row.ef_construction, 500);
        assert_eq!(row.index_type, "ivf_pq");
        assert_eq!(row.pq_m, 8);
        assert_eq!(row.ivf_cells, 128);
        assert_eq!(row.ivf_nprobe, 16);
    }

    /// A quantization change keeps the HNSW shape the index was built with.
    #[test]
    fn a_quantization_clause_keeps_the_hnsw_shape() {
        let mut base = stored();
        base.m = 24;
        base.ef_construction = 300;
        let overlay = parse_set_clause("ALTER VECTOR INDEX ON x SET (index_type = 'hnsw_pq')")
            .expect("clause parses");
        let row = merge(base, &overlay);
        assert_eq!(row.m, 24);
        assert_eq!(row.ef_construction, 300);
        assert_eq!(row.index_type, "hnsw_pq");
    }

    fn parse(clause: &str) -> Result<ParamOverlay, DdlError> {
        parse_set_clause(&format!("ALTER VECTOR INDEX ON x SET ({clause})"))
    }

    #[test]
    fn an_empty_clause_is_rejected() {
        assert!(parse("").is_err());
    }

    #[test]
    fn a_malformed_item_is_rejected() {
        assert!(parse("m = 32, ef_construction").is_err());
    }

    #[test]
    fn an_unknown_parameter_is_rejected() {
        assert!(parse("bogus = 4").is_err());
    }

    #[test]
    fn an_unknown_index_type_is_rejected() {
        assert!(parse("index_type = 'bogus'").is_err());
    }

    #[test]
    fn a_non_numeric_value_is_rejected() {
        assert!(parse("m = many").is_err());
    }

    #[test]
    fn a_zero_valued_parameter_is_rejected() {
        assert!(parse("m = 0").is_err());
    }

    /// `m0` cannot be made durable, so accepting it would apply the ratio on
    /// one node until its next restart.
    #[test]
    fn m0_is_rejected_rather_than_dropped() {
        assert!(parse("m0 = 64").is_err());
    }

    fn check(clause: &str) -> Result<(), DdlError> {
        let overlay = parse(clause).expect("clause parses");
        let row = merge(stored(), &overlay);
        validate(&row, &overlay)
    }

    #[test]
    fn quantization_parameters_require_a_quantized_index_type() {
        assert!(check("pq_m = 2").is_err());
        assert!(check("index_type = 'hnsw_pq', pq_m = 2").is_ok());
    }

    #[test]
    fn pq_m_must_divide_the_declared_dim() {
        // The stored row declares dim 384.
        assert!(check("index_type = 'hnsw_pq', pq_m = 5").is_err());
        assert!(check("index_type = 'hnsw_pq', pq_m = 8").is_ok());
    }

    #[test]
    fn ivf_nprobe_must_not_exceed_ivf_cells() {
        assert!(check("index_type = 'ivf_pq', ivf_cells = 8, ivf_nprobe = 64").is_err());
        assert!(check("index_type = 'ivf_pq', ivf_cells = 64, ivf_nprobe = 8").is_ok());
    }

    /// Moving back to plain HNSW leaves the old quantization values in the row,
    /// where the new type ignores them. The statement asked for none of them.
    #[test]
    fn returning_to_hnsw_is_not_blocked_by_stale_quantization_values() {
        let mut base = stored();
        base.index_type = "ivf_pq".to_string();
        base.pq_m = 8;
        base.ivf_cells = 64;
        base.ivf_nprobe = 8;
        let overlay = parse("index_type = 'hnsw'").expect("clause parses");
        let row = merge(base, &overlay);
        validate(&row, &overlay).expect("a downgrade to hnsw is accepted");
        assert_eq!(row.index_type, "hnsw");
    }
}
