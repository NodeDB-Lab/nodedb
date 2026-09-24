// SPDX-License-Identifier: BUSL-1.1

//! `CREATE VECTOR INDEX` DSL handler.
//!
//! Parses through the shared index-DDL grammar in [`super::options`], so a
//! statement is either understood exactly as written or rejected: an
//! unrecognized option keyword, an unparseable value, an unknown metric or
//! index type, and a dimension of zero are all errors at DDL time. The
//! quantization surface (INDEX_TYPE, PQ_M, IVF_CELLS, IVF_NPROBE) is validated
//! before anything is dispatched or persisted.
//!
//! The Data Plane's verdict is propagated to the client and nothing is made
//! durable until it accepts. A configuration the engine refuses must not be
//! reported as a created index, and must not be left in the catalog for the
//! next boot to seed.

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::catalog::IndexKind;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::index_registry::{
    IndexRegistration, propose_index_record,
};
use crate::control::server::shared::session::ddl_buffer;
use crate::control::state::SharedState;
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::VectorOp;

use super::super::super::result::{DdlError, DdlResult};
use super::options::{
    ColumnMode, HeaderSpec, NameMode, OptionSpec, ParsedOptions, closed_set, parse_index_statement,
};
use super::support::ddl_err;

const CONTEXT: &str = "CREATE VECTOR INDEX";
const COMMAND: &str = "CREATE VECTOR INDEX";
const LEADING: &[&str] = &["CREATE", "VECTOR", "INDEX"];

const SYNTAX: &str = "CREATE VECTOR INDEX [IF NOT EXISTS] <name> ON <collection> [(<column>)] \
     DIM <dim> [METRIC cosine|l2|inner_product|manhattan|chebyshev|hamming|jaccard|pearson] \
     [M <m>] [EF_CONSTRUCTION <ef>] [INDEX_TYPE hnsw|hnsw_pq|ivf_pq] [PQ_M <m>] \
     [IVF_CELLS <n>] [IVF_NPROBE <n>]";

const HEADER: HeaderSpec = HeaderSpec {
    name: NameMode::Required,
    columns: ColumnMode::AtMostOne,
    syntax: SYNTAX,
};

const OPTIONS: &[OptionSpec] = &[
    OptionSpec::ident("METRIC"),
    OptionSpec::uint("M"),
    OptionSpec::uint("EF_CONSTRUCTION"),
    OptionSpec::uint("DIM"),
    OptionSpec::ident("INDEX_TYPE"),
    OptionSpec::uint("PQ_M"),
    OptionSpec::uint("IVF_CELLS"),
    OptionSpec::uint("IVF_NPROBE"),
];

/// Distance metrics `execute_set_vector_params` accepts — kept in sync with
/// its `resolved_metric_str` match so a metric the Control Plane admits can
/// never be one the Data Plane refuses.
const KNOWN_METRICS: &[&str] = &[
    "l2",
    "euclidean",
    "cosine",
    "inner_product",
    "ip",
    "dot",
    "manhattan",
    "l1",
    "chebyshev",
    "linf",
    "hamming",
    "jaccard",
    "pearson",
];

/// Supported INDEX_TYPE keywords — kept in sync with
/// `nodedb_vector::index_config::IndexType`.
const KNOWN_INDEX_TYPES: &[&str] = &["hnsw", "hnsw_pq", "ivf_pq"];

/// The validated build parameters of one vector index.
struct VectorIndexParams {
    metric: String,
    m: usize,
    ef_construction: usize,
    dim: usize,
    index_type: String,
    pq_m: usize,
    ivf_cells: usize,
    ivf_nprobe: usize,
}

/// `CREATE VECTOR INDEX [IF NOT EXISTS] <name> ON <collection> [(<column>)] …`
///
/// The optional `(<column>)` names the embedding column the index covers, so
/// one collection can carry several vector indexes (e.g. a text-embedding and
/// an image-embedding column). Omitted → the collection's default vector field.
pub async fn create_vector_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let stmt = parse_index_statement(sql, LEADING, &HEADER, OPTIONS, CONTEXT)?;
    let params = validate(&stmt.options)?;

    let index_name = &stmt.header.name;
    let collection = &stmt.header.collection;
    let field_name = stmt.header.column().to_string();
    let tenant_id = identity.tenant_id;

    let existing = state
        .credentials
        .catalog()
        .get_vector_index_params(
            database_id.as_u64(),
            tenant_id.as_u64(),
            collection,
            &field_name,
        )
        .map_err(|e| ddl_err("XX000", format!("read vector index params: {e}")))?;
    if existing.is_some() {
        if stmt.header.if_not_exists {
            return Ok(vec![status()]);
        }
        return Err(ddl_err(
            "42710",
            format!(
                "a vector index already exists on '{collection}'{}; \
                 use ALTER VECTOR INDEX to change its parameters",
                describe_column(&field_name)
            ),
        ));
    }

    // A name already taken by an index of any kind would leave exactly one of
    // the two droppable, since the registry is keyed by name.
    if let Some(taken) = state
        .credentials
        .catalog()
        .get_index_record(database_id.as_u64(), tenant_id.as_u64(), index_name)
        .map_err(|e| ddl_err("XX000", format!("read index registry: {e}")))?
    {
        if stmt.header.if_not_exists && taken.kind == IndexKind::Vector {
            return Ok(vec![status()]);
        }
        return Err(ddl_err(
            "42710",
            format!(
                "index '{index_name}' already exists on '{}' ({})",
                taken.collection,
                taken.kind.display_type()
            ),
        ));
    }

    crate::control::server::shared::ddl::owner::propose_owner(
        state,
        IndexKind::Vector.owner_object_type(),
        database_id.as_u64(),
        tenant_id,
        index_name,
        &identity.username,
    )?;

    let set_params_plan = PhysicalPlan::Vector(VectorOp::SetParams {
        collection: nodedb_types::QualifiedCollection::new(database_id, collection),
        field_name: field_name.clone(),
        dim: params.dim,
        m: params.m,
        ef_construction: params.ef_construction,
        metric: params.metric.clone(),
        index_type: params.index_type.clone(),
        pq_m: params.pq_m,
        ivf_cells: params.ivf_cells,
        ivf_nprobe: params.ivf_nprobe,
    });

    // Register with the engine first and surface its verdict, before anything
    // is durable. The engine refuses to reconfigure an index that has already
    // materialized, and post-apply cannot propagate that refusal to the
    // client — this pre-flight is the only place the statement can fail
    // closed. The post-apply dispatch that follows re-installs the same
    // parameters on this node, which is a no-op on an unmaterialized index.
    //
    // Inside an explicit transaction the parameters install at COMMIT from
    // the buffered catalog row, so the pre-flight changes nothing: it probes
    // for a materialized index and refuses the same way.
    if ddl_buffer::is_active() {
        crate::control::server::shared::ddl::engine_apply::refuse_materialized_vector_index(
            state,
            tenant_id,
            database_id,
            collection,
            &field_name,
            "42P16",
            CONTEXT,
        )
        .await?;
    } else {
        crate::control::server::shared::ddl::engine_apply::apply_in_engine(
            state,
            tenant_id,
            database_id,
            collection,
            set_params_plan,
            "42P16",
            CONTEXT,
        )
        .await?;
    }

    // Only now make it durable. The replicated catalog row re-registers the
    // index at boot via `seed_vector_index_params`, and each node's post-apply
    // appends its own `VectorParams` redo record for `replay_vector_wal`, so a
    // crash between them converges on the next start.
    let stored = nodedb_types::StoredVectorIndexParams {
        database_id: database_id.as_u64(),
        tenant_id: tenant_id.as_u64(),
        collection: collection.to_string(),
        field_name: field_name.clone(),
        dim: params.dim,
        metric: params.metric.clone(),
        m: params.m,
        ef_construction: params.ef_construction,
        index_type: params.index_type.clone(),
        pq_m: params.pq_m,
        ivf_cells: params.ivf_cells,
        ivf_nprobe: params.ivf_nprobe,
    };
    let outcome = super::super::vector_replicate::propose_put_params(state, &stored)?;

    // Single node: no applier runs, so post-apply never fires. Run the
    // per-node install the post-apply lane runs everywhere else — the redo
    // record plus the fan-out that reaches every core, not just the one the
    // pre-flight dispatched to.
    if outcome.needs_local_apply() {
        let shared = state
            .self_arc()
            .map_err(|e| ddl_err("XX000", format!("install vector index params: {e}")))?;
        crate::control::catalog_entry::post_apply::install_vector_index_params(stored, shared)
            .await;
    }

    propose_index_record(
        state,
        &IndexRegistration {
            database_id,
            tenant_id,
            name: index_name,
            kind: IndexKind::Vector,
            collection,
            fields: vec![field_name.clone()],
        },
    )?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!(
            "created vector index '{index_name}' on '{collection}'{} \
             (metric={}, m={}, ef_construction={}, dim={}, index_type={}, \
             pq_m={}, ivf_cells={}, ivf_nprobe={})",
            describe_column(&field_name),
            params.metric,
            params.m,
            params.ef_construction,
            params.dim,
            params.index_type,
            params.pq_m,
            params.ivf_cells,
            params.ivf_nprobe,
        ),
    );

    Ok(vec![status()])
}

fn status() -> DdlResult {
    DdlResult::Status {
        command: COMMAND.to_string(),
        rows_affected: None,
    }
}

fn describe_column(field_name: &str) -> String {
    if field_name.is_empty() {
        String::new()
    } else {
        format!(" column '{field_name}'")
    }
}

/// Resolve every option to a value the engine will accept, or fail.
fn validate(options: &ParsedOptions) -> Result<VectorIndexParams, DdlError> {
    let metric = match options.text("METRIC") {
        Some(value) => closed_set(value, KNOWN_METRICS, "metric", CONTEXT)?,
        None => "cosine".to_string(),
    };

    let index_type = match options.text("INDEX_TYPE") {
        Some(value) => closed_set(value, KNOWN_INDEX_TYPES, "index_type", CONTEXT)?,
        None => "hnsw".to_string(),
    };

    // A zero-dimension index can never match anything, so an omitted DIM is
    // the same defect as an explicit `DIM 0` — reached without the user ever
    // typing a bad value. Both are refused here rather than at first search.
    let dim = options.uint("DIM").ok_or_else(|| {
        ddl_err(
            "42601",
            format!("{CONTEXT}: DIM is required; syntax: {SYNTAX}"),
        )
    })?;
    if dim == 0 {
        return Err(ddl_err(
            "22023",
            format!("{CONTEXT}: DIM must be greater than zero"),
        ));
    }

    let m = positive(options, "M", 16)?;
    let ef_construction = positive(options, "EF_CONSTRUCTION", 200)?;

    let uses_pq = matches!(index_type.as_str(), "hnsw_pq" | "ivf_pq");
    let pq_m = options.uint("PQ_M").unwrap_or(0);
    let ivf_cells = options.uint("IVF_CELLS").unwrap_or(0);
    let ivf_nprobe = options.uint("IVF_NPROBE").unwrap_or(0);

    if !uses_pq && (options.has("PQ_M") || options.has("IVF_CELLS") || options.has("IVF_NPROBE")) {
        return Err(ddl_err(
            "42601",
            format!(
                "{CONTEXT}: PQ_M / IVF_CELLS / IVF_NPROBE require INDEX_TYPE hnsw_pq or ivf_pq"
            ),
        ));
    }

    if uses_pq && pq_m > 0 && !dim.is_multiple_of(pq_m) {
        return Err(ddl_err(
            "22023",
            format!("{CONTEXT}: pq_m ({pq_m}) must divide dim ({dim}) evenly"),
        ));
    }

    if index_type == "ivf_pq" && ivf_nprobe > 0 && ivf_cells > 0 && ivf_nprobe > ivf_cells {
        return Err(ddl_err(
            "22023",
            format!("{CONTEXT}: ivf_nprobe ({ivf_nprobe}) must not exceed ivf_cells ({ivf_cells})"),
        ));
    }

    if index_type != "ivf_pq" && options.has("IVF_NPROBE") && options.has("IVF_CELLS") {
        return Err(ddl_err(
            "42601",
            format!("{CONTEXT}: IVF_CELLS / IVF_NPROBE require INDEX_TYPE ivf_pq"),
        ));
    }

    Ok(VectorIndexParams {
        metric,
        m,
        ef_construction,
        dim,
        index_type,
        pq_m,
        ivf_cells,
        ivf_nprobe,
    })
}

/// An explicitly specified build parameter must be positive: zero reaches the
/// engine as "unspecified" and is silently replaced by the default, which is
/// not what a statement that named the option asked for.
fn positive(
    options: &ParsedOptions,
    name: &'static str,
    default: usize,
) -> Result<usize, DdlError> {
    match options.uint(name) {
        None => Ok(default),
        Some(0) => Err(ddl_err(
            "22023",
            format!("{CONTEXT}: {name} must be greater than zero"),
        )),
        Some(value) => Ok(value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(sql: &str) -> Result<VectorIndexParams, DdlError> {
        let stmt = parse_index_statement(sql, LEADING, &HEADER, OPTIONS, CONTEXT)?;
        validate(&stmt.options)
    }

    const BASE: &str = "CREATE VECTOR INDEX idx ON coll (emb)";

    #[test]
    fn documented_form_resolves_to_its_stated_values() {
        let p = parse(&format!(
            "{BASE} METRIC cosine DIM 384 M 32 EF_CONSTRUCTION 400"
        ))
        .unwrap();
        assert_eq!(p.metric, "cosine");
        assert_eq!(p.dim, 384);
        assert_eq!(p.m, 32);
        assert_eq!(p.ef_construction, 400);
        assert_eq!(p.index_type, "hnsw");
    }

    #[test]
    fn metric_defaults_to_cosine_when_omitted() {
        assert_eq!(parse(&format!("{BASE} DIM 4")).unwrap().metric, "cosine");
    }

    #[test]
    fn unknown_metric_is_rejected() {
        assert!(parse(&format!("{BASE} METRIC euclidian DIM 4")).is_err());
    }

    #[test]
    fn metric_alias_is_normalized_not_rejected() {
        assert_eq!(
            parse(&format!("{BASE} METRIC EUCLIDEAN DIM 4"))
                .unwrap()
                .metric,
            "euclidean"
        );
    }

    #[test]
    fn dim_is_required() {
        assert!(parse(&format!("{BASE} METRIC cosine")).is_err());
    }

    #[test]
    fn zero_dim_is_rejected() {
        assert!(parse(&format!("{BASE} DIM 0")).is_err());
    }

    #[test]
    fn non_numeric_dim_is_rejected() {
        assert!(parse(&format!("{BASE} DIM three")).is_err());
    }

    #[test]
    fn non_numeric_ef_construction_is_rejected() {
        assert!(parse(&format!("{BASE} DIM 4 EF_CONSTRUCTION high")).is_err());
    }

    #[test]
    fn zero_valued_build_parameter_is_rejected() {
        assert!(parse(&format!("{BASE} DIM 4 M 0")).is_err());
    }

    #[test]
    fn unrecognized_option_syntax_is_rejected() {
        assert!(parse(&format!("{BASE} WITH (dim = 3, metric = 'cosine')")).is_err());
    }

    #[test]
    fn unknown_index_type_is_rejected() {
        assert!(parse(&format!("{BASE} DIM 4 INDEX_TYPE bogus_type")).is_err());
    }

    #[test]
    fn pq_m_must_divide_dim() {
        assert!(parse(&format!("{BASE} DIM 6 INDEX_TYPE hnsw_pq PQ_M 4")).is_err());
        assert!(parse(&format!("{BASE} DIM 4 INDEX_TYPE hnsw_pq PQ_M 2")).is_ok());
    }

    #[test]
    fn quantization_parameters_require_a_quantized_index_type() {
        assert!(parse(&format!("{BASE} DIM 4 PQ_M 2")).is_err());
    }

    #[test]
    fn ivf_nprobe_must_not_exceed_ivf_cells() {
        assert!(
            parse(&format!(
                "{BASE} DIM 4 INDEX_TYPE ivf_pq PQ_M 2 IVF_CELLS 8 IVF_NPROBE 64"
            ))
            .is_err()
        );
        assert!(
            parse(&format!(
                "{BASE} DIM 4 INDEX_TYPE ivf_pq PQ_M 2 IVF_CELLS 64 IVF_NPROBE 8"
            ))
            .is_ok()
        );
    }

    #[test]
    fn if_not_exists_is_parsed_off_the_header() {
        let stmt = parse_index_statement(
            "CREATE VECTOR INDEX IF NOT EXISTS idx ON coll (emb) DIM 4",
            LEADING,
            &HEADER,
            OPTIONS,
            CONTEXT,
        )
        .unwrap();
        assert!(stmt.header.if_not_exists);
        assert_eq!(stmt.header.name, "idx");
        assert_eq!(stmt.header.column(), "emb");
    }

    #[test]
    fn column_clause_is_optional() {
        let stmt = parse_index_statement(
            "CREATE VECTOR INDEX idx ON coll DIM 4",
            LEADING,
            &HEADER,
            OPTIONS,
            CONTEXT,
        )
        .unwrap();
        assert_eq!(stmt.header.column(), "");
    }
}
