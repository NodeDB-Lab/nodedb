// SPDX-License-Identifier: BUSL-1.1

//! Resolve the `WITH (primary='vector', vector_field=...)` access-path
//! config for `build_and_persist`.

use super::super::super::super::result::DdlError;
use super::build_flags::err;

/// The full form of a vector-engine collection, shown when one is refused.
const VECTOR_ENGINE_FORM: &str = "CREATE COLLECTION <name> (id STRING PRIMARY KEY, \
     <field> VECTOR(<n>)) WITH (engine='vector', vector_field='<field>', dim=<n>)";

/// The WITH options a collection is built from, with the vector engine's
/// access path made explicit.
///
/// `engine='vector'` selects the vector engine, which is a vector-primary
/// collection. With no `primary` key it implies `primary='vector'`. Another
/// `primary` contradicts it and is refused, as is a missing `vector_field`
/// or `dim`. A document collection searched through a vector index uses
/// `engine='document_schemaless'`. Any other engine passes its options
/// through unchanged.
pub(super) fn vector_engine_options(
    engine: Option<&str>,
    options: &[(String, String)],
) -> Result<Vec<(String, String)>, DdlError> {
    let mut resolved = options.to_vec();
    if engine != Some("vector") {
        return Ok(resolved);
    }
    let get = |key: &str| {
        options
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(key))
            .map(|(_, v)| v.as_str())
    };
    match get("primary") {
        None => resolved.push(("primary".to_string(), "vector".to_string())),
        Some(p) if p.eq_ignore_ascii_case("vector") => {}
        Some(other) => {
            return Err(err(
                "42601",
                format!(
                    "engine='vector' is a vector-primary collection, but primary='{other}' \
                     names another access path; drop primary=, or for a document collection \
                     searched through a vector index use engine='document_schemaless'. \
                     Full form: {VECTOR_ENGINE_FORM}"
                ),
            ));
        }
    }
    let missing: Vec<&str> = ["vector_field", "dim"]
        .into_iter()
        .filter(|key| get(key).is_none_or(|v| v.trim().is_empty()))
        .collect();
    if !missing.is_empty() {
        return Err(err(
            "42601",
            format!(
                "engine='vector' requires the {} option{}. Full form: {VECTOR_ENGINE_FORM}",
                missing.join(" and "),
                if missing.len() > 1 { "s" } else { "" }
            ),
        ));
    }
    Ok(resolved)
}

/// Resolve `PrimaryEngine` + optional `VectorPrimaryConfig` from the
/// WITH-clause `primary=` / `vector_field=` knobs. Validates the
/// vector field exists in the column list and the declared `dim`
/// matches the column's `VECTOR(n)` type when both are present.
pub(super) fn resolve_primary_engine(
    options: &[(String, String)],
    columns: &[(String, String)],
    fields: &[(String, String)],
    collection_type: &nodedb_types::CollectionType,
) -> Result<
    (
        nodedb_types::PrimaryEngine,
        Option<nodedb_types::VectorPrimaryConfig>,
    ),
    DdlError,
> {
    match nodedb_sql::ddl_ast::parse::vector_primary::parse_vector_primary_options_from_kvs(options)
    {
        Ok(Some(mut vp_cfg)) => {
            let col_list: Vec<(String, String)> = if fields.is_empty() {
                columns.to_vec()
            } else {
                fields.to_vec()
            };
            nodedb_sql::ddl_ast::parse::vector_primary::validate_vector_field(&vp_cfg, &col_list)
                .map_err(|e| err("42601", e.to_string()))?;
            nodedb_sql::ddl_ast::parse::vector_primary::validate_payload_indexes(
                &mut vp_cfg,
                &col_list,
            )
            .map_err(|e| err("42601", e.to_string()))?;
            // Infer dim from VECTOR(n) column type when not in WITH clause.
            if let Some((_, type_str)) = col_list
                .iter()
                .find(|(n, _)| n.eq_ignore_ascii_case(&vp_cfg.vector_field))
            {
                let upper_t = type_str.to_uppercase();
                if let Some(inner) = upper_t
                    .strip_prefix("VECTOR(")
                    .and_then(|s| s.strip_suffix(')'))
                    && let Ok(d) = inner.trim().parse::<u32>()
                {
                    if vp_cfg.dim == 0 {
                        vp_cfg.dim = d;
                    } else if vp_cfg.dim != d {
                        return Err(err(
                            "42601",
                            format!(
                                "vector dim mismatch: WITH clause specifies {}, column type VECTOR({}) specifies {}",
                                vp_cfg.dim, d, d
                            ),
                        ));
                    }
                }
            }
            check_codec_shape(&vp_cfg)?;
            Ok((nodedb_types::PrimaryEngine::Vector, Some(vp_cfg)))
        }
        Ok(None) => Ok((
            nodedb_types::PrimaryEngine::infer_from_collection_type(collection_type),
            None,
        )),
        Err(e) => Err(err("42601", e.to_string())),
    }
}

/// Refuse a quantization the codec cannot build at the collection's dim. PQ
/// and OPQ must train at the default `pq_m`, with which a vector-primary
/// collection builds its codec. A graph codec stores the dimension as `u16`.
fn check_codec_shape(cfg: &nodedb_types::VectorPrimaryConfig) -> Result<(), DdlError> {
    use nodedb_types::VectorQuantization;
    use nodedb_vector::collection::check_graph_codec_shape;
    let dim = cfg.dim as usize;
    let pq_m = nodedb_vector::index_config::DEFAULT_PQ_M;
    let shape = match cfg.quantization {
        VectorQuantization::Pq => nodedb_vector::quantize::pq::check_pq_shape(dim, pq_m),
        VectorQuantization::Opq => check_graph_codec_shape(dim)
            .and_then(|()| nodedb_vector::quantize::pq::check_opq_shape(dim, pq_m)),
        VectorQuantization::RaBitQ
        | VectorQuantization::Bbq
        | VectorQuantization::Binary
        | VectorQuantization::Ternary => check_graph_codec_shape(dim),
        VectorQuantization::None | VectorQuantization::Sq8 => Ok(()),
    };
    shape.map_err(|e| err("22023", e.to_string()))
}
