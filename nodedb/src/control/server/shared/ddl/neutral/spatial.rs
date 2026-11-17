// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `CREATE SPATIAL INDEX` DDL handling.
//!
//! Syntax:
//! ```sql
//! CREATE SPATIAL INDEX [IF NOT EXISTS] [<name>] ON <collection>(<field>)
//!     [USING RTREE|GEOHASH] [PRECISION <n>]
//! DROP [SPATIAL] INDEX <name>
//! ```
//!
//! The index registers in the catalog index registry, which is what makes the
//! drop statements above resolve it — the registry is the only record of a
//! spatial index's identity, since the R-tree itself is built per collection.
//!
//! `ON <collection> FIELDS <field>` is accepted as an equivalent spelling of
//! the parenthesized column. Parsing goes through the shared index-DDL grammar
//! so an unknown `USING` value, an out-of-range `PRECISION`, and any leftover
//! token are rejected rather than replaced by a default the statement never
//! asked for.
//!
//! The handler builds [`DdlResult`](super::super::result::DdlResult) directly
//! and carries no pgwire types.

use crate::control::security::catalog::IndexKind;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::index_registry::{
    IndexRegistration, index_name_holder, propose_index_record,
};
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::owner;
use super::super::result::{DdlError, DdlResult};
use super::dsl::options::{
    ColumnMode, HeaderSpec, NameMode, OptionSpec, closed_set, parse_index_statement,
};

const CONTEXT: &str = "CREATE SPATIAL INDEX";
const LEADING: &[&str] = &["CREATE", "SPATIAL", "INDEX"];

const SYNTAX: &str = "CREATE SPATIAL INDEX [IF NOT EXISTS] [<name>] ON <collection>(<field>) \
     [USING RTREE|GEOHASH] [PRECISION <1-12>]";

/// Substituted by the parser when the statement names no index.
const PLACEHOLDER_NAME: &str = "_auto_spatial";

const HEADER: HeaderSpec = HeaderSpec {
    name: NameMode::Optional {
        fallback: PLACEHOLDER_NAME,
    },
    columns: ColumnMode::ExactlyOne,
    syntax: SYNTAX,
};

const OPTIONS: &[OptionSpec] = &[OptionSpec::ident("USING"), OptionSpec::uint("PRECISION")];

const KNOWN_INDEX_TYPES: &[&str] = &["rtree", "geohash"];

/// Geohash cells are encoded in base-32 characters; twelve is the finest
/// resolution the encoder produces.
const MAX_GEOHASH_PRECISION: usize = 12;

/// Default geohash resolution — roughly a 1 km cell, the useful middle of the
/// range for the point-proximity queries this index serves.
const DEFAULT_GEOHASH_PRECISION: usize = 6;

/// `CREATE SPATIAL INDEX [IF NOT EXISTS] [<name>] ON <collection>(<field>)
///  [USING RTREE|GEOHASH] [PRECISION <n>]`
pub async fn create_spatial_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let stmt = parse_index_statement(sql, LEADING, &HEADER, OPTIONS, CONTEXT)?;

    let index_type = match stmt.options.text("USING") {
        Some(value) => closed_set(value, KNOWN_INDEX_TYPES, "index type", CONTEXT)?,
        None => "rtree".to_string(),
    };

    let precision = resolve_precision(&index_type, stmt.options.uint("PRECISION"))?;

    let index_name = &stmt.header.name;
    let collection = &stmt.header.collection;
    let field = stmt.header.column();
    let tenant_id = identity.tenant_id;

    // The parser substitutes a placeholder when the name is omitted; a
    // tenant-global placeholder will collide across collections and leave
    // only one of them droppable, so it resolves per collection and field.
    let index_name = if index_name == PLACEHOLDER_NAME {
        format!("{collection}_{field}_spatial_idx")
    } else {
        index_name.clone()
    };
    if let Some(taken) = index_name_holder(state, database_id, tenant_id, &index_name, CONTEXT)? {
        if stmt.header.if_not_exists && taken.kind == IndexKind::Spatial {
            return Ok(vec![DdlResult::Status {
                command: CONTEXT.to_string(),
                rows_affected: None,
            }]);
        }
        return Err(DdlError::new(
            "42710",
            format!(
                "{CONTEXT}: index '{index_name}' already exists on '{}' ({})",
                taken.collection,
                taken.kind.display_type()
            ),
        ));
    }

    propose_index_record(
        state,
        &IndexRegistration {
            database_id,
            tenant_id,
            name: &index_name,
            kind: IndexKind::Spatial,
            collection,
            fields: vec![field.to_string()],
            state: crate::control::security::catalog::IndexRecordState::Ready,
        },
    )
    .await?;
    owner::propose_owner(
        state,
        IndexKind::Spatial.owner_object_type(),
        database_id.as_u64(),
        tenant_id,
        &index_name,
        &identity.username,
    )
    .await?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!(
            "created spatial index '{index_name}' on '{collection}'({field}) \
             using {index_type}{}",
            match precision {
                Some(p) => format!(" precision {p}"),
                None => String::new(),
            }
        ),
    );

    Ok(vec![DdlResult::Status {
        command: CONTEXT.to_string(),
        rows_affected: None,
    }])
}

/// Resolve the geohash resolution, or reject a `PRECISION` that cannot apply.
///
/// The bound is a real limit of the encoder, so a value past it is a statement
/// that cannot be honoured — clamping it silently builds an index at a
/// resolution other than the one requested.
fn resolve_precision(
    index_type: &str,
    requested: Option<usize>,
) -> Result<Option<usize>, DdlError> {
    if index_type != "geohash" {
        return match requested {
            None => Ok(None),
            Some(_) => Err(DdlError::new(
                "42601",
                format!("{CONTEXT}: PRECISION applies only to USING GEOHASH"),
            )),
        };
    }

    match requested {
        None => Ok(Some(DEFAULT_GEOHASH_PRECISION)),
        Some(p) if (1..=MAX_GEOHASH_PRECISION).contains(&p) => Ok(Some(p)),
        Some(p) => Err(DdlError::new(
            "22023",
            format!("{CONTEXT}: PRECISION must be between 1 and {MAX_GEOHASH_PRECISION}, got {p}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::server::shared::ddl::neutral::dsl::options::IndexStatement;

    fn parse(sql: &str) -> Result<IndexStatement, DdlError> {
        parse_index_statement(sql, LEADING, &HEADER, OPTIONS, CONTEXT)
    }

    #[test]
    fn documented_fields_form_is_accepted() {
        let stmt = parse("CREATE SPATIAL INDEX ON restaurants FIELDS location").unwrap();
        assert_eq!(stmt.header.collection, "restaurants");
        assert_eq!(stmt.header.column(), "location");
        assert_eq!(stmt.header.name, PLACEHOLDER_NAME);
    }

    #[test]
    fn documented_unnamed_paren_form_is_accepted() {
        let stmt = parse("CREATE SPATIAL INDEX ON locations(geom) USING RTREE").unwrap();
        assert_eq!(stmt.header.collection, "locations");
        assert_eq!(stmt.header.column(), "geom");
        assert_eq!(stmt.options.text("USING"), Some("RTREE"));
    }

    #[test]
    fn named_form_still_parses() {
        let stmt = parse("CREATE SPATIAL INDEX idx_geo ON locations (geom)").unwrap();
        assert_eq!(stmt.header.name, "idx_geo");
    }

    #[test]
    fn unknown_index_type_is_rejected() {
        let opts = parse("CREATE SPATIAL INDEX ON l(geom) USING QUADTREE").unwrap();
        let err = closed_set(
            opts.options.text("USING").unwrap(),
            KNOWN_INDEX_TYPES,
            "index type",
            CONTEXT,
        )
        .unwrap_err();
        assert!(err.message.to_lowercase().contains("quadtree"));
    }

    #[test]
    fn non_numeric_precision_is_rejected() {
        assert!(parse("CREATE SPATIAL INDEX ON l(geom) USING GEOHASH PRECISION high").is_err());
    }

    #[test]
    fn unrecognized_trailing_tokens_are_rejected() {
        assert!(parse("CREATE SPATIAL INDEX ON l(geom) WITH (index = 'rtree')").is_err());
    }

    #[test]
    fn geohash_defaults_to_a_stated_precision() {
        assert_eq!(
            resolve_precision("geohash", None).unwrap(),
            Some(DEFAULT_GEOHASH_PRECISION)
        );
    }

    #[test]
    fn out_of_range_precision_is_rejected_not_clamped() {
        assert!(resolve_precision("geohash", Some(99)).is_err());
        assert!(resolve_precision("geohash", Some(0)).is_err());
        assert_eq!(resolve_precision("geohash", Some(12)).unwrap(), Some(12));
    }

    #[test]
    fn precision_on_an_rtree_is_rejected() {
        assert!(resolve_precision("rtree", Some(6)).is_err());
        assert_eq!(resolve_precision("rtree", None).unwrap(), None);
    }
}
