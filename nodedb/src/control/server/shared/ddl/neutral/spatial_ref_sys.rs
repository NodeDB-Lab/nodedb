// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `CREATE SPATIAL_REF_SYS` and `DROP SPATIAL_REF_SYS`.
//!
//! A user-defined CRS belongs to the whole cluster, so both statements need
//! a superuser, as other catalog-wide DDL does. Codes come from the user
//! range only. A dropped code stays a tombstone and is never reused.
//!
//! Each handler checks the statement against the catalog's set, proposes
//! the entry, then reads the set back: two statements on two nodes can race
//! to one code, and only the one the log orders first defines it.
//!
//! The statement returns once every core on this node holds the new set:
//! the metadata applier awaits the install before the entry counts as
//! applied. A replicated write carries the metadata index its proposer
//! applied, and a replica holds the write until its own metadata apply
//! reaches that index. So every core that evaluates a write planned after
//! the statement already holds the set the statement made.

use nodedb_crs::{
    CrsDef, CrsParseError, CustomCrsError, CustomCrsSet, CustomCrsState, Srid, parse_proj_string,
    parse_projjson, parse_wkt2,
};
use nodedb_sql::ddl_ast::CrsTextFormat;

use crate::control::catalog_entry::CatalogEntry;
use crate::control::propose_outcome::ProposeOutcome;
use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::result::{DdlError, DdlResult};

/// SQLSTATE of a code outside the user range or a definition that does not
/// parse.
const INVALID_PARAMETER: &str = "22023";

/// SQLSTATE of a code that is live or dropped.
const DUPLICATE_OBJECT: &str = "42710";

/// SQLSTATE of a drop of a code that is not live.
const UNDEFINED_OBJECT: &str = "42704";

/// SQLSTATE of a drop of a code that a catalog definition names.
const DEPENDENT_OBJECTS_STILL_EXIST: &str = "2BP01";

/// Handle `CREATE SPATIAL_REF_SYS <code> (NAME '..', PROJ|PROJJSON|WKT2 '..')`.
pub async fn create_spatial_ref_sys(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    code: u64,
    name: &str,
    format: CrsTextFormat,
    definition: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    const COMMAND: &str = "CREATE SPATIAL_REF_SYS";
    require_superuser(state, identity, COMMAND)?;
    let srid = srid_of(code)?;
    let def = parse_definition(srid, name.trim(), format, definition)?;
    let mut probe = CustomCrsSet::clone(&state.credentials.catalog().custom_crs_set());
    probe.define(def.clone()).map_err(set_error)?;

    let entry = CatalogEntry::PutSpatialRefSys(Box::new(def.clone()));
    let outcome = crate::control::metadata_proposer::propose_catalog_entry_async(state, &entry)
        .await
        .map_err(|e| DdlError::from_error_in_context("metadata propose", &e))?;
    if let ProposeOutcome::Replicated { .. } = outcome {
        match state.credentials.catalog().custom_crs_set().get(srid) {
            Some(CustomCrsState::Live(live)) if **live == def => {}
            Some(CustomCrsState::Live(_)) => {
                return Err(set_error(CustomCrsError::Exists { srid }));
            }
            Some(CustomCrsState::Dropped) => {
                return Err(set_error(CustomCrsError::WasDropped { srid }));
            }
            None => {
                return Err(DdlError::internal(format!(
                    "{COMMAND} {srid}: the committed entry did not define the code"
                )));
            }
        }
    }
    Ok(status(COMMAND))
}

/// Handle `DROP SPATIAL_REF_SYS <code>`.
///
/// A code that a catalog definition names as an SRID stays live: an SRID
/// argument or geometry literal in a generated column, CHECK, type guard or
/// other evaluated expression, a geometry type, or an array GEOREF. The
/// same number in another role, such as `amount < 910001`, names nothing.
/// The handler refuses it before proposing, and the apply refuses it again
/// when such a definition committed in between. Once the drop commits, no
/// definition names the code, so a write or a replayed record never
/// resolves it through a definition. A stored geometry that carries the
/// code keeps it, and a read of it reports the code as dropped.
pub async fn drop_spatial_ref_sys(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    code: u64,
) -> Result<Vec<DdlResult>, DdlError> {
    const COMMAND: &str = "DROP SPATIAL_REF_SYS";
    require_superuser(state, identity, COMMAND)?;
    let srid = srid_of(code)?;
    let catalog = state.credentials.catalog();
    let mut probe = CustomCrsSet::clone(&catalog.custom_crs_set());
    probe.drop_crs(srid).map_err(set_error)?;
    refuse_if_named(catalog, srid)?;

    let entry = CatalogEntry::DropSpatialRefSys { srid };
    let outcome = crate::control::metadata_proposer::propose_catalog_entry_async(state, &entry)
        .await
        .map_err(|e| DdlError::from_error_in_context("metadata propose", &e))?;
    // A racing drop of the same code leaves it dropped as well, which is
    // the outcome this statement asked for. A code still live means the
    // apply found a definition that names it.
    if let ProposeOutcome::Replicated { .. } = outcome
        && let Some(CustomCrsState::Live(_)) = catalog.custom_crs_set().get(srid)
    {
        refuse_if_named(catalog, srid)?;
        return Err(DdlError::internal(format!(
            "{COMMAND} {srid}: the committed entry left the code live"
        )));
    }
    Ok(status(COMMAND))
}

/// Refuse a drop of `srid` while a catalog definition names it. The error
/// lists every such definition.
fn refuse_if_named(
    catalog: &crate::control::security::catalog::SystemCatalog,
    srid: Srid,
) -> Result<(), DdlError> {
    let referrers = catalog
        .srid_referrers(srid)
        .map_err(|e| DdlError::from_error_in_context("spatial_ref_sys dependency scan", &e))?;
    if referrers.is_empty() {
        return Ok(());
    }
    Err(DdlError::new(
        DEPENDENT_OBJECTS_STILL_EXIST,
        dependents_message(srid, &referrers),
    ))
}

/// The message of a drop refused for the definitions in `referrers`.
fn dependents_message(srid: Srid, referrers: &[String]) -> String {
    format!(
        "cannot drop SRID {srid}: {} catalog definition(s) name it: {}; \
         alter or drop them first",
        referrers.len(),
        referrers.join("; ")
    )
}

fn status(command: &str) -> Vec<DdlResult> {
    vec![DdlResult::Status {
        command: command.to_string(),
        rows_affected: None,
    }]
}

/// `code` as an SRID in the user range.
fn srid_of(code: u64) -> Result<Srid, DdlError> {
    Srid::new_user(code).map_err(|e| set_error(CustomCrsError::from(e)))
}

/// Parse the definition text in `format`.
fn parse_definition(
    srid: Srid,
    name: &str,
    format: CrsTextFormat,
    text: &str,
) -> Result<CrsDef, DdlError> {
    let (label, parsed) = match format {
        CrsTextFormat::Proj => ("PROJ", parse_proj_string(srid, name, text)),
        CrsTextFormat::ProjJson => ("PROJJSON", parse_projjson(srid, name, text)),
        CrsTextFormat::Wkt2 => ("WKT2", parse_wkt2(srid, name, text)),
    };
    parsed.map_err(|e: CrsParseError| {
        DdlError::new(
            INVALID_PARAMETER,
            format!("SRID {srid}: invalid {label} definition: {e}"),
        )
    })
}

/// The DDL error a refused set change reports.
fn set_error(error: CustomCrsError) -> DdlError {
    let sqlstate = match error {
        CustomCrsError::NotUserCode(_) => INVALID_PARAMETER,
        CustomCrsError::Exists { .. } | CustomCrsError::WasDropped { .. } => DUPLICATE_OBJECT,
        CustomCrsError::NotFound { .. } => UNDEFINED_OBJECT,
    };
    DdlError::new(sqlstate, error.to_string())
}

/// Superuser gate: on denial it emits `AuditEvent::PermissionDenied` and
/// returns SQLSTATE 42501.
fn require_superuser(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    action: &str,
) -> Result<(), DdlError> {
    if identity.is_superuser {
        return Ok(());
    }
    state.audit_record_with_db(
        AuditEvent::PermissionDenied,
        Some(identity.tenant_id),
        None,
        &identity.username,
        action,
    );
    Err(DdlError::new(
        "42501",
        format!("permission denied: {action} requires superuser"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_outside_the_user_range_are_refused() {
        assert!(srid_of(910_000).is_ok());
        assert!(srid_of(989_999).is_ok());
        for code in [0, 4326, 909_999, 990_001, 999_999, u64::from(u32::MAX) + 1] {
            let err = srid_of(code).unwrap_err();
            assert_eq!(err.sqlstate, INVALID_PARAMETER, "{code}");
        }
    }

    #[test]
    fn definitions_parse_in_each_format() {
        let srid = Srid::new(910_100).unwrap();
        let def =
            parse_definition(srid, "n", CrsTextFormat::Proj, "+proj=longlat +datum=WGS84").unwrap();
        let wkt = def.to_wkt2();
        let json = def.to_projjson();
        assert_eq!(
            parse_definition(srid, "n", CrsTextFormat::Wkt2, &wkt).unwrap(),
            def
        );
        assert_eq!(
            parse_definition(srid, "n", CrsTextFormat::ProjJson, &json).unwrap(),
            def
        );
        let err = parse_definition(srid, "n", CrsTextFormat::Proj, "+proj=bonne").unwrap_err();
        assert_eq!(err.sqlstate, INVALID_PARAMETER);
    }

    #[test]
    fn a_refused_drop_names_every_dependent() {
        let srid = Srid::new(910_100).unwrap();
        let message = dependents_message(
            srid,
            &[
                "array 'scene' (tenant 1, database 0): GEOREF".to_string(),
                "collection 'parcels' (tenant 1, database 0): CHECK constraint 'z'".to_string(),
            ],
        );
        assert!(
            message.starts_with("cannot drop SRID 910100: 2 catalog"),
            "{message}"
        );
        assert!(message.contains("array 'scene'"), "{message}");
        assert!(message.contains("CHECK constraint 'z'"), "{message}");
    }

    #[test]
    fn refused_changes_map_to_their_sqlstate() {
        let srid = Srid::new(910_100).unwrap();
        assert_eq!(
            set_error(CustomCrsError::WasDropped { srid }).sqlstate,
            DUPLICATE_OBJECT
        );
        assert_eq!(
            set_error(CustomCrsError::NotFound { srid }).sqlstate,
            UNDEFINED_OBJECT
        );
    }
}
