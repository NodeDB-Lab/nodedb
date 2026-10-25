// SPDX-License-Identifier: BUSL-1.1

//! `nodedb.spatial_ref_sys` row producer: every built-in CRS, then every
//! live user-defined CRS.
//!
//! Columns follow PostGIS `spatial_ref_sys`, plus `name`, `kind` and the
//! accuracy of the datum shift to WGS 84. `srtext` is WKT2:2019 and
//! `proj4text` a PROJ string. `proj4text` is NULL for a CRS no PROJ string
//! expresses, such as one with a time-dependent datum shift, as PostGIS
//! leaves it empty for such a CRS. A dropped user-defined code is not
//! listed: it resolves to nothing.

use std::collections::HashMap;

use nodedb_crs::{CrsDef, CustomCrsState, DatumShift, NODEDB_AUTHORITY, Srid, SridAuthority};
use nodedb_types::Value;

use crate::control::state::SharedState;

use super::tables::collections::encode_row;

pub fn spatial_ref_sys(state: &SharedState) -> crate::Result<Vec<Vec<u8>>> {
    let mut rows = Vec::new();
    for entry in nodedb_crs::builtin_entries() {
        let srid = Srid::new(entry.code).map_err(|e| crate::Error::Internal {
            detail: format!("built-in CRS table: {e}"),
        })?;
        let def = entry.definition(srid).map_err(|e| crate::Error::Internal {
            detail: format!("built-in CRS {srid}: {e}"),
        })?;
        rows.push(encode_row(row(&def, Some(entry.proj.to_string())))?);
    }
    let custom = state.credentials.catalog().custom_crs_set();
    for entry in custom.entries() {
        match &entry.state {
            CustomCrsState::Live(def) => {
                // The only export error is a time-dependent shift, which no
                // PROJ string carries: the column is NULL, as in PostGIS.
                let proj4text = def.to_proj_string().ok();
                rows.push(encode_row(row(def, proj4text))?);
            }
            CustomCrsState::Dropped => {}
        }
    }
    Ok(rows)
}

/// One row: `def` with `proj4text` as the PROJ text to show, NULL when
/// `None`.
fn row(def: &CrsDef, proj4text: Option<String>) -> HashMap<String, Value> {
    let srid = def.srid;
    let auth_name = match srid.authority() {
        SridAuthority::Epsg => "EPSG",
        SridAuthority::NodeDb | SridAuthority::User | SridAuthority::Reserved => NODEDB_AUTHORITY,
    };
    let accuracy = match &def.datum.to_wgs84 {
        DatumShift::Helmert(h) => h.accuracy_m.map_or(Value::Null, Value::Float),
        DatumShift::Identity => Value::Null,
    };
    let mut r = HashMap::with_capacity(8);
    r.insert("srid".into(), Value::Integer(i64::from(srid.code())));
    r.insert("auth_name".into(), Value::String(auth_name.into()));
    r.insert("auth_srid".into(), Value::Integer(i64::from(srid.code())));
    r.insert("name".into(), Value::String(def.name.to_string()));
    r.insert("kind".into(), Value::String(def.kind.as_str().into()));
    r.insert(
        "proj4text".into(),
        proj4text.map_or(Value::Null, Value::String),
    );
    r.insert("srtext".into(), Value::String(def.to_wkt2()));
    r.insert("datum_shift_accuracy_m".into(), accuracy);
    r
}
