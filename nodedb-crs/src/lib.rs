// SPDX-License-Identifier: Apache-2.0

//! Coordinate reference systems for NodeDB.
//!
//! An [`Srid`] names a CRS. A [`CrsContext`] resolves it to a [`CrsDef`]:
//! user-defined CRSs first, then the built-in EPSG table. A [`Transformer`]
//! maps coordinates between two definitions, at a [`CoordinateEpoch`] when a
//! datum shift is time-dependent. A definition reads from and
//! writes to PROJ strings, WKT2 and PROJJSON.
//!
//! The crate does no file or network I/O, so it builds for
//! `wasm32-unknown-unknown`.

pub mod datum;
pub mod def;
pub mod error;
pub mod geo_transform;
pub mod method;
pub mod parse;
pub mod registry;
pub mod transform;

pub use nodedb_srid::srid;

pub use def::{
    AreaOfUse, AxisDirection, AxisOrder, CoordinateEpoch, CrsDef, CrsKind, Datum, DatumShift,
    Ellipsoid, Helmert, HelmertRates, LinearUnit, MAX_COORDINATE_EPOCH, MIN_COORDINATE_EPOCH,
    RotationConvention,
};
pub use error::{
    CrsError, CrsParseError, CustomCrsError, DomainReason, InvalidEpoch, ProjExportError,
    UnknownSrid,
};
pub use geo_transform::GeoTransform;
pub use method::{
    AeaParams, AeqdParams, CassParams, CeaParams, EqEarthParams, EqcParams, GeosParams, HomParams,
    HomVariant, KrovakParams, LabrdParams, LaeaParams, LccParams, MercParams, NzmgParams,
    PolyParams, ProjectionDef, Projector, SinuParams, SomercParams, StereParams, StereaParams,
    SweepAxis, TmParams, WebMercParams,
};
pub use nodedb_srid::{
    InvalidSridCode, NODEDB_AUTHORITY, NotUserSrid, Srid, SridAuthority, SridResolver,
    code_from_name,
};
pub use parse::{parse_proj_string, parse_projjson, parse_wkt2};
pub use registry::{
    BuiltinEntry, CrsContext, CustomCrs, CustomCrsSet, CustomCrsState, DuplicateCustomSrid,
    builtin_codes, builtin_entries, builtin_lookup, geographic_crs_on_datum,
};
pub use transform::Transformer;
