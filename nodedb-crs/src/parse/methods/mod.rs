// SPDX-License-Identifier: Apache-2.0

//! EPSG projection methods and their parameters, mapped to PROJ keys.
//!
//! WKT2 and PROJJSON name a projection by its EPSG method and parameters.
//! Each [`MethodSpec`] maps one method to the PROJ `+proj` value and flags
//! the PROJ-string parser reads, and each [`ParamSpec`](spec::ParamSpec) maps one parameter
//! to the PROJ keys it sets. Readers look a method up by EPSG code, then by
//! name. Writers pick one method per projection shape. Each method family
//! has its own table file.

pub(super) mod azimuthal;
pub(super) mod conic;
pub(super) mod cylindrical;
pub(super) mod lookup;
pub(super) mod mercator;
pub(super) mod params;
pub(super) mod spec;

pub(crate) use lookup::{method_by, writer_spec};
pub(crate) use spec::{Derive, FLAG_KEYS, MethodSpec, ParamKind, Target};
