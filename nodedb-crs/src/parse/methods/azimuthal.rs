// SPDX-License-Identifier: Apache-2.0

//! Azimuthal and perspective methods: stereographic, Lambert azimuthal
//! equal area, azimuthal equidistant and geostationary satellite.

use super::params::{
    FE, FN, GEOS_PARAMS, K0_NAT, LAT_NAT, LAT_STD, LON_NAT, LON_ORIG, NATURAL_ORIGIN,
};
use super::spec::{Derive, MethodSpec, method};

pub(super) const POLAR_A: MethodSpec = method(
    "Polar Stereographic (variant A)",
    Some(9810),
    "stere",
    &[],
    &[LAT_NAT, LON_NAT, K0_NAT, FE, FN],
);
pub(super) const POLAR_B: MethodSpec = MethodSpec {
    name: "Polar Stereographic (variant B)",
    code: Some(9829),
    proj: "stere",
    flags: &[],
    params: &[LAT_STD, LON_ORIG, FE, FN, LAT_NAT, K0_NAT],
    derive: Derive::PolarLatitudeFromStandardParallel,
};
pub(super) const STERE: MethodSpec = method(
    "Stereographic",
    None,
    "stere",
    &[],
    &[LAT_NAT, LON_NAT, K0_NAT, FE, FN, LAT_STD],
);
pub(super) const STEREA: MethodSpec = method(
    "Oblique Stereographic",
    Some(9809),
    "sterea",
    &[],
    &[LAT_NAT, LON_NAT, K0_NAT, FE, FN],
);
pub(super) const LAEA: MethodSpec = method(
    "Lambert Azimuthal Equal Area",
    Some(9820),
    "laea",
    &[],
    NATURAL_ORIGIN,
);
pub(super) const AEQD: MethodSpec = method(
    "Azimuthal Equidistant",
    Some(1125),
    "aeqd",
    &[],
    NATURAL_ORIGIN,
);
pub(super) const GUAM: MethodSpec = method(
    "Guam Projection",
    Some(9831),
    "aeqd",
    &["guam"],
    NATURAL_ORIGIN,
);
pub(super) const GEOS_Y: MethodSpec = method(
    "Geostationary Satellite (Sweep Y)",
    None,
    "geos",
    &[],
    GEOS_PARAMS,
);
pub(super) const GEOS_X: MethodSpec = method(
    "Geostationary Satellite (Sweep X)",
    None,
    "geos",
    &["sweep=x"],
    GEOS_PARAMS,
);

/// Methods readers accept and writers never pick: aliases of a writer
/// method with their own parameter sets.
pub(super) const READ_ONLY: &[MethodSpec] = &[
    method(
        "Lambert Azimuthal Equal Area (Spherical)",
        Some(1027),
        "laea",
        &[],
        NATURAL_ORIGIN,
    ),
    method(
        "Modified Azimuthal Equidistant",
        Some(9832),
        "aeqd",
        &[],
        NATURAL_ORIGIN,
    ),
];
