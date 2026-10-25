// SPDX-License-Identifier: Apache-2.0

//! Equal-area and equidistant cylindrical methods, Cassini-Soldner and the
//! pseudocylindrical Sinusoidal and Equal Earth.

use super::params::{FE, FN, K0_NAT, LAT_NAT, LAT_TS_1, LON_NAT, LONGITUDE_ONLY, NATURAL_ORIGIN};
use super::spec::{MethodSpec, method};

pub(super) const CEA: MethodSpec = method(
    "Lambert Cylindrical Equal Area",
    Some(9835),
    "cea",
    &[],
    &[LAT_TS_1, LON_NAT, FE, FN, K0_NAT],
);
pub(super) const EQC: MethodSpec = method(
    "Equidistant Cylindrical",
    Some(1028),
    "eqc",
    &[],
    &[LAT_TS_1, LAT_NAT, LON_NAT, FE, FN],
);
pub(super) const CASS: MethodSpec =
    method("Cassini-Soldner", Some(9806), "cass", &[], NATURAL_ORIGIN);
pub(super) const HYPERBOLIC_CASS: MethodSpec = method(
    "Hyperbolic Cassini-Soldner",
    Some(9833),
    "cass",
    &["hyperbolic"],
    NATURAL_ORIGIN,
);
pub(super) const SINU: MethodSpec = method("Sinusoidal", None, "sinu", &[], LONGITUDE_ONLY);
pub(super) const EQEARTH: MethodSpec =
    method("Equal Earth", Some(1078), "eqearth", &[], LONGITUDE_ONLY);

/// Methods readers accept and writers never pick: aliases of a writer
/// method with their own parameter sets.
pub(super) const READ_ONLY: &[MethodSpec] = &[
    method(
        "Lambert Cylindrical Equal Area (Spherical)",
        Some(9834),
        "cea",
        &[],
        &[LAT_TS_1, LON_NAT, FE, FN],
    ),
    method(
        "Equidistant Cylindrical (Spherical)",
        Some(1029),
        "eqc",
        &[],
        &[LAT_TS_1, LAT_NAT, LON_NAT, FE, FN],
    ),
];
