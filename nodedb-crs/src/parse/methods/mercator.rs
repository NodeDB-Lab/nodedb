// SPDX-License-Identifier: Apache-2.0

//! Mercator, transverse and oblique Mercator, and the other conformal
//! grids: Swiss, Laborde and the New Zealand Map Grid.

use super::params::{
    AZI, FE, FN, HOM_A_PARAMS, HOM_B_PARAMS, K_IL, K_NAT, K0_IL, K0_NAT, LAT_NAT, LAT_PC, LAT_TS_1,
    LON_NAT, LON_PC, NATURAL_ORIGIN,
};
use super::spec::{Angle, MethodSpec, fixed, method};

pub(super) const TMERC: MethodSpec = method(
    "Transverse Mercator",
    Some(9807),
    "tmerc",
    &[],
    &[LAT_NAT, LON_NAT, K_NAT, FE, FN],
);
pub(super) const HOM_A: MethodSpec = method(
    "Hotine Oblique Mercator (variant A)",
    Some(9812),
    "omerc",
    &["no_uoff"],
    HOM_A_PARAMS,
);
pub(super) const HOM_B: MethodSpec = method(
    "Hotine Oblique Mercator (variant B)",
    Some(9815),
    "omerc",
    &[],
    HOM_B_PARAMS,
);
pub(super) const MERC_A: MethodSpec = method(
    "Mercator (variant A)",
    Some(9804),
    "merc",
    &[],
    &[
        fixed("Latitude of natural origin", 8801, Angle, 0.0),
        LON_NAT,
        K0_NAT,
        FE,
        FN,
    ],
);
pub(super) const MERC_B: MethodSpec = method(
    "Mercator (variant B)",
    Some(9805),
    "merc",
    &[],
    &[LAT_TS_1, LON_NAT, FE, FN],
);
pub(super) const WEBMERC: MethodSpec = method(
    "Popular Visualisation Pseudo Mercator",
    Some(1024),
    "webmerc",
    &[],
    &[
        fixed("Latitude of natural origin", 8801, Angle, 0.0),
        LON_NAT,
        FE,
        FN,
    ],
);
pub(super) const SOMERC: MethodSpec = method(
    "Swiss Oblique Cylindrical",
    Some(9814),
    "somerc",
    &[],
    &[LAT_PC, LON_PC, FE, FN, K0_IL],
);
pub(super) const LABRD: MethodSpec = method(
    "Laborde Oblique Mercator",
    Some(9813),
    "labrd",
    &[],
    &[LAT_PC, LON_PC, AZI, K_IL, FE, FN],
);
pub(super) const NZMG: MethodSpec = method(
    "New Zealand Map Grid",
    Some(9811),
    "nzmg",
    &[],
    NATURAL_ORIGIN,
);

/// Methods readers accept and writers never pick: aliases of a writer
/// method with their own parameter sets.
pub(super) const READ_ONLY: &[MethodSpec] = &[
    method(
        "Transverse Mercator (South Orientated)",
        Some(9808),
        "tmerc",
        &[],
        &[LAT_NAT, LON_NAT, K_NAT, FE, FN],
    ),
    method(
        "Mercator (Spherical)",
        Some(1026),
        "merc",
        &[],
        &[
            fixed("Latitude of natural origin", 8801, Angle, 0.0),
            LON_NAT,
            FE,
            FN,
        ],
    ),
];
