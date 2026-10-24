// SPDX-License-Identifier: Apache-2.0

//! Conic methods: Lambert Conic Conformal, Albers, American Polyconic and
//! Krovak.

use super::params::{
    E_FO, K0_NAT, KROVAK_PARAMS, LAT_1, LAT_2, LAT_FO, LCC_1SP_PARAMS, LCC_2SP_PARAMS, LON_FO,
    N_FO, NATURAL_ORIGIN,
};
use super::spec::{Angle, MethodSpec, Scale, method, param};

pub(super) const LCC_2SP: MethodSpec = method(
    "Lambert Conic Conformal (2SP)",
    Some(9802),
    "lcc",
    &[],
    LCC_2SP_PARAMS,
);
pub(super) const AEA: MethodSpec = method(
    "Albers Equal Area",
    Some(9822),
    "aea",
    &[],
    &[LAT_FO, LON_FO, LAT_1, LAT_2, E_FO, N_FO],
);
pub(super) const POLY: MethodSpec = method(
    "American Polyconic",
    Some(9818),
    "poly",
    &[],
    NATURAL_ORIGIN,
);
pub(super) const KROVAK: MethodSpec =
    method("Krovak", Some(9819), "krovak", &["czech"], KROVAK_PARAMS);
pub(super) const KROVAK_NORTH: MethodSpec = method(
    "Krovak (North Orientated)",
    Some(1041),
    "krovak",
    &[],
    KROVAK_PARAMS,
);
pub(super) const KROVAK_MOD: MethodSpec = method(
    "Krovak Modified",
    Some(1042),
    "mod_krovak",
    &["czech"],
    KROVAK_PARAMS,
);
pub(super) const KROVAK_MOD_NORTH: MethodSpec = method(
    "Krovak Modified (North Orientated)",
    Some(1043),
    "mod_krovak",
    &[],
    KROVAK_PARAMS,
);

/// Methods readers accept and writers never pick: aliases of a writer
/// method with their own parameter sets.
pub(super) const READ_ONLY: &[MethodSpec] = &[
    method(
        "Lambert Conic Conformal (1SP)",
        Some(9801),
        "lcc",
        &[],
        LCC_1SP_PARAMS,
    ),
    method(
        "Lambert Conic Conformal (West Orientated)",
        Some(9826),
        "lcc",
        &[],
        LCC_1SP_PARAMS,
    ),
    method(
        "Lambert Conic Conformal (2SP Michigan)",
        Some(1051),
        "lcc",
        &[],
        &[
            LAT_FO,
            LON_FO,
            LAT_1,
            LAT_2,
            E_FO,
            N_FO,
            param("Ellipsoid scaling factor", 1038, Scale, &["k_0"]),
        ],
    ),
    method(
        "Lambert Conic Conformal (1SP variant B)",
        Some(1102),
        "lcc",
        &[],
        &[
            param("Latitude of natural origin", 8801, Angle, &["lat_1"]),
            K0_NAT,
            LAT_FO,
            LON_FO,
            E_FO,
            N_FO,
        ],
    ),
];
