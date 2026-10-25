// SPDX-License-Identifier: Apache-2.0

//! EPSG parameters and parameter sets the method families share.

use super::spec::{Angle, Length, ParamSpec, Scale, Target, fixed, param};

pub(super) const LAT_NAT: ParamSpec = param("Latitude of natural origin", 8801, Angle, &["lat_0"]);
pub(super) const LON_NAT: ParamSpec = param("Longitude of natural origin", 8802, Angle, &["lon_0"]);
pub(super) const K_NAT: ParamSpec = param("Scale factor at natural origin", 8805, Scale, &["k"]);
pub(super) const K0_NAT: ParamSpec = param("Scale factor at natural origin", 8805, Scale, &["k_0"]);
pub(super) const FE: ParamSpec = param("False easting", 8806, Length, &["x_0"]);
pub(super) const FN: ParamSpec = param("False northing", 8807, Length, &["y_0"]);
pub(super) const LAT_FO: ParamSpec = param("Latitude of false origin", 8821, Angle, &["lat_0"]);
pub(super) const LON_FO: ParamSpec = param("Longitude of false origin", 8822, Angle, &["lon_0"]);
pub(super) const LAT_1: ParamSpec =
    param("Latitude of 1st standard parallel", 8823, Angle, &["lat_1"]);
pub(super) const LAT_2: ParamSpec =
    param("Latitude of 2nd standard parallel", 8824, Angle, &["lat_2"]);
pub(super) const LAT_TS_1: ParamSpec = param(
    "Latitude of 1st standard parallel",
    8823,
    Angle,
    &["lat_ts"],
);
pub(super) const E_FO: ParamSpec = param("Easting at false origin", 8826, Length, &["x_0"]);
pub(super) const N_FO: ParamSpec = param("Northing at false origin", 8827, Length, &["y_0"]);
pub(super) const LAT_PC: ParamSpec =
    param("Latitude of projection centre", 8811, Angle, &["lat_0"]);
pub(super) const LONC_PC: ParamSpec =
    param("Longitude of projection centre", 8812, Angle, &["lonc"]);
pub(super) const LON_PC: ParamSpec =
    param("Longitude of projection centre", 8812, Angle, &["lon_0"]);
pub(super) const ALPHA: ParamSpec = param("Azimuth of initial line", 8813, Angle, &["alpha"]);
pub(super) const AZI: ParamSpec = param("Azimuth of initial line", 8813, Angle, &["azi"]);
pub(super) const GAMMA: ParamSpec =
    param("Angle from Rectified to Skew Grid", 8814, Angle, &["gamma"]);
pub(super) const K_IL: ParamSpec = param("Scale factor on initial line", 8815, Scale, &["k"]);
pub(super) const K0_IL: ParamSpec = param("Scale factor on initial line", 8815, Scale, &["k_0"]);
pub(super) const E_PC: ParamSpec = param("Easting at projection centre", 8816, Length, &["x_0"]);
pub(super) const N_PC: ParamSpec = param("Northing at projection centre", 8817, Length, &["y_0"]);
pub(super) const LAT_STD: ParamSpec =
    param("Latitude of standard parallel", 8832, Angle, &["lat_ts"]);
pub(super) const LON_ORIG: ParamSpec = param("Longitude of origin", 8833, Angle, &["lon_0"]);
pub(super) const SAT_HEIGHT: ParamSpec = ParamSpec {
    name: "Satellite Height",
    code: None,
    kind: Length,
    target: Target::Keys(&["h"]),
};

pub(super) const NATURAL_ORIGIN: &[ParamSpec] = &[LAT_NAT, LON_NAT, FE, FN];
pub(super) const LONGITUDE_ONLY: &[ParamSpec] = &[LON_NAT, FE, FN];
pub(super) const HOM_A_PARAMS: &[ParamSpec] = &[LAT_PC, LONC_PC, ALPHA, GAMMA, K_IL, FE, FN];
pub(super) const HOM_B_PARAMS: &[ParamSpec] = &[LAT_PC, LONC_PC, ALPHA, GAMMA, K_IL, E_PC, N_PC];
pub(super) const LCC_2SP_PARAMS: &[ParamSpec] = &[LAT_FO, LON_FO, LAT_1, LAT_2, E_FO, N_FO, K0_NAT];
pub(super) const LCC_1SP_PARAMS: &[ParamSpec] = &[
    param(
        "Latitude of natural origin",
        8801,
        Angle,
        &["lat_1", "lat_0"],
    ),
    LON_NAT,
    K0_NAT,
    FE,
    FN,
];
pub(super) const KROVAK_PARAMS: &[ParamSpec] = &[
    LAT_PC,
    LON_ORIG,
    param("Co-latitude of cone axis", 1036, Angle, &["alpha"]),
    fixed("Latitude of pseudo standard parallel", 8818, Angle, 78.5),
    param(
        "Scale factor on pseudo standard parallel",
        8819,
        Scale,
        &["k"],
    ),
    FE,
    FN,
];
pub(super) const GEOS_PARAMS: &[ParamSpec] = &[SAT_HEIGHT, LON_NAT, FE, FN];
