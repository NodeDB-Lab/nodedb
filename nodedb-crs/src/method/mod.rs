// SPDX-License-Identifier: Apache-2.0

//! Map projection methods.

pub mod aea;
pub mod aeqd;
pub mod angle;
pub mod authalic;
pub mod cass;
pub mod cea;
pub mod conformal;
pub mod eqc;
pub mod eqearth;
pub mod geodesic;
pub mod geos;
pub mod krovak;
pub mod labrd;
pub mod laea;
pub mod lcc;
pub mod merc;
pub mod meridian;
pub mod nzmg;
pub mod omerc;
pub mod poly;
pub mod projection;
pub mod projector;
pub mod sinu;
pub mod somerc;
pub mod stere;
pub mod sterea;
pub mod tmerc;
pub mod webmerc;

pub use aea::{AeaParams, Albers};
pub use aeqd::{AeqdParams, AzimuthalEquidistant};
pub use cass::{CassParams, Cassini};
pub use cea::{CeaParams, CylindricalEqualArea};
pub use eqc::{EqcParams, EquidistantCylindrical};
pub use eqearth::{EqEarthParams, EqualEarth};
pub use geos::{GeosParams, GeostationarySatellite, SweepAxis};
pub use krovak::{Krovak, KrovakParams};
pub use labrd::{LabordeObliqueMercator, LabrdParams};
pub use laea::{LaeaParams, LambertAzimuthalEqualArea};
pub use lcc::{LambertConformalConic, LccParams};
pub use merc::{MercParams, Mercator};
pub use nzmg::{NewZealandMapGrid, NzmgParams};
pub use omerc::{HomParams, HomVariant, HotineObliqueMercator, alpha_from_gamma, gamma_from_alpha};
pub use poly::{PolyParams, Polyconic};
pub use projection::ProjectionDef;
pub use projector::Projector;
pub use sinu::{SinuParams, Sinusoidal};
pub use somerc::{SomercParams, SwissObliqueMercator};
pub use stere::{StereParams, Stereographic};
pub use sterea::{ObliqueStereographic, StereaParams};
pub use tmerc::{
    TAU_NEWTON_MAX_ITERS, TAU_TOLERANCE, TM_MAX_LON_OFFSET_DEG, TmParams, TransverseMercator,
};
pub use webmerc::{WebMercParams, WebMercator};
