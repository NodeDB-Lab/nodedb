// SPDX-License-Identifier: Apache-2.0

//! Rates of change of the seven Helmert parameters: the time-dependent
//! (15-parameter) Helmert transformation, IOGP Guidance Note 7-2 §4.2.5,
//! EPSG methods 1053 to 1058.

use serde::{Deserialize, Serialize};

/// Yearly rates of the seven Helmert parameters and the epoch the static
/// parameters hold at.
///
/// At coordinate epoch t each parameter p takes the value
/// p + ṗ (t − t0), where t0 is `reference_epoch`.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct HelmertRates {
    pub dtx_m_per_yr: f64,
    pub dty_m_per_yr: f64,
    pub dtz_m_per_yr: f64,
    pub drx_arcsec_per_yr: f64,
    pub dry_arcsec_per_yr: f64,
    pub drz_arcsec_per_yr: f64,
    pub dscale_ppm_per_yr: f64,
    /// Parameter reference epoch t0, decimal year.
    pub reference_epoch: f64,
}

impl HelmertRates {
    /// Rates from the seven values in `+towgs84` order: tx, ty, tz metres
    /// per year, rx, ry, rz arc-seconds per year, scale ppm per year.
    pub const fn new(rates: [f64; 7], reference_epoch: f64) -> HelmertRates {
        HelmertRates {
            dtx_m_per_yr: rates[0],
            dty_m_per_yr: rates[1],
            dtz_m_per_yr: rates[2],
            drx_arcsec_per_yr: rates[3],
            dry_arcsec_per_yr: rates[4],
            drz_arcsec_per_yr: rates[5],
            dscale_ppm_per_yr: rates[6],
            reference_epoch,
        }
    }

    /// The seven rates in `+towgs84` order, in the stored rotation
    /// convention.
    pub fn values(&self) -> [f64; 7] {
        [
            self.dtx_m_per_yr,
            self.dty_m_per_yr,
            self.dtz_m_per_yr,
            self.drx_arcsec_per_yr,
            self.dry_arcsec_per_yr,
            self.drz_arcsec_per_yr,
            self.dscale_ppm_per_yr,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_follow_towgs84_order() {
        let r = HelmertRates::new([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0], 2010.0);
        assert_eq!(r.values(), [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]);
        assert_eq!(r.reference_epoch, 2010.0);
    }
}
