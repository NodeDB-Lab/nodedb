// SPDX-License-Identifier: Apache-2.0

//! The 7-parameter Helmert transformation in its small-angle form, IOGP
//! Guidance Note 7-2 §4.2.3. This is the form PROJ `+towgs84` applies.
//!
//! [`Helmert::forward`] and [`Helmert::inverse`] apply the static
//! parameters. A time-dependent transformation, GN 7-2 §4.2.5, first takes
//! its parameters at the coordinate epoch with [`Helmert::at_epoch`].

use crate::def::Helmert;

/// Radians per arc-second, π / 648 000.
const RAD_PER_ARCSEC: f64 = core::f64::consts::PI / 648_000.0;

/// Scale per part per million.
const PER_PPM: f64 = 1e-6;

/// Cross product a × b.
fn cross(a: [f64; 3], b: [f64; 3]) -> [f64; 3] {
    [
        a[1] * b[2] - a[2] * b[1],
        a[2] * b[0] - a[0] * b[2],
        a[0] * b[1] - a[1] * b[0],
    ]
}

/// Dot product a · b.
fn dot(a: [f64; 3], b: [f64; 3]) -> f64 {
    a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
}

impl Helmert {
    /// Position-vector rotations in radians.
    fn rotation_rad(&self) -> [f64; 3] {
        self.position_vector_rotations().map(|r| r * RAD_PER_ARCSEC)
    }

    /// Translation vector in metres.
    fn translation_m(&self) -> [f64; 3] {
        [self.tx_m, self.ty_m, self.tz_m]
    }

    /// The static transformation this one equals at coordinate epoch
    /// `epoch`, decimal year: each parameter p becomes p + ṗ (t − t0),
    /// IOGP GN 7-2 §4.2.5. A static transformation returns itself.
    pub fn at_epoch(&self, epoch: f64) -> Helmert {
        let Some(rates) = self.rates else {
            return *self;
        };
        let dt = epoch - rates.reference_epoch;
        let r = rates.values();
        Helmert {
            tx_m: self.tx_m + r[0] * dt,
            ty_m: self.ty_m + r[1] * dt,
            tz_m: self.tz_m + r[2] * dt,
            rx_arcsec: self.rx_arcsec + r[3] * dt,
            ry_arcsec: self.ry_arcsec + r[4] * dt,
            rz_arcsec: self.rz_arcsec + r[5] * dt,
            scale_ppm: self.scale_ppm + r[6] * dt,
            convention: self.convention,
            accuracy_m: self.accuracy_m,
            rates: None,
        }
    }

    /// Map source ECEF metres to WGS 84 ECEF metres:
    /// V_T = (1 + dS) (I + K) V_S + T, where K V = r × V in the
    /// position-vector convention.
    pub fn forward(&self, v: [f64; 3]) -> [f64; 3] {
        let m = 1.0 + self.scale_ppm * PER_PPM;
        let rv = cross(self.rotation_rad(), v);
        let t = self.translation_m();
        [
            m * (v[0] + rv[0]) + t[0],
            m * (v[1] + rv[1]) + t[1],
            m * (v[2] + rv[2]) + t[2],
        ]
    }

    /// The exact inverse of [`Helmert::forward`]:
    /// V_S = (I + K)⁻¹ (V_T − T) / (1 + dS), with
    /// (I + K)⁻¹ = (I − K + r rᵀ) / (1 + |r|²).
    pub fn inverse(&self, v: [f64; 3]) -> [f64; 3] {
        let m = 1.0 + self.scale_ppm * PER_PPM;
        let t = self.translation_m();
        let u = [(v[0] - t[0]) / m, (v[1] - t[1]) / m, (v[2] - t[2]) / m];
        let r = self.rotation_rad();
        let ru = cross(r, u);
        let r_dot_u = dot(r, u);
        let det = 1.0 + dot(r, r);
        [
            (u[0] - ru[0] + r[0] * r_dot_u) / det,
            (u[1] - ru[1] + r[1] * r_dot_u) / det,
            (u[2] - ru[2] + r[2] * r_dot_u) / det,
        ]
    }
}

#[cfg(test)]
mod tests {
    use crate::def::{Helmert, HelmertRates, RotationConvention};

    /// IOGP GN 7-2 §4.2.3 worked example: WGS 72 → WGS 84, EPSG
    /// transformation 1238, position-vector convention.
    fn wgs72_to_wgs84() -> Helmert {
        Helmert::position_vector([0.0, 0.0, 4.5, 0.0, 0.0, 0.554, 0.219])
    }

    /// Input of the GN 7-2 example, WGS 72 geocentric metres.
    const GN_INPUT: [f64; 3] = [3_657_660.66, 255_768.55, 5_201_382.11];

    /// Half a centimetre: GN 7-2 states its results to 0.01 m.
    const GN_HALF_CM: f64 = 0.005;

    #[test]
    fn iogp_position_vector_example() {
        let out = wgs72_to_wgs84().forward(GN_INPUT);
        // GN 7-2 prints YT = 255 778.43 m and ZT = 5 201 387.75 m.
        assert!((out[1] - 255_778.43).abs() < GN_HALF_CM, "{out:?}");
        assert!((out[2] - 5_201_387.75).abs() < GN_HALF_CM, "{out:?}");
        // GN 7-2 prints XT = 3 657 660.78 m. Its own matrix formula gives
        // M (XS − rZ YS) = 3 657 660.774 m, which rounds to .77, so the
        // check uses the formula value to a millimetre.
        assert!((out[0] - 3_657_660.774_1).abs() < 1e-3, "{out:?}");
    }

    /// GN 7-2: the coordinate-frame parameters for the same example are the
    /// position-vector ones with the rotations negated.
    #[test]
    fn coordinate_frame_gives_the_same_result() {
        let pv = wgs72_to_wgs84();
        let mut cf = Helmert::position_vector([0.0, 0.0, 4.5, 0.0, 0.0, -0.554, 0.219]);
        cf.convention = RotationConvention::CoordinateFrame;
        assert_eq!(pv.forward(GN_INPUT), cf.forward(GN_INPUT));
    }

    /// IOGP GN 7-2 (September 2019) §4.2.5 worked example: EPSG
    /// transformation 6276, ITRF2008 to GDA94, Time-dependent Coordinate
    /// Frame rotation (method 1056), coordinate epoch 2013.90. Translations
    /// in metres, rotations in arc-seconds, scale in ppm, per year for the
    /// rates.
    fn itrf2008_to_gda94() -> Helmert {
        let mut h = Helmert::position_vector([
            -0.084_68,
            -0.019_42,
            0.032_01,
            -0.000_425_4,
            0.002_257_8,
            0.002_401_5,
            0.009_71,
        ]);
        h.convention = RotationConvention::CoordinateFrame;
        h.rates = Some(HelmertRates::new(
            [
                0.001_42,
                0.001_34,
                0.000_90,
                0.001_546_1,
                0.001_182_0,
                0.001_155_1,
                0.000_109,
            ],
            GN_TD_REFERENCE_EPOCH,
        ));
        h
    }

    /// Parameter reference epoch t0 of EPSG transformation 6276.
    const GN_TD_REFERENCE_EPOCH: f64 = 1994.00;

    /// Coordinate epoch t of the GN 7-2 §4.2.5 example.
    const GN_TD_EPOCH: f64 = 2013.90;

    /// ITRF2008 input of the GN 7-2 §4.2.5 example, metres.
    const GN_TD_INPUT: [f64; 3] = [-3_789_470.710, 4_841_770.404, -1_690_893.952];

    /// GDA94 output GN 7-2 §4.2.5 prints, metres.
    const GN_TD_OUTPUT: [f64; 3] = [-3_789_470.004, 4_841_770.686, -1_690_895.108];

    /// PROJ 9.8.1 `+proj=helmert` with the EPSG 6276 parameters,
    /// `+t_epoch=1994 +convention=coordinate_frame`, on GN_TD_INPUT at
    /// 2013.90.
    const PROJ_TD_OUTPUT: [f64; 3] = [
        -3_789_470.004_157_804,
        4_841_770.686_452_708,
        -1_690_895.108_020_519_6,
    ];

    /// Agreement with PROJ, which applies the same linear form.
    const PROJ_TD_TOLERANCE_M: f64 = 1e-6;

    #[test]
    fn iogp_time_dependent_coordinate_frame_example() {
        let out = itrf2008_to_gda94()
            .at_epoch(GN_TD_EPOCH)
            .forward(GN_TD_INPUT);
        for i in 0..3 {
            assert!((out[i] - GN_TD_OUTPUT[i]).abs() < GN_HALF_CM, "{out:?}");
            assert!(
                (out[i] - PROJ_TD_OUTPUT[i]).abs() < PROJ_TD_TOLERANCE_M,
                "{out:?}"
            );
        }
        // The reverse direction returns the GN 7-2 input.
        let back = itrf2008_to_gda94()
            .at_epoch(GN_TD_EPOCH)
            .inverse(GN_TD_OUTPUT);
        for i in 0..3 {
            assert!((back[i] - GN_TD_INPUT[i]).abs() < GN_HALF_CM, "{back:?}");
        }
    }

    /// Method 1053 is method 1056 with every rotation and rotation rate
    /// negated, GN 7-2 §4.2.5.
    #[test]
    fn time_dependent_position_vector_matches_coordinate_frame() {
        let cf = itrf2008_to_gda94();
        let mut pv = cf;
        pv.convention = RotationConvention::PositionVector;
        pv.rx_arcsec = -cf.rx_arcsec;
        pv.ry_arcsec = -cf.ry_arcsec;
        pv.rz_arcsec = -cf.rz_arcsec;
        let mut rates = cf.rates.unwrap();
        rates.drx_arcsec_per_yr = -rates.drx_arcsec_per_yr;
        rates.dry_arcsec_per_yr = -rates.dry_arcsec_per_yr;
        rates.drz_arcsec_per_yr = -rates.drz_arcsec_per_yr;
        pv.rates = Some(rates);
        let a = cf.at_epoch(GN_TD_EPOCH).forward(GN_TD_INPUT);
        let b = pv.at_epoch(GN_TD_EPOCH).forward(GN_TD_INPUT);
        for i in 0..3 {
            assert!((a[i] - b[i]).abs() < 1e-9, "{a:?} {b:?}");
        }
    }

    /// At the reference epoch the rates contribute nothing.
    #[test]
    fn reference_epoch_applies_the_static_parameters() {
        let h = itrf2008_to_gda94();
        let mut fixed = h;
        fixed.rates = None;
        assert_eq!(
            h.at_epoch(GN_TD_REFERENCE_EPOCH).forward(GN_TD_INPUT),
            fixed.forward(GN_TD_INPUT)
        );
    }

    #[test]
    fn inverse_round_trip_below_a_micrometre() {
        let h = Helmert::position_vector([-11.0, 851.0, 5.0, 2.5, -1.75, 3.2, 12.0]);
        for v in [
            GN_INPUT,
            [-2_000_000.0, 5_500_000.0, -2_500_000.0],
            [6_378_137.0, 0.0, 0.0],
        ] {
            let back = h.inverse(h.forward(v));
            for (got, want) in back.iter().zip(v) {
                assert!((got - want).abs() < 1e-6, "{v:?} -> {back:?}");
            }
        }
    }
}
