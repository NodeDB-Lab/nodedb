// SPDX-License-Identifier: Apache-2.0

//! PROJ-string writer.
//!
//! Numbers print in Rust's shortest round-trip form, so
//! [`parse_proj_string`](super::parse_proj_string) reads back the same
//! values. PROJ strings carry neither the Helmert rotation convention nor
//! its accuracy: the writer emits position-vector rotations, and the
//! accuracy and area of use are dropped.
//!
//! `+towgs84` has no rates, so a definition with a time-dependent Helmert
//! has no PROJ string: the writer refuses it with
//! [`ProjExportError::TimeDependentShift`], as PROJ refuses the same export.
//! WKT2 and PROJJSON keep the rates and the epoch.

use core::fmt::Write;

use crate::def::{CrsDef, CrsKind, DatumShift, Ellipsoid, LinearUnit};
use crate::error::ProjExportError;

impl CrsDef {
    /// This definition as a PROJ string. The EPSG datum code is dropped:
    /// PROJ strings do not carry it. A time-dependent datum shift fails with
    /// [`ProjExportError::TimeDependentShift`].
    pub fn to_proj_string(&self) -> Result<String, ProjExportError> {
        // Every parameter is written with a leading space, the first one's
        // stripped at the end.
        let mut out = String::new();
        match &self.projection {
            None => out.push_str(" +proj=longlat"),
            Some(p) => p.write_proj(&mut out),
        }
        if !self.axis.is_enu() {
            push(&mut out, format_args!(" +axis={}", self.axis));
        }
        write_ellipsoid(&mut out, &self.datum.ellipsoid);
        if self.datum.prime_meridian_deg != 0.0 {
            push(
                &mut out,
                format_args!(" +pm={}", self.datum.prime_meridian_deg),
            );
        }
        match &self.datum.to_wgs84 {
            DatumShift::Identity => {}
            DatumShift::Helmert(h) if h.rates.is_some() => {
                return Err(ProjExportError::TimeDependentShift { srid: self.srid });
            }
            DatumShift::Helmert(h) => {
                let [tx, ty, tz, rx, ry, rz, s] = h.to_towgs84();
                push(
                    &mut out,
                    format_args!(" +towgs84={tx},{ty},{tz},{rx},{ry},{rz},{s}"),
                );
            }
        }
        match self.kind {
            CrsKind::Geographic => {}
            CrsKind::Projected => write_unit(&mut out, &self.unit),
        }
        out.push_str(" +no_defs");
        Ok(out.split_off(1))
    }
}

/// Append formatted text. Writing to a `String` cannot fail, so the
/// `fmt::Result` carries nothing.
fn push(out: &mut String, args: core::fmt::Arguments<'_>) {
    let _ = out.write_fmt(args);
}

fn write_ellipsoid(out: &mut String, e: &Ellipsoid) {
    match e.proj_name() {
        Some(name) => push(out, format_args!(" +ellps={name}")),
        None if e.is_sphere() => push(out, format_args!(" +R={}", e.a)),
        None => push(out, format_args!(" +a={} +rf={}", e.a, e.inv_f)),
    }
}

fn write_unit(out: &mut String, unit: &LinearUnit) {
    match unit.proj_name() {
        Some(name) => push(out, format_args!(" +units={name}")),
        None => push(out, format_args!(" +to_meter={}", unit.metres_per_unit)),
    }
}

#[cfg(test)]
mod tests {
    use crate::def::{CrsDef, Datum, DatumShift, Ellipsoid, Helmert};
    use crate::parse::parse_proj_string;
    use crate::srid::Srid;

    fn round_trip(text: &str) {
        let srid = Srid::new(910_000).unwrap();
        let def = parse_proj_string(srid, "rt", text).unwrap();
        let written = def.to_proj_string().unwrap();
        let again = parse_proj_string(srid, "rt", &written).unwrap();
        assert_eq!(again, def, "{text} -> {written}");
    }

    #[test]
    fn round_trips() {
        round_trip("+proj=longlat +datum=WGS84 +no_defs");
        round_trip("+proj=utm +zone=48 +datum=WGS84 +units=m +no_defs");
        round_trip("+proj=utm +zone=50 +south +ellps=GRS80 +towgs84=0,0,0 +units=m +no_defs");
        round_trip(
            "+proj=omerc +no_uoff +lat_0=4 +lonc=102.25 +alpha=323.025796466667 \
             +gamma=323.130102361111 +k=0.99984 +x_0=804671 +y_0=0 +ellps=GRS80 +units=m +no_defs",
        );
        round_trip("+proj=omerc +lat_0=4 +lonc=115 +alpha=53.31580995 +k=0.99984 +no_rot");
        round_trip(
            "+proj=tmerc +lat_0=4 +lon_0=102 +k=0.99995 +x_0=40000 +y_0=-10000 \
             +a=6377304.063 +rf=300.8017 +pm=paris +towgs84=-11,851,5,0.1,-0.2,0.3,1.5 \
             +to_meter=0.3047972654",
        );
        round_trip("+proj=longlat +R=6371007.181 +no_defs");
        round_trip("+proj=tmerc +a=6378000 +rf=297.5 +units=us-ft");
        round_trip("+proj=tmerc +axis=wsu +lon_0=17 +ellps=WGS84");
        round_trip("+proj=longlat +axis=neu +datum=WGS84");
    }

    #[test]
    fn every_method_round_trips() {
        for text in [
            "+proj=lcc +lat_1=33 +lat_2=45 +lat_0=23 +lon_0=-96 +x_0=10 +y_0=-20",
            "+proj=lcc +axis=wnu +lat_1=65 +lat_0=65 +lon_0=-18 +k_0=1 +x_0=-500000 +ellps=intl",
            "+proj=merc +lat_ts=10 +lon_0=3",
            "+proj=merc +k=0.997 +lon_0=110",
            "+proj=webmerc +datum=WGS84",
            "+proj=cea +lat_ts=30 +ellps=WGS84",
            "+proj=cea +k_0=1 +R=6371000",
            "+proj=eqc +lat_ts=12 +lat_0=1",
            "+proj=stere +lat_0=90 +lat_ts=70 +lon_0=-45",
            "+proj=stere +lat_0=52 +lon_0=5 +k=0.9999",
            "+proj=ups +south +ellps=WGS84",
            "+proj=sterea +lat_0=52.15 +lon_0=5.38 +k=0.9999079 +x_0=155000 +y_0=463000",
            "+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=23 +lon_0=-96",
            "+proj=laea +lat_0=52 +lon_0=10 +x_0=4321000 +y_0=3210000",
            "+proj=cass +lat_0=4 +lon_0=101 +hyperbolic",
            "+proj=cass +axis=swu +lat_0=48 +lon_0=31.8 +pm=-17.6666666666667",
            "+proj=poly +lat_0=1 +lon_0=-54",
            "+proj=aeqd +guam +lat_0=13.47 +lon_0=144.75",
            "+proj=aeqd +lat_0=90",
            "+proj=sinu +R=6371007.181",
            "+proj=eqearth +lon_0=150",
            "+proj=krovak +axis=swu +czech +ellps=bessel",
            "+proj=mod_krovak +ellps=bessel +x_0=5000000 +y_0=5000000",
            "+proj=somerc +lat_0=46.95 +lon_0=7.44 +k_0=1 +x_0=2600000 +y_0=1200000",
            "+proj=nzmg +ellps=intl",
            "+proj=geos +h=35785831 +sweep=x +lon_0=-75",
            "+proj=labrd +lat_0=-18.9 +lon_0=46.43 +azi=18.9 +k=0.9995 +x_0=400000",
        ] {
            round_trip(text);
        }
    }

    /// `+towgs84` has no rates, so a time-dependent shift is refused.
    #[test]
    fn time_dependent_shift_has_no_proj_string() {
        let srid = Srid::new(910_000).unwrap();
        let mut def = parse_proj_string(srid, "td", "+proj=longlat +ellps=GRS80").unwrap();
        let mut h = Helmert::position_vector([1.0, 2.0, 3.0, 0.5, 0.0, 0.0, 0.25]);
        h.rates = Some(crate::def::HelmertRates::new([0.1; 7], 2010.0));
        def.datum.to_wgs84 = DatumShift::Helmert(h);
        assert_eq!(
            def.to_proj_string(),
            Err(crate::error::ProjExportError::TimeDependentShift { srid })
        );
    }

    #[test]
    fn coordinate_frame_writes_position_vector_rotations() {
        let mut h = Helmert::position_vector([1.0, 2.0, 3.0, 0.5, 0.0, 0.0, 0.0]);
        h.convention = crate::def::RotationConvention::CoordinateFrame;
        let def = CrsDef {
            srid: Srid::WGS84,
            name: "cf".into(),
            kind: crate::def::CrsKind::Geographic,
            datum: Datum {
                ellipsoid: Ellipsoid::INTERNATIONAL_1924,
                prime_meridian_deg: 0.0,
                to_wgs84: DatumShift::Helmert(h),
                epsg_code: Some(6230),
            },
            projection: None,
            unit: crate::def::LinearUnit::METRE,
            axis: crate::def::AxisOrder::ENU,
            area_of_use: None,
        };
        assert_eq!(
            def.to_proj_string().as_deref(),
            Ok("+proj=longlat +ellps=intl +towgs84=1,2,3,-0.5,0,0,0 +no_defs")
        );
    }
}
