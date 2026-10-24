// SPDX-License-Identifier: Apache-2.0

//! Helmert transformation parameters as WKT2 and PROJJSON name them.
//!
//! A static Helmert has seven parameters, IOGP Guidance Note 7-2 §4.2.3. A
//! time-dependent Helmert, GN 7-2 §4.2.5, adds the seven yearly rates and
//! the parameter reference epoch. Unit factors follow the EPSG convention:
//! SI base unit per unit, with rates per second, so "millimetres per year"
//! is 0.001 / [`SECONDS_PER_YEAR`] metre per second.

use super::model::{ARC_SECOND_RAD, PPM, convert, normalize_name};
use crate::def::{DatumShift, Helmert, HelmertRates, RotationConvention};
use crate::error::CrsParseError;

/// Seconds per year, EPSG unit of measure 1029 ("year"), factor
/// 31556925.445 s. Rate units in EPSG and PROJ divide by this year.
pub(crate) const SECONDS_PER_YEAR: f64 = 31_556_925.445;

/// The unit a Helmert parameter holds in a [`Helmert`] value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HelmertUnit {
    /// Metre.
    Length,
    /// Arc-second.
    Angle,
    /// Part per million.
    Scale,
    /// Metre per year.
    LengthRate,
    /// Arc-second per year.
    AngleRate,
    /// Part per million per year.
    ScaleRate,
    /// Year.
    Time,
}

impl HelmertUnit {
    /// SI factor of the unit: metres, radians, unity or seconds per unit,
    /// per second for a rate.
    pub(crate) const fn factor(self) -> f64 {
        match self {
            HelmertUnit::Length => 1.0,
            HelmertUnit::Angle => ARC_SECOND_RAD,
            HelmertUnit::Scale => PPM,
            HelmertUnit::LengthRate => 1.0 / SECONDS_PER_YEAR,
            HelmertUnit::AngleRate => ARC_SECOND_RAD / SECONDS_PER_YEAR,
            HelmertUnit::ScaleRate => PPM / SECONDS_PER_YEAR,
            HelmertUnit::Time => SECONDS_PER_YEAR,
        }
    }

    /// The unit's EPSG name: units of measure 9001, 9104, 9202, 1042,
    /// 1043, 1041 and 1029.
    pub(crate) const fn name(self) -> &'static str {
        match self {
            HelmertUnit::Length => "metre",
            HelmertUnit::Angle => "arc-second",
            HelmertUnit::Scale => "parts per million",
            HelmertUnit::LengthRate => "metres per year",
            HelmertUnit::AngleRate => "arc-seconds per year",
            HelmertUnit::ScaleRate => "parts per million per year",
            HelmertUnit::Time => "year",
        }
    }

    /// The WKT2 unit keyword.
    pub(crate) const fn wkt_keyword(self) -> &'static str {
        match self {
            HelmertUnit::Length | HelmertUnit::LengthRate => "LENGTHUNIT",
            HelmertUnit::Angle | HelmertUnit::AngleRate => "ANGLEUNIT",
            HelmertUnit::Scale | HelmertUnit::ScaleRate => "SCALEUNIT",
            HelmertUnit::Time => "TIMEUNIT",
        }
    }

    /// The PROJJSON unit type.
    pub(crate) const fn projjson_type(self) -> &'static str {
        match self {
            HelmertUnit::Length | HelmertUnit::LengthRate => "LinearUnit",
            HelmertUnit::Angle | HelmertUnit::AngleRate => "AngularUnit",
            HelmertUnit::Scale | HelmertUnit::ScaleRate => "ScaleUnit",
            HelmertUnit::Time => "TimeUnit",
        }
    }
}

/// Number of Helmert parameters: seven values, seven rates, one epoch.
pub(crate) const HELMERT_PARAM_COUNT: usize = 15;

/// Index of the parameter reference epoch in [`HELMERT_PARAMS`].
const EPOCH_SLOT: usize = 14;

/// First rate slot in [`HELMERT_PARAMS`].
const FIRST_RATE_SLOT: usize = 7;

/// EPSG code, name and unit of every Helmert parameter: the seven values
/// in `+towgs84` order (8605 to 8611), their rates (1040 to 1046), and the
/// parameter reference epoch (1047).
pub(crate) const HELMERT_PARAMS: [(u32, &str, HelmertUnit); HELMERT_PARAM_COUNT] = [
    (8605, "X-axis translation", HelmertUnit::Length),
    (8606, "Y-axis translation", HelmertUnit::Length),
    (8607, "Z-axis translation", HelmertUnit::Length),
    (8608, "X-axis rotation", HelmertUnit::Angle),
    (8609, "Y-axis rotation", HelmertUnit::Angle),
    (8610, "Z-axis rotation", HelmertUnit::Angle),
    (8611, "Scale difference", HelmertUnit::Scale),
    (
        1040,
        "Rate of change of X-axis translation",
        HelmertUnit::LengthRate,
    ),
    (
        1041,
        "Rate of change of Y-axis translation",
        HelmertUnit::LengthRate,
    ),
    (
        1042,
        "Rate of change of Z-axis translation",
        HelmertUnit::LengthRate,
    ),
    (
        1043,
        "Rate of change of X-axis rotation",
        HelmertUnit::AngleRate,
    ),
    (
        1044,
        "Rate of change of Y-axis rotation",
        HelmertUnit::AngleRate,
    ),
    (
        1045,
        "Rate of change of Z-axis rotation",
        HelmertUnit::AngleRate,
    ),
    (
        1046,
        "Rate of change of Scale difference",
        HelmertUnit::ScaleRate,
    ),
    (1047, "Parameter reference epoch", HelmertUnit::Time),
];

/// The parameter slot a transformation parameter fills, by EPSG code or
/// name.
pub(crate) fn helmert_slot(code: Option<u32>, name: &str) -> Option<usize> {
    let normalized = normalize_name(name);
    HELMERT_PARAMS
        .iter()
        .position(|(c, n, _)| code == Some(*c) || normalize_name(n) == normalized)
}

/// The values a transformation's parameters give, in [`Helmert`] units.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct HelmertValues {
    values: [f64; HELMERT_PARAM_COUNT],
    given: [bool; HELMERT_PARAM_COUNT],
}

impl HelmertValues {
    /// Record parameter `slot` as `value` in a unit of SI factor `factor`.
    /// `None` takes the parameter's own unit.
    pub(crate) fn set(&mut self, slot: usize, value: f64, factor: Option<f64>) {
        let Some((_, _, unit)) = HELMERT_PARAMS.get(slot) else {
            return;
        };
        let canonical = unit.factor();
        self.values[slot] = convert(value, factor.unwrap_or(canonical), canonical);
        self.given[slot] = true;
    }

    /// The datum shift these values give under the EPSG method
    /// `method_code` / `method_name`.
    pub(crate) fn into_shift(
        self,
        method_code: Option<u32>,
        method_name: &str,
        accuracy_m: Option<f64>,
        format: &'static str,
    ) -> Result<DatumShift, CrsParseError> {
        let invalid = |detail: String| CrsParseError::content(format, detail);
        let (convention, time_dependent) =
            method_kind(method_code, method_name).ok_or_else(|| {
                invalid(format!(
                    "transformation method '{method_name}' is not a Helmert transformation"
                ))
            })?;
        let mut values = [0.0; 7];
        values.copy_from_slice(&self.values[..FIRST_RATE_SLOT]);
        let mut helmert = Helmert::position_vector(values);
        helmert.convention = convention;
        helmert.accuracy_m = accuracy_m;
        let any_rate = self.given[FIRST_RATE_SLOT..].iter().any(|g| *g);
        if time_dependent {
            if !self.given[EPOCH_SLOT] {
                return Err(invalid(format!(
                    "time-dependent method '{method_name}' has no parameter reference epoch"
                )));
            }
            let mut rates = [0.0; 7];
            rates.copy_from_slice(&self.values[FIRST_RATE_SLOT..EPOCH_SLOT]);
            helmert.rates = Some(HelmertRates::new(rates, self.values[EPOCH_SLOT]));
        } else if any_rate {
            return Err(invalid(format!(
                "method '{method_name}' is static, but the transformation has rates; use a \
                 time-dependent method such as EPSG 1057"
            )));
        }
        Ok(DatumShift::Helmert(helmert))
    }
}

/// Rotation convention and whether the method is time-dependent, from the
/// EPSG method code or name, IOGP GN 7-2 §4.2.3, §4.2.5 and §4.3.1.
fn method_kind(code: Option<u32>, name: &str) -> Option<(RotationConvention, bool)> {
    use RotationConvention::{CoordinateFrame, PositionVector};
    // Geocentric translations: 9603 geog2D, 1031 geocentric, 1035 geog3D.
    // Position vector: 9606, 1033, 1037. Coordinate frame: 9607, 1032, 1038.
    // Time-dependent position vector: 1054 geog2D, 1053 geocentric, 1055
    // geog3D. Time-dependent coordinate frame: 1057, 1056, 1058.
    match code {
        Some(9603 | 1031 | 1035 | 9606 | 1033 | 1037) => return Some((PositionVector, false)),
        Some(9607 | 1032 | 1038) => return Some((CoordinateFrame, false)),
        Some(1053..=1055) => return Some((PositionVector, true)),
        Some(1056..=1058) => return Some((CoordinateFrame, true)),
        Some(_) | None => {}
    }
    let normalized = normalize_name(name);
    let prefixes = [
        ("timedependentpositionvector", PositionVector, true),
        ("timedependentcoordinateframe", CoordinateFrame, true),
        ("positionvector", PositionVector, false),
        ("geocentrictranslations", PositionVector, false),
        ("coordinateframe", CoordinateFrame, false),
    ];
    prefixes
        .iter()
        .find(|(prefix, _, _)| normalized.starts_with(prefix))
        .map(|(_, convention, td)| (*convention, *td))
}

/// The EPSG method name and code a writer gives `h`: the geog2D domain
/// variant of its convention, time-dependent when it has rates.
pub(crate) fn writer_method(h: &Helmert) -> (&'static str, u32) {
    match (h.convention, h.rates.is_some()) {
        (RotationConvention::PositionVector, false) => {
            ("Position Vector transformation (geog2D domain)", 9606)
        }
        (RotationConvention::CoordinateFrame, false) => {
            ("Coordinate Frame rotation (geog2D domain)", 9607)
        }
        (RotationConvention::PositionVector, true) => (
            "Time-dependent Position Vector transformation (geog2D domain)",
            1054,
        ),
        (RotationConvention::CoordinateFrame, true) => (
            "Time-dependent Coordinate Frame rotation (geog2D domain)",
            1057,
        ),
    }
}

/// The parameters a writer emits for `h`, in the stored convention: the
/// seven values, then the rates and epoch when it has them.
pub(crate) fn writer_params(
    h: &Helmert,
) -> impl Iterator<Item = (u32, &'static str, HelmertUnit, f64)> {
    let mut values = [0.0; HELMERT_PARAM_COUNT];
    values[..FIRST_RATE_SLOT].copy_from_slice(&[
        h.tx_m,
        h.ty_m,
        h.tz_m,
        h.rx_arcsec,
        h.ry_arcsec,
        h.rz_arcsec,
        h.scale_ppm,
    ]);
    let count = match h.rates {
        None => FIRST_RATE_SLOT,
        Some(r) => {
            values[FIRST_RATE_SLOT..EPOCH_SLOT].copy_from_slice(&r.values());
            values[EPOCH_SLOT] = r.reference_epoch;
            HELMERT_PARAM_COUNT
        }
    };
    HELMERT_PARAMS
        .into_iter()
        .zip(values)
        .take(count)
        .map(|((code, name, unit), value)| (code, name, unit, value))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shift(code: Option<u32>, name: &str, values: HelmertValues) -> Result<Helmert, String> {
        match values.into_shift(code, name, None, "test") {
            Ok(DatumShift::Helmert(h)) => Ok(h),
            Ok(DatumShift::Identity) => Err("identity".into()),
            Err(e) => Err(e.to_string()),
        }
    }

    fn seven() -> HelmertValues {
        let mut v = HelmertValues::default();
        for (slot, value) in [1.0, 2.0, 3.0, 0.1, 0.2, 0.3, 4.0].into_iter().enumerate() {
            v.set(slot, value, None);
        }
        v
    }

    #[test]
    fn static_methods_pick_the_convention() {
        let cf = shift(Some(9607), "x", seven()).unwrap();
        assert_eq!(cf.convention, RotationConvention::CoordinateFrame);
        assert_eq!(cf.rates, None);
        let pv = shift(None, "Position Vector transformation", seven()).unwrap();
        assert_eq!(pv.convention, RotationConvention::PositionVector);
        assert_eq!(pv.to_towgs84(), [1.0, 2.0, 3.0, 0.1, 0.2, 0.3, 4.0]);
        for code in [9603, 1031, 1035, 1037] {
            assert_eq!(
                shift(Some(code), "x", seven()).map(|h| h.convention),
                Ok(RotationConvention::PositionVector)
            );
        }
        assert!(shift(None, "NTv2", seven()).is_err());
    }

    /// Rates in the EPSG per-second factors PROJ writes: millimetres per
    /// year 3.16887651727315e-11 m/s, milliarc-seconds per year
    /// 1.53631468932076e-16 rad/s, parts per billion per year
    /// 3.16887651727315e-17 /s, year 31556925.445 s.
    #[test]
    fn time_dependent_rates_convert_from_epsg_units() {
        let mut v = seven();
        let slot = |name: &str| helmert_slot(None, name).unwrap();
        v.set(
            slot("Rate of change of X-axis translation"),
            1.42,
            Some(3.168_876_517_273_15e-11),
        );
        v.set(
            slot("Rate of change of Z-axis rotation"),
            1.1551,
            Some(1.536_314_689_320_76e-16),
        );
        v.set(
            slot("Rate of change of Scale difference"),
            0.109,
            Some(3.168_876_517_273_15e-17),
        );
        v.set(
            helmert_slot(Some(1047), "").unwrap(),
            1994.0,
            Some(SECONDS_PER_YEAR),
        );
        let h = shift(None, "Time-dependent Coordinate Frame rotation (geocen)", v).unwrap();
        assert_eq!(h.convention, RotationConvention::CoordinateFrame);
        let r = h.rates.unwrap();
        assert!((r.dtx_m_per_yr - 0.001_42).abs() < 1e-15, "{r:?}");
        assert!((r.drz_arcsec_per_yr - 0.001_155_1).abs() < 1e-15, "{r:?}");
        assert!((r.dscale_ppm_per_yr - 0.000_109).abs() < 1e-15, "{r:?}");
        assert_eq!(r.reference_epoch, 1994.0);
        assert_eq!(
            shift(Some(1053), "x", v).map(|h| h.convention),
            Ok(RotationConvention::PositionVector)
        );
    }

    #[test]
    fn rates_need_a_time_dependent_method_and_an_epoch() {
        let mut v = seven();
        v.set(FIRST_RATE_SLOT, 0.001, None);
        let err = shift(Some(9606), "x", v).unwrap_err();
        assert!(err.contains("static"), "{err}");
        let err = shift(Some(1054), "x", v).unwrap_err();
        assert!(err.contains("reference epoch"), "{err}");
    }

    /// A GDA94 `BoundCRS` carrying EPSG transformation 6276 (ITRF2008 to
    /// GDA94, method 1056) with the parameter units PROJ 9.8.1 writes.
    const TIME_DEPENDENT_PROJJSON: &str = r#"{"type": "BoundCRS",
      "source_crs": {"type": "GeographicCRS", "name": "GDA94",
        "datum": {"type": "GeodeticReferenceFrame", "name": "Geocentric Datum of Australia 1994",
          "ellipsoid": {"name": "GRS 1980", "semi_major_axis": 6378137, "inverse_flattening": 298.257222101}},
        "coordinate_system": {"subtype": "ellipsoidal", "axis": [
          {"name": "Geodetic latitude", "abbreviation": "Lat", "direction": "north", "unit": "degree"},
          {"name": "Geodetic longitude", "abbreviation": "Lon", "direction": "east", "unit": "degree"}]},
        "id": {"authority": "EPSG", "code": 4283}},
      "target_crs": {"type": "GeographicCRS", "name": "WGS 84",
        "datum": {"type": "GeodeticReferenceFrame", "name": "World Geodetic System 1984",
          "ellipsoid": {"name": "WGS 84", "semi_major_axis": 6378137, "inverse_flattening": 298.257223563}},
        "coordinate_system": {"subtype": "ellipsoidal", "axis": [
          {"name": "Geodetic latitude", "abbreviation": "Lat", "direction": "north", "unit": "degree"},
          {"name": "Geodetic longitude", "abbreviation": "Lon", "direction": "east", "unit": "degree"}]}},
      "transformation": {"name": "ITRF2008 to GDA94 (1)",
        "method": {"name": "Time-dependent Coordinate Frame rotation (geocen)", "id": {"authority": "EPSG", "code": 1056}},
        "parameters": [
          {"name": "X-axis translation", "value": -84.68, "unit": {"type": "LinearUnit", "name": "millimetre", "conversion_factor": 0.001}, "id": {"authority": "EPSG", "code": 8605}},
          {"name": "Y-axis translation", "value": -19.42, "unit": {"type": "LinearUnit", "name": "millimetre", "conversion_factor": 0.001}, "id": {"authority": "EPSG", "code": 8606}},
          {"name": "Z-axis translation", "value": 32.01, "unit": {"type": "LinearUnit", "name": "millimetre", "conversion_factor": 0.001}, "id": {"authority": "EPSG", "code": 8607}},
          {"name": "X-axis rotation", "value": -0.4254, "unit": {"type": "AngularUnit", "name": "milliarc-second", "conversion_factor": 4.84813681109536e-09}, "id": {"authority": "EPSG", "code": 8608}},
          {"name": "Y-axis rotation", "value": 2.2578, "unit": {"type": "AngularUnit", "name": "milliarc-second", "conversion_factor": 4.84813681109536e-09}, "id": {"authority": "EPSG", "code": 8609}},
          {"name": "Z-axis rotation", "value": 2.4015, "unit": {"type": "AngularUnit", "name": "milliarc-second", "conversion_factor": 4.84813681109536e-09}, "id": {"authority": "EPSG", "code": 8610}},
          {"name": "Scale difference", "value": 9.71, "unit": {"type": "ScaleUnit", "name": "parts per billion", "conversion_factor": 1e-09}, "id": {"authority": "EPSG", "code": 8611}},
          {"name": "Rate of change of X-axis translation", "value": 1.42, "unit": {"type": "LinearUnit", "name": "millimetres per year", "conversion_factor": 3.16887651727315e-11}, "id": {"authority": "EPSG", "code": 1040}},
          {"name": "Rate of change of Y-axis translation", "value": 1.34, "unit": {"type": "LinearUnit", "name": "millimetres per year", "conversion_factor": 3.16887651727315e-11}, "id": {"authority": "EPSG", "code": 1041}},
          {"name": "Rate of change of Z-axis translation", "value": 0.9, "unit": {"type": "LinearUnit", "name": "millimetres per year", "conversion_factor": 3.16887651727315e-11}, "id": {"authority": "EPSG", "code": 1042}},
          {"name": "Rate of change of X-axis rotation", "value": 1.5461, "unit": {"type": "AngularUnit", "name": "milliarc-seconds per year", "conversion_factor": 1.53631468932076e-16}, "id": {"authority": "EPSG", "code": 1043}},
          {"name": "Rate of change of Y-axis rotation", "value": 1.182, "unit": {"type": "AngularUnit", "name": "milliarc-seconds per year", "conversion_factor": 1.53631468932076e-16}, "id": {"authority": "EPSG", "code": 1044}},
          {"name": "Rate of change of Z-axis rotation", "value": 1.1551, "unit": {"type": "AngularUnit", "name": "milliarc-seconds per year", "conversion_factor": 1.53631468932076e-16}, "id": {"authority": "EPSG", "code": 1045}},
          {"name": "Rate of change of Scale difference", "value": 0.109, "unit": {"type": "ScaleUnit", "name": "parts per billion per year", "conversion_factor": 3.16887651727315e-17}, "id": {"authority": "EPSG", "code": 1046}},
          {"name": "Parameter reference epoch", "value": 1994, "unit": {"type": "TimeUnit", "name": "year", "conversion_factor": 31556925.445}, "id": {"authority": "EPSG", "code": 1047}}]}}"#;

    /// Relative agreement of a parameter converted from PROJ's 15-digit
    /// unit factors.
    const FACTOR_REL_TOL: f64 = 1e-12;

    fn close(got: f64, want: f64) -> bool {
        (got - want).abs() <= want.abs() * FACTOR_REL_TOL
    }

    #[test]
    fn projjson_time_dependent_bound_crs_reads_and_round_trips() {
        use crate::parse::{parse_projjson, parse_wkt2};
        use crate::srid::Srid;
        let srid = Srid::new(910_010).unwrap();
        let def = parse_projjson(srid, "td", TIME_DEPENDENT_PROJJSON).unwrap();
        let DatumShift::Helmert(h) = def.datum.to_wgs84 else {
            panic!("expected a Helmert shift");
        };
        assert_eq!(h.convention, RotationConvention::CoordinateFrame);
        let want = [
            -0.084_68,
            -0.019_42,
            0.032_01,
            -0.000_425_4,
            0.002_257_8,
            0.002_401_5,
            0.009_71,
        ];
        for (got, want) in h.to_towgs84().iter().zip(want).take(3) {
            assert!(close(*got, want), "{got} {want}");
        }
        assert!(
            close(h.rx_arcsec, want[3]) && close(h.scale_ppm, want[6]),
            "{h:?}"
        );
        let r = h.rates.unwrap();
        let want_rates = [
            0.001_42,
            0.001_34,
            0.000_90,
            0.001_546_1,
            0.001_182,
            0.001_155_1,
            0.000_109,
        ];
        for (got, want) in r.values().iter().zip(want_rates) {
            assert!(close(*got, want), "{got} {want}");
        }
        assert_eq!(r.reference_epoch, 1994.0);
        let json = parse_projjson(srid, "td", &def.to_projjson()).unwrap();
        assert_eq!(json, def);
        let wkt = parse_wkt2(srid, "td", &def.to_wkt2()).unwrap();
        assert_eq!(wkt, def);
    }

    #[test]
    fn wkt2_time_dependent_bound_crs_reads_named_units() {
        use crate::parse::parse_wkt2;
        use crate::srid::Srid;
        let text = r#"BOUNDCRS[
            SOURCECRS[GEOGCRS["GDA94",DATUM["GDA94",ELLIPSOID["GRS 1980",6378137,298.257222101]],
                CS[ellipsoidal,2],AXIS["latitude",north],AXIS["longitude",east],
                ANGLEUNIT["degree",0.0174532925199433]]],
            TARGETCRS[GEOGCRS["WGS 84",DATUM["World Geodetic System 1984",ELLIPSOID["WGS 84",6378137,298.257223563]],
                CS[ellipsoidal,2],AXIS["latitude",north],AXIS["longitude",east],ANGLEUNIT["degree",0.0174532925199433]]],
            ABRIDGEDTRANSFORMATION["t",
                METHOD["Time-dependent Position Vector transformation (geog2D domain)",ID["EPSG",1054]],
                PARAMETER["X-axis translation",0.5],
                PARAMETER["Rate of change of Z-axis translation",2,LENGTHUNIT["millimetres per year",3.16887651727315E-11]],
                PARAMETER["Parameter reference epoch",2020,TIMEUNIT["year",31556925.445]]]]"#;
        let def = parse_wkt2(Srid::new(910_011).unwrap(), "td", text).unwrap();
        let DatumShift::Helmert(h) = def.datum.to_wgs84 else {
            panic!("expected a Helmert shift");
        };
        assert_eq!(h.convention, RotationConvention::PositionVector);
        assert_eq!(h.tx_m, 0.5);
        let r = h.rates.unwrap();
        assert!(close(r.dtz_m_per_yr, 0.002), "{r:?}");
        assert_eq!(r.reference_epoch, 2020.0);
    }

    #[test]
    fn writers_emit_rates_only_when_present() {
        let static_h = Helmert::translation(1.0, 2.0, 3.0);
        assert_eq!(writer_params(&static_h).count(), 7);
        assert_eq!(writer_method(&static_h).1, 9606);
        let mut td = static_h;
        td.convention = RotationConvention::CoordinateFrame;
        td.rates = Some(HelmertRates::new([0.0; 7], 2020.0));
        let params: Vec<_> = writer_params(&td).collect();
        assert_eq!(params.len(), HELMERT_PARAM_COUNT);
        assert_eq!(params[EPOCH_SLOT].0, 1047);
        assert_eq!(params[EPOCH_SLOT].3, 2020.0);
        assert_eq!(writer_method(&td).1, 1057);
    }
}
