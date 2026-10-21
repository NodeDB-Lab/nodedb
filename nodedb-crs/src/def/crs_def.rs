// SPDX-License-Identifier: Apache-2.0

//! A complete coordinate reference system definition.

use serde::{Deserialize, Serialize};

use super::area::AreaOfUse;
use super::axis::AxisOrder;
use super::datum::Datum;
use super::unit::LinearUnit;
use crate::method::ProjectionDef;
use crate::srid::Srid;

pub use nodedb_srid::CrsKind;

/// One CRS: datum, optional projection, unit, axis order and area of use.
///
/// Serialized field by field, so a decode reproduces the value exactly.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CrsDef {
    pub srid: Srid,
    pub name: Box<str>,
    pub kind: CrsKind,
    pub datum: Datum,
    /// The map projection. `None` for a geographic CRS.
    pub projection: Option<ProjectionDef>,
    /// Unit of projected coordinates. Geographic coordinates are always
    /// degrees and ignore it.
    pub unit: LinearUnit,
    /// Order and direction of the coordinate axes, PROJ `+axis`.
    pub axis: AxisOrder,
    pub area_of_use: Option<AreaOfUse>,
}

/// Number of fields in the MessagePack array form of [`CrsDef`].
const CRS_DEF_FIELDS: usize = 8;

impl CrsDef {
    /// Whether coordinates are longitude / latitude degrees.
    pub fn is_geographic(&self) -> bool {
        match self.kind {
            CrsKind::Geographic => true,
            CrsKind::Projected => false,
        }
    }
}

impl zerompk::ToMessagePack for CrsDef {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_array_len(CRS_DEF_FIELDS)?;
        self.srid.write(writer)?;
        writer.write_string(&self.name)?;
        self.kind.write(writer)?;
        self.datum.write(writer)?;
        self.projection.write(writer)?;
        self.unit.write(writer)?;
        self.axis.write(writer)?;
        self.area_of_use.write(writer)
    }
}

impl<'a> zerompk::FromMessagePack<'a> for CrsDef {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        reader.check_array_len(CRS_DEF_FIELDS)?;
        Ok(CrsDef {
            srid: Srid::read(reader)?,
            name: String::read(reader)?.into_boxed_str(),
            kind: CrsKind::read(reader)?,
            datum: Datum::read(reader)?,
            projection: Option::<ProjectionDef>::read(reader)?,
            unit: LinearUnit::read(reader)?,
            axis: AxisOrder::read(reader)?,
            area_of_use: Option::<AreaOfUse>::read(reader)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::def::{DatumShift, Ellipsoid, Helmert, RotationConvention};
    use crate::method::TmParams;

    fn sample() -> CrsDef {
        let mut helmert = Helmert::position_vector([1.0, -2.5, 3.25, 0.1, 0.2, -0.3, 1.5]);
        helmert.convention = RotationConvention::CoordinateFrame;
        helmert.accuracy_m = Some(2.0);
        helmert.rates = Some(crate::def::HelmertRates::new(
            [0.001, -0.002, 0.0, 0.0001, 0.0, -0.0003, 0.01],
            2020.0,
        ));
        CrsDef {
            srid: Srid::new(910_001).unwrap(),
            name: "Test TM".into(),
            kind: CrsKind::Projected,
            datum: Datum {
                ellipsoid: Ellipsoid::EVEREST_1830_MODIFIED,
                prime_meridian_deg: 2.5,
                to_wgs84: DatumShift::Helmert(helmert),
                epsg_code: Some(6245),
            },
            projection: Some(ProjectionDef::TransverseMercator(TmParams {
                lat0_deg: 1.0,
                lon0_deg: 102.0,
                k0: 0.9999,
                false_easting: 1.0e5,
                false_northing: -2.0e5,
            })),
            unit: LinearUnit {
                metres_per_unit: 0.3048,
            },
            axis: AxisOrder::parse("swu").unwrap(),
            area_of_use: AreaOfUse::parse("99,1,105,7").unwrap(),
        }
    }

    #[test]
    fn msgpack_round_trip_is_exact() {
        let def = sample();
        let bytes = zerompk::to_msgpack_vec(&def).unwrap();
        let back: CrsDef = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(back, def);
    }

    #[test]
    fn kind_helper() {
        assert!(!sample().is_geographic());
    }
}
