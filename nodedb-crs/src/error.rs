// SPDX-License-Identifier: Apache-2.0

//! Errors for SRID resolution, PROJ-string parsing and coordinate transforms.

pub use nodedb_srid::{CrsParseError, NotUserSrid, UnknownSrid};

use crate::srid::Srid;

/// Any error a CRS operation reports.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum CrsError {
    /// The coordinate lies outside the region where a projection is defined
    /// or accurate.
    #[error("coordinate ({x}, {y}) is outside the domain of SRID {srid}: {reason}")]
    OutOfDomain {
        srid: Srid,
        x: f64,
        y: f64,
        reason: DomainReason,
    },
    #[error(transparent)]
    Unknown(#[from] UnknownSrid),
    #[error(transparent)]
    Parse(#[from] CrsParseError),
    #[error(transparent)]
    Epoch(#[from] InvalidEpoch),
    #[error(transparent)]
    ProjExport(#[from] ProjExportError),
}

/// Why a coordinate is outside a projection's domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DomainReason {
    /// An ordinate is NaN or infinite.
    NonFinite,
    /// Latitude is beyond ±90°.
    LatitudeRange,
    /// Longitude is too far from a Transverse Mercator central meridian for
    /// the Krüger series to hold.
    CentralMeridianOffset,
    /// The Hotine Oblique Mercator U term reaches ±1, where the projection
    /// diverges.
    ObliqueSingularity,
    /// The point is a pole or a singular point the projection maps to
    /// infinity.
    Singularity,
    /// Latitude is beyond the projection's limit, such as ±85.06° for
    /// Pseudo-Mercator.
    LatitudeLimit,
    /// A geostationary view does not see the point.
    NotVisible,
    /// The point is the antipode of an azimuthal projection's centre.
    Antipode,
    /// Projected coordinates fall outside the area the projection maps.
    OutsideProjection,
    /// An iterative inverse did not converge.
    NoConvergence,
}

impl core::fmt::Display for DomainReason {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            DomainReason::NonFinite => "non-finite ordinate",
            DomainReason::LatitudeRange => "latitude beyond ±90°",
            DomainReason::CentralMeridianOffset => "too far from the central meridian",
            DomainReason::ObliqueSingularity => "on the oblique projection's singular line",
            DomainReason::Singularity => "at a point the projection maps to infinity",
            DomainReason::LatitudeLimit => "latitude beyond the projection's limit",
            DomainReason::NotVisible => "not visible from the geostationary satellite",
            DomainReason::Antipode => "at the antipode of the projection centre",
            DomainReason::OutsideProjection => "outside the area the projection maps",
            DomainReason::NoConvergence => "the inverse projection did not converge",
        })
    }
}

/// A coordinate epoch outside the accepted range, or not a finite number.
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
#[error(
    "coordinate epoch {epoch} is outside {min}..={max} decimal years",
    min = crate::def::MIN_COORDINATE_EPOCH,
    max = crate::def::MAX_COORDINATE_EPOCH
)]
pub struct InvalidEpoch {
    /// The epoch given, decimal year.
    pub epoch: f64,
}

/// A definition a PROJ string cannot express.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ProjExportError {
    /// `+towgs84` has no rates, so a time-dependent Helmert shift has no
    /// PROJ-string form. PROJ refuses the same export.
    #[error(
        "SRID {srid} has a time-dependent datum shift, which a PROJ string cannot carry; \
         use WKT2 or PROJJSON"
    )]
    TimeDependentShift { srid: Srid },
}

/// A change to a [`CustomCrsSet`](crate::CustomCrsSet) that the set refuses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CustomCrsError {
    /// User-defined CRSs take codes from the user range only.
    #[error(transparent)]
    NotUserCode(#[from] NotUserSrid),
    /// A live CRS already has this code.
    #[error("SRID {srid} is already defined")]
    Exists { srid: Srid },
    /// The code belonged to a dropped CRS. Dropped codes stay retired, so
    /// stored data never changes meaning.
    #[error("SRID {srid} was dropped and cannot be reused")]
    WasDropped { srid: Srid },
    /// No live CRS has this code.
    #[error("SRID {srid} is not a live user-defined CRS")]
    NotFound { srid: Srid },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messages_name_the_input() {
        let err = CrsError::from(UnknownSrid::Undefined { code: 2154 });
        assert!(err.to_string().contains("2154"));
        let err = CrsError::from(InvalidEpoch { epoch: 2300.0 });
        assert!(err.to_string().contains("2300"), "{err}");
        let srid = Srid::new(910_001).unwrap();
        let err = CrsError::from(ProjExportError::TimeDependentShift { srid });
        assert!(err.to_string().contains("910001"), "{err}");
    }
}
