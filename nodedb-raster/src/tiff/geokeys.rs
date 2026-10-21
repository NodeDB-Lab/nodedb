// SPDX-License-Identifier: Apache-2.0

//! GeoKey directory parsing (OGC GeoTIFF 1.1, Requirements Class
//! GeoKeyDirectoryTag).
//!
//! The directory is a `u16` array: a 4-short header, then 4 shorts per
//! key. A key value lives inline (location 0), in the directory array
//! itself (34735), in GeoDoubleParamsTag (34736), or in GeoAsciiParamsTag
//! (34737). Every reference is bounds-checked against the array it names.

use std::mem::size_of;

use nodedb_types::decode_bounds::checked_decode_capacity;

/// `GTModelTypeGeoKey`: 1 projected, 2 geographic, 3 geocentric.
pub const GT_MODEL_TYPE: u16 = 1024;
/// `GTRasterTypeGeoKey`: 1 PixelIsArea, 2 PixelIsPoint.
pub const GT_RASTER_TYPE: u16 = 1025;
/// `GeodeticCRSGeoKey` (`GeographicTypeGeoKey` in GeoTIFF 1.0).
pub const GEOGRAPHIC_TYPE: u16 = 2048;
/// `ProjectedCRSGeoKey` (`ProjectedCSTypeGeoKey` in GeoTIFF 1.0).
pub const PROJECTED_CS_TYPE: u16 = 3072;
/// `GeodeticDatumGeoKey`: EPSG datum code, or 32767.
pub const GEODETIC_DATUM: u16 = 2050;
/// `PrimeMeridianGeoKey`: EPSG prime meridian code, or 32767.
pub const PRIME_MERIDIAN: u16 = 2051;
/// `GeogAngularUnitsGeoKey`: EPSG angle unit code, or 32767.
pub const GEOG_ANGULAR_UNITS: u16 = 2054;
/// `GeogAngularUnitSizeGeoKey`: radians per user-defined angle unit.
pub const GEOG_ANGULAR_UNIT_SIZE: u16 = 2055;
/// `EllipsoidGeoKey`: EPSG ellipsoid code, or 32767.
pub const ELLIPSOID: u16 = 2056;
/// `EllipsoidSemiMajorAxisGeoKey`, in the geodetic linear unit.
pub const SEMI_MAJOR_AXIS: u16 = 2057;
/// `EllipsoidSemiMinorAxisGeoKey`.
pub const SEMI_MINOR_AXIS: u16 = 2058;
/// `EllipsoidInvFlatteningGeoKey`.
pub const INV_FLATTENING: u16 = 2059;
/// `GeogAzimuthUnitsGeoKey`: EPSG angle unit of azimuths.
pub const GEOG_AZIMUTH_UNITS: u16 = 2060;
/// `PrimeMeridianLongitudeGeoKey`, in the angular unit.
pub const PRIME_MERIDIAN_LONGITUDE: u16 = 2061;
/// `ProjectionGeoKey`: EPSG conversion code, or 32767.
pub const PROJECTION: u16 = 3074;
/// `ProjMethodGeoKey` (`ProjCoordTransGeoKey` in GeoTIFF 1.0).
pub const PROJ_METHOD: u16 = 3075;
/// `ProjLinearUnitsGeoKey`: EPSG length unit code, or 32767.
pub const PROJ_LINEAR_UNITS: u16 = 3076;
/// `ProjLinearUnitSizeGeoKey`: metres per user-defined length unit.
pub const PROJ_LINEAR_UNIT_SIZE: u16 = 3077;
/// `ProjStdParallel1GeoKey`.
pub const PROJ_STD_PARALLEL_1: u16 = 3078;
/// `ProjStdParallel2GeoKey`.
pub const PROJ_STD_PARALLEL_2: u16 = 3079;
/// `ProjNatOriginLongGeoKey`.
pub const PROJ_NAT_ORIGIN_LONG: u16 = 3080;
/// `ProjNatOriginLatGeoKey`.
pub const PROJ_NAT_ORIGIN_LAT: u16 = 3081;
/// `ProjFalseEastingGeoKey`.
pub const PROJ_FALSE_EASTING: u16 = 3082;
/// `ProjFalseNorthingGeoKey`.
pub const PROJ_FALSE_NORTHING: u16 = 3083;
/// `ProjFalseOriginLongGeoKey`.
pub const PROJ_FALSE_ORIGIN_LONG: u16 = 3084;
/// `ProjFalseOriginLatGeoKey`.
pub const PROJ_FALSE_ORIGIN_LAT: u16 = 3085;
/// `ProjFalseOriginEastingGeoKey`.
pub const PROJ_FALSE_ORIGIN_EASTING: u16 = 3086;
/// `ProjFalseOriginNorthingGeoKey`.
pub const PROJ_FALSE_ORIGIN_NORTHING: u16 = 3087;
/// `ProjCenterLongGeoKey`.
pub const PROJ_CENTER_LONG: u16 = 3088;
/// `ProjCenterLatGeoKey`.
pub const PROJ_CENTER_LAT: u16 = 3089;
/// `ProjCenterEastingGeoKey`.
pub const PROJ_CENTER_EASTING: u16 = 3090;
/// `ProjCenterNorthingGeoKey`.
pub const PROJ_CENTER_NORTHING: u16 = 3091;
/// `ProjScaleAtNatOriginGeoKey`.
pub const PROJ_SCALE_AT_NAT_ORIGIN: u16 = 3092;
/// `ProjScaleAtCenterGeoKey`.
pub const PROJ_SCALE_AT_CENTER: u16 = 3093;
/// `ProjAzimuthAngleGeoKey`, in the azimuth unit.
pub const PROJ_AZIMUTH_ANGLE: u16 = 3094;
/// `ProjStraightVertPoleLongGeoKey`.
pub const PROJ_STRAIGHT_VERT_POLE_LONG: u16 = 3095;
/// `ProjRectifiedGridAngleGeoKey`, in the azimuth unit.
pub const PROJ_RECTIFIED_GRID_ANGLE: u16 = 3096;

const DIRECTORY_TAG: u16 = 34735;
const DOUBLE_PARAMS_TAG: u16 = 34736;
const ASCII_PARAMS_TAG: u16 = 34737;
const HEADER_SHORTS: usize = 4;
const ENTRY_SHORTS: usize = 4;
const DIRECTORY_VERSION: u16 = 1;
/// Upper bound on the bytes of the parsed key list.
const MAX_KEY_LIST_BYTES: usize = 4 * 1024 * 1024;

/// Every way a GeoKey directory can be malformed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GeoKeyError {
    #[error("the directory holds {len} shorts, fewer than its 4-short header")]
    TooShort { len: usize },

    #[error("directory version {version} is not 1")]
    Version { version: u16 },

    #[error("the header declares {declared} keys, but the directory holds {available}")]
    KeyCount { declared: u16, available: usize },

    #[error("key {key} follows key {previous}; keys must be strictly ascending")]
    UnsortedKey { key: u16, previous: u16 },

    #[error("key {key} has an inline value with count {count}; an inline value has count 1")]
    InlineCount { key: u16, count: u16 },

    #[error("key {key} stores its value in tag {location}, which the file lacks")]
    MissingParams { key: u16, location: u16 },

    #[error("key {key} stores its value in unknown tag {location}")]
    UnknownLocation { key: u16, location: u16 },

    #[error(
        "key {key} reads {count} values at offset {offset} of tag {location}, \
         which holds {len}"
    )]
    OutOfBounds {
        key: u16,
        location: u16,
        offset: u16,
        count: u16,
        len: usize,
    },

    #[error("key {key} ASCII value is not UTF-8")]
    AsciiEncoding { key: u16 },
}

/// One key value, in the type of the array it came from.
#[derive(Debug, Clone, PartialEq)]
pub enum GeoKeyValue {
    Short(Vec<u16>),
    Double(Vec<f64>),
    Ascii(String),
}

/// A parsed GeoKey directory, keys strictly ascending.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct GeoKeyDirectory {
    keys: Vec<(u16, GeoKeyValue)>,
}

impl GeoKeyDirectory {
    /// Parse `directory` (GeoKeyDirectoryTag) with its optional double and
    /// ASCII parameter arrays.
    pub fn parse(
        directory: &[u16],
        doubles: Option<&[f64]>,
        ascii: Option<&str>,
    ) -> Result<Self, GeoKeyError> {
        let (header, body) = match directory.split_at_checked(HEADER_SHORTS) {
            Some(split) => split,
            None => {
                return Err(GeoKeyError::TooShort {
                    len: directory.len(),
                });
            }
        };
        if header[0] != DIRECTORY_VERSION {
            return Err(GeoKeyError::Version { version: header[0] });
        }
        let declared = header[3];
        let available = body.len() / ENTRY_SHORTS;
        let capacity = checked_decode_capacity(
            usize::from(declared),
            size_of::<(u16, GeoKeyValue)>(),
            body.len(),
            ENTRY_SHORTS,
            usize::from(u16::MAX),
            MAX_KEY_LIST_BYTES,
        )
        .ok_or(GeoKeyError::KeyCount {
            declared,
            available,
        })?;
        let mut keys: Vec<(u16, GeoKeyValue)> = Vec::with_capacity(capacity);
        for &[key, location, count, offset] in
            body.as_chunks::<ENTRY_SHORTS>().0.iter().take(capacity)
        {
            if let Some((previous, _)) = keys.last()
                && *previous >= key
            {
                return Err(GeoKeyError::UnsortedKey {
                    key,
                    previous: *previous,
                });
            }
            let reference = Reference {
                key,
                location,
                offset,
                count,
            };
            let value = match location {
                0 => {
                    if count != 1 {
                        return Err(GeoKeyError::InlineCount { key, count });
                    }
                    GeoKeyValue::Short(vec![offset])
                }
                DIRECTORY_TAG => GeoKeyValue::Short(reference.slice(directory)?.to_vec()),
                DOUBLE_PARAMS_TAG => {
                    let doubles = doubles.ok_or(GeoKeyError::MissingParams { key, location })?;
                    GeoKeyValue::Double(reference.slice(doubles)?.to_vec())
                }
                ASCII_PARAMS_TAG => {
                    let ascii = ascii.ok_or(GeoKeyError::MissingParams { key, location })?;
                    let bytes = reference.slice(ascii.as_bytes())?;
                    let text = std::str::from_utf8(bytes)
                        .map_err(|_| GeoKeyError::AsciiEncoding { key })?;
                    // Each ASCII value ends with a `|` separator.
                    let text = text.strip_suffix('|').unwrap_or(text);
                    GeoKeyValue::Ascii(text.to_owned())
                }
                _ => return Err(GeoKeyError::UnknownLocation { key, location }),
            };
            keys.push((key, value));
        }
        Ok(Self { keys })
    }

    /// The value of `key`.
    pub fn get(&self, key: u16) -> Option<&GeoKeyValue> {
        self.keys
            .binary_search_by_key(&key, |(seen, _)| *seen)
            .ok()
            .and_then(|index| self.keys.get(index))
            .map(|(_, value)| value)
    }

    /// The first short of `key`, when `key` holds shorts.
    pub fn short(&self, key: u16) -> Option<u16> {
        match self.get(key)? {
            GeoKeyValue::Short(values) => values.first().copied(),
            GeoKeyValue::Double(_) | GeoKeyValue::Ascii(_) => None,
        }
    }

    /// The first double of `key`, when `key` holds doubles.
    pub fn double(&self, key: u16) -> Option<f64> {
        match self.get(key)? {
            GeoKeyValue::Double(values) => values.first().copied(),
            GeoKeyValue::Short(_) | GeoKeyValue::Ascii(_) => None,
        }
    }

    /// The number of keys.
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// True when the directory holds no key.
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

/// Where one key's value sits inside a parameter array.
struct Reference {
    key: u16,
    location: u16,
    offset: u16,
    count: u16,
}

impl Reference {
    /// The `count` values at `offset` of `values`, bounds-checked.
    fn slice<'v, T>(&self, values: &'v [T]) -> Result<&'v [T], GeoKeyError> {
        let start = usize::from(self.offset);
        let end = start + usize::from(self.count);
        values.get(start..end).ok_or(GeoKeyError::OutOfBounds {
            key: self.key,
            location: self.location,
            offset: self.offset,
            count: self.count,
            len: values.len(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(keys: u16) -> Vec<u16> {
        vec![1, 1, 0, keys]
    }

    #[test]
    fn inline_keys_parse() {
        let mut dir = header(3);
        dir.extend([GT_MODEL_TYPE, 0, 1, 1]);
        dir.extend([GT_RASTER_TYPE, 0, 1, 2]);
        dir.extend([PROJECTED_CS_TYPE, 0, 1, 32647]);
        let parsed = GeoKeyDirectory::parse(&dir, None, None).expect("parses");
        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed.short(GT_MODEL_TYPE), Some(1));
        assert_eq!(parsed.short(GT_RASTER_TYPE), Some(2));
        assert_eq!(parsed.short(PROJECTED_CS_TYPE), Some(32647));
    }

    #[test]
    fn parameter_arrays_are_read_by_offset() {
        let mut dir = header(2);
        dir.extend([1026, ASCII_PARAMS_TAG, 6, 0]);
        dir.extend([2057, DOUBLE_PARAMS_TAG, 2, 1]);
        let doubles = [0.0, 6_378_137.0, 298.257];
        let parsed =
            GeoKeyDirectory::parse(&dir, Some(&doubles), Some("UTM47|other|")).expect("parses");
        assert_eq!(
            parsed.get(2057),
            Some(&GeoKeyValue::Double(vec![6_378_137.0, 298.257]))
        );
        assert_eq!(
            parsed.get(1026),
            Some(&GeoKeyValue::Ascii("UTM47".to_owned()))
        );
    }

    #[test]
    fn directory_shorts_are_read_from_the_directory() {
        let mut dir = header(1);
        dir.extend([4000, DIRECTORY_TAG, 2, 8]);
        dir.extend([7, 9]);
        let parsed = GeoKeyDirectory::parse(&dir, None, None).expect("parses");
        assert_eq!(parsed.get(4000), Some(&GeoKeyValue::Short(vec![7, 9])));
    }

    #[test]
    fn key_count_is_bounded_by_the_array() {
        let mut dir = header(5);
        dir.extend([GT_MODEL_TYPE, 0, 1, 1]);
        assert_eq!(
            GeoKeyDirectory::parse(&dir, None, None),
            Err(GeoKeyError::KeyCount {
                declared: 5,
                available: 1
            })
        );
    }

    #[test]
    fn malformed_directories_are_typed_errors() {
        assert!(matches!(
            GeoKeyDirectory::parse(&[1, 1], None, None),
            Err(GeoKeyError::TooShort { len: 2 })
        ));
        assert!(matches!(
            GeoKeyDirectory::parse(&[2, 1, 0, 0], None, None),
            Err(GeoKeyError::Version { version: 2 })
        ));

        let mut dup = header(2);
        dup.extend([GT_MODEL_TYPE, 0, 1, 1, GT_MODEL_TYPE, 0, 1, 2]);
        assert!(matches!(
            GeoKeyDirectory::parse(&dup, None, None),
            Err(GeoKeyError::UnsortedKey { .. })
        ));

        let mut unsorted = header(2);
        unsorted.extend([GT_RASTER_TYPE, 0, 1, 1, GT_MODEL_TYPE, 0, 1, 1]);
        assert!(matches!(
            GeoKeyDirectory::parse(&unsorted, None, None),
            Err(GeoKeyError::UnsortedKey { .. })
        ));

        let mut inline = header(1);
        inline.extend([GT_MODEL_TYPE, 0, 2, 1]);
        assert!(matches!(
            GeoKeyDirectory::parse(&inline, None, None),
            Err(GeoKeyError::InlineCount { .. })
        ));

        let mut missing = header(1);
        missing.extend([2057, DOUBLE_PARAMS_TAG, 1, 0]);
        assert!(matches!(
            GeoKeyDirectory::parse(&missing, None, None),
            Err(GeoKeyError::MissingParams { .. })
        ));

        let mut past = header(1);
        past.extend([2057, DOUBLE_PARAMS_TAG, 4, 2]);
        assert!(matches!(
            GeoKeyDirectory::parse(&past, Some(&[1.0, 2.0, 3.0]), None),
            Err(GeoKeyError::OutOfBounds { .. })
        ));

        let mut unknown = header(1);
        unknown.extend([2057, 999, 1, 0]);
        assert!(matches!(
            GeoKeyDirectory::parse(&unknown, None, None),
            Err(GeoKeyError::UnknownLocation { .. })
        ));
    }

    #[test]
    fn ascii_slice_inside_a_multibyte_char_is_an_error() {
        let mut dir = header(1);
        dir.extend([1026, ASCII_PARAMS_TAG, 1, 0]);
        assert!(matches!(
            GeoKeyDirectory::parse(&dir, None, Some("é|")),
            Err(GeoKeyError::AsciiEncoding { .. })
        ));
    }
}
