// SPDX-License-Identifier: Apache-2.0

//! CRS names: authority codes, OGC URNs and OGC URLs.

/// NodeDB code of the MODIS sinusoidal grid, SR-ORG:6842.
const MODIS_SINUSOIDAL_CODE: i64 = 990_001;

/// SR-ORG code of the MODIS sinusoidal grid.
const SR_ORG_MODIS: &str = "6842";

/// EPSG code of WGS 84, which OGC CRS84 maps to. CRS84 has longitude
/// first, the axis order NodeDB uses for every geographic CRS.
const WGS84_CODE: i64 = 4326;

/// Authority of NodeDB's own and user-defined codes.
pub const NODEDB_AUTHORITY: &str = "NODEDB";

/// OGC URN prefix, compared without case.
const URN_PREFIX: &str = "urn:ogc:def:crs:";

/// OGC URL prefixes, compared without case.
const URL_PREFIXES: [&str; 2] = [
    "http://www.opengis.net/def/crs/",
    "https://www.opengis.net/def/crs/",
];

/// The numeric code `name` refers to, before any registry check. `None`
/// when the name matches no supported form.
pub fn code_from_name(name: &str) -> Option<i64> {
    let name = name.trim();
    if let Some(rest) = strip_prefix_ci(name, URN_PREFIX) {
        // AUTHORITY:[version]:code
        let mut parts = rest.split(':');
        let authority = parts.next()?;
        let _version = parts.next()?;
        let code = parts.next()?;
        if parts.next().is_some() {
            return None;
        }
        return authority_code(authority, code);
    }
    if let Some(rest) = URL_PREFIXES.iter().find_map(|p| strip_prefix_ci(name, p)) {
        // AUTHORITY/version/code
        let mut parts = rest.split('/');
        let authority = parts.next()?;
        let _version = parts.next()?;
        let code = parts.next()?;
        if parts.next().is_some_and(|tail| !tail.is_empty()) {
            return None;
        }
        return authority_code(authority, code);
    }
    if name.eq_ignore_ascii_case("CRS84") {
        return Some(WGS84_CODE);
    }
    let (authority, code) = name.split_once(':')?;
    authority_code(authority, code)
}

/// Map an authority and its code to a NodeDB code.
fn authority_code(authority: &str, code: &str) -> Option<i64> {
    if authority.eq_ignore_ascii_case("EPSG") || authority.eq_ignore_ascii_case(NODEDB_AUTHORITY) {
        decimal(code)
    } else if authority.eq_ignore_ascii_case("OGC") {
        code.eq_ignore_ascii_case("CRS84").then_some(WGS84_CODE)
    } else if authority.eq_ignore_ascii_case("SR-ORG") {
        (code == SR_ORG_MODIS).then_some(MODIS_SINUSOIDAL_CODE)
    } else {
        None
    }
}

/// A non-empty run of ASCII digits as an integer.
fn decimal(text: &str) -> Option<i64> {
    if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

/// `text` without `prefix`, compared without ASCII case.
fn strip_prefix_ci<'a>(text: &'a str, prefix: &str) -> Option<&'a str> {
    let head = text.get(..prefix.len())?;
    head.eq_ignore_ascii_case(prefix)
        .then(|| &text[prefix.len()..])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epsg_forms() {
        assert_eq!(code_from_name("EPSG:32648"), Some(32648));
        assert_eq!(code_from_name("epsg:4326"), Some(4326));
        assert_eq!(code_from_name("urn:ogc:def:crs:EPSG::3375"), Some(3375));
        assert_eq!(
            code_from_name("urn:ogc:def:crs:EPSG:9.8.1:3376"),
            Some(3376)
        );
        assert_eq!(
            code_from_name("http://www.opengis.net/def/crs/EPSG/0/4742"),
            Some(4742)
        );
        assert_eq!(
            code_from_name("https://www.opengis.net/def/crs/EPSG/0/4742"),
            Some(4742)
        );
    }

    #[test]
    fn nodedb_forms() {
        assert_eq!(code_from_name("NODEDB:910001"), Some(910_001));
        assert_eq!(code_from_name("nodedb:990001"), Some(990_001));
        assert_eq!(
            code_from_name("urn:ogc:def:crs:NODEDB::910002"),
            Some(910_002)
        );
    }

    #[test]
    fn crs84_forms() {
        for name in [
            "urn:ogc:def:crs:OGC:1.3:CRS84",
            "urn:ogc:def:crs:OGC::CRS84",
            "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
            "OGC:CRS84",
            "CRS84",
        ] {
            assert_eq!(code_from_name(name), Some(4326), "{name}");
        }
    }

    #[test]
    fn sr_org_modis() {
        assert_eq!(code_from_name("SR-ORG:6842"), Some(990_001));
        assert_eq!(code_from_name("SR-ORG:1"), None);
    }

    #[test]
    fn refuses_other_forms() {
        for name in [
            "",
            "4326",
            "EPSG:",
            "EPSG:-1",
            "EPSG:4326x",
            "ESRI:102100",
            "urn:ogc:def:crs:EPSG:4326",
        ] {
            assert_eq!(code_from_name(name), None, "{name}");
        }
    }
}
