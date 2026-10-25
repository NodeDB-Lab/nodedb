// SPDX-License-Identifier: Apache-2.0

//! The `GDAL_METADATA` tag (42112): GDAL's XML band metadata.
//!
//! ```xml
//! <GDALMetadata>
//!   <Item name="SCALE" sample="0" role="scale">0.0000275</Item>
//!   <Item name="OFFSET" sample="0" role="offset">-0.2</Item>
//!   <Item name="DESCRIPTION" sample="0" role="description">red</Item>
//! </GDALMetadata>
//! ```
//!
//! Only per-band items (those with a `sample` attribute) are read: name
//! (`description`), `scale`, `offset`, `unittype`, a nodata item
//! (`NODATA` or `_FillValue`), and `valid_range` / `valid_min` /
//! `valid_max`. Other items are ignored.

use std::collections::BTreeMap;

use crate::error::RasterError;
use crate::nodata::parse_gdal_nodata;

/// The metadata of one band.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BandMetadata {
    pub name: Option<String>,
    pub scale: Option<f64>,
    pub offset: Option<f64>,
    pub units: Option<String>,
    pub nodata: Option<f64>,
    pub valid_min: Option<f64>,
    pub valid_max: Option<f64>,
}

impl BandMetadata {
    /// The valid range, when both ends are known.
    pub fn valid_range(&self) -> Option<(f64, f64)> {
        self.valid_min.zip(self.valid_max)
    }
}

/// One parsed `<Item>`.
#[derive(Debug, Default)]
struct Item {
    name: String,
    sample: Option<String>,
    role: Option<String>,
    text: String,
}

/// Per-band metadata by zero-based band index.
pub fn parse_gdal_metadata(xml: &str) -> Result<BTreeMap<u32, BandMetadata>, RasterError> {
    let mut bands: BTreeMap<u32, BandMetadata> = BTreeMap::new();
    let mut rest = xml;
    while let Some(start) = rest.find("<Item") {
        let after = rest.get(start + "<Item".len()..).unwrap_or_default();
        let Some((item, tail)) = parse_item(after) else {
            break;
        };
        rest = tail;
        let Some(sample) = &item.sample else {
            continue;
        };
        let band = sample
            .trim()
            .parse::<u32>()
            .map_err(|_| RasterError::InvalidMetadata {
                name: "sample".to_owned(),
                text: sample.clone(),
            })?;
        apply(bands.entry(band).or_default(), &item)?;
    }
    Ok(bands)
}

/// Fold one item into its band.
fn apply(band: &mut BandMetadata, item: &Item) -> Result<(), RasterError> {
    let role = item
        .role
        .as_deref()
        .unwrap_or_default()
        .to_ascii_lowercase();
    let name = item.name.to_ascii_lowercase();
    let text = item.text.trim();
    let number = || {
        text.parse::<f64>()
            .map_err(|_| RasterError::InvalidMetadata {
                name: item.name.clone(),
                text: text.to_owned(),
            })
    };
    match (role.as_str(), name.as_str()) {
        ("description", _) | (_, "description") => band.name = Some(text.to_owned()),
        ("scale", _) | ("", "scale") => band.scale = Some(number()?),
        ("offset", _) | ("", "offset") => band.offset = Some(number()?),
        ("unittype", _) | ("", "units") => band.units = Some(text.to_owned()),
        (_, "nodata" | "_fillvalue") => band.nodata = Some(parse_gdal_nodata(text)?),
        (_, "valid_min") => band.valid_min = Some(number()?),
        (_, "valid_max") => band.valid_max = Some(number()?),
        (_, "valid_range") => {
            let (min, max) = parse_range(text).ok_or(RasterError::InvalidMetadata {
                name: item.name.clone(),
                text: text.to_owned(),
            })?;
            band.valid_min = Some(min);
            band.valid_max = Some(max);
        }
        _ => {}
    }
    Ok(())
}

/// Two numbers, comma or space separated, optionally in braces.
fn parse_range(text: &str) -> Option<(f64, f64)> {
    let inner = text
        .trim()
        .trim_start_matches(['{', '[', '('])
        .trim_end_matches(['}', ']', ')']);
    let mut numbers = inner
        .split(|c: char| c == ',' || c.is_whitespace())
        .filter(|part| !part.is_empty())
        .map(str::parse::<f64>);
    let min = numbers.next()?.ok()?;
    let max = numbers.next()?.ok()?;
    numbers.next().is_none().then_some((min, max))
}

/// Parse the attributes and text of an item whose `<Item` is consumed.
/// Returns the item and the input after it.
fn parse_item(input: &str) -> Option<(Item, &str)> {
    let close = input.find('>')?;
    let (attrs, after) = (input.get(..close)?, input.get(close + 1..)?);
    let mut item = Item::default();
    let self_closing = attrs.trim_end().ends_with('/');
    let mut rest = attrs.trim_end_matches('/');
    while let Some(eq) = rest.find('=') {
        let key = rest.get(..eq)?.trim();
        let value_part = rest.get(eq + 1..)?.trim_start();
        let quote = value_part.chars().next()?;
        if quote != '"' && quote != '\'' {
            return None;
        }
        let body = value_part.get(1..)?;
        let end = body.find(quote)?;
        let value = unescape(body.get(..end)?);
        match key {
            "name" => item.name = value,
            "sample" => item.sample = Some(value),
            "role" => item.role = Some(value),
            _ => {}
        }
        rest = body.get(end + 1..)?;
    }
    if self_closing {
        return Some((item, after));
    }
    let end = after.find("</Item>")?;
    item.text = unescape(after.get(..end)?);
    Some((item, after.get(end + "</Item>".len()..)?))
}

/// Replace the five predefined XML entities.
fn unescape(text: &str) -> String {
    text.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

#[cfg(test)]
mod tests {
    use super::*;

    const XML: &str = r#"<GDALMetadata>
  <Item name="AREA_OR_POINT">Area</Item>
  <Item name="SCALE" sample="0" role="scale">2.75e-05</Item>
  <Item name="OFFSET" sample="0" role="offset">-0.2</Item>
  <Item name="DESCRIPTION" sample="0" role="description">red &amp; nir</Item>
  <Item name="UNITTYPE" sample="1" role="unittype">K</Item>
  <Item name="NODATA" sample="1">-9999</Item>
  <Item name="valid_range" sample="1">{1, 65455}</Item>
  <Item name="empty" sample="2" />
</GDALMetadata>"#;

    #[test]
    fn per_band_items_parse() {
        let bands = parse_gdal_metadata(XML).expect("parses");
        let red = &bands[&0];
        assert_eq!(red.scale, Some(2.75e-05));
        assert_eq!(red.offset, Some(-0.2));
        assert_eq!(red.name.as_deref(), Some("red & nir"));
        let second = &bands[&1];
        assert_eq!(second.units.as_deref(), Some("K"));
        assert_eq!(second.nodata, Some(-9999.0));
        assert_eq!(second.valid_range(), Some((1.0, 65455.0)));
        assert!(bands.contains_key(&2));
        assert_eq!(bands.len(), 3, "dataset items are ignored");
    }

    #[test]
    fn bad_numbers_are_typed_errors() {
        let bad = r#"<Item name="SCALE" sample="0" role="scale">wide</Item>"#;
        assert!(matches!(
            parse_gdal_metadata(bad),
            Err(RasterError::InvalidMetadata { .. })
        ));
        let bad_band = r#"<Item name="SCALE" sample="x" role="scale">1</Item>"#;
        assert!(matches!(
            parse_gdal_metadata(bad_band),
            Err(RasterError::InvalidMetadata { .. })
        ));
    }

    #[test]
    fn truncated_xml_stops_without_error() {
        assert!(parse_gdal_metadata(r#"<Item name="SCALE" sample="0""#).is_ok());
        assert!(parse_gdal_metadata("").is_ok_and(|bands| bands.is_empty()));
        assert_eq!(parse_range("3 4"), Some((3.0, 4.0)));
        assert_eq!(parse_range("3"), None);
    }
}
