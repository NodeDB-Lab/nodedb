// SPDX-License-Identifier: Apache-2.0

//! The sources of `COPY ... FROM`: a server file, an HTTP(S) URL, or an S3
//! object.

/// One source a `COPY ... FROM` reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopySource {
    /// A server file path.
    File(String),
    /// An `http://` or `https://` URL, read with byte-range requests.
    Http(String),
    /// An `s3://bucket/key` object.
    S3 { bucket: String, key: String },
}

/// Why a source string names no source.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CopySourceError {
    #[error("source must not be empty")]
    Empty,
    #[error("'{0}' names no bucket and key; write s3://<bucket>/<key>")]
    S3Shape(String),
    #[error("'{0}' uses an unsupported scheme; use a server path, https://, http:// or s3://")]
    Scheme(String),
}

impl CopySource {
    /// The source `text` names: `s3://bucket/key`, an `http(s)://` URL, or
    /// a server path.
    pub fn parse(text: &str) -> Result<Self, CopySourceError> {
        if text.is_empty() {
            return Err(CopySourceError::Empty);
        }
        let lower = text.to_ascii_lowercase();
        if lower.starts_with("https://") || lower.starts_with("http://") {
            return Ok(Self::Http(text.to_string()));
        }
        if lower.starts_with("s3://") {
            let rest = &text["s3://".len()..];
            return match rest.split_once('/') {
                Some((bucket, key)) if !bucket.is_empty() && !key.is_empty() => Ok(Self::S3 {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                }),
                _ => Err(CopySourceError::S3Shape(text.to_string())),
            };
        }
        if let Some((scheme, _)) = text.split_once("://")
            && !scheme.is_empty()
            && scheme
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '+')
        {
            return Err(CopySourceError::Scheme(text.to_string()));
        }
        Ok(Self::File(text.to_string()))
    }

    /// The source as the statement wrote it, for messages.
    pub fn display(&self) -> String {
        match self {
            Self::File(path) => path.clone(),
            Self::Http(url) => url.clone(),
            Self::S3 { bucket, key } => format!("s3://{bucket}/{key}"),
        }
    }

    /// The path part a format is detected from: the file path, the URL path
    /// without its query, or the object key.
    pub fn format_hint(&self) -> &str {
        match self {
            Self::File(path) => path,
            Self::Http(url) => {
                let end = url.find(['?', '#']).unwrap_or(url.len());
                &url[..end]
            }
            Self::S3 { key, .. } => key,
        }
    }
}

/// `WINDOW (x0, y0, x1, y1)`: the pixel columns `x0..x1` and rows `y0..y1`
/// of the image, half-open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CopyWindow {
    pub x0: u32,
    pub y0: u32,
    pub x1: u32,
    pub y1: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_scheme_names_its_source() {
        assert_eq!(
            CopySource::parse("/data/a.tif"),
            Ok(CopySource::File("/data/a.tif".into()))
        );
        assert_eq!(
            CopySource::parse("HTTPS://h.example/a.tif?x=1"),
            Ok(CopySource::Http("HTTPS://h.example/a.tif?x=1".into()))
        );
        assert_eq!(
            CopySource::parse("s3://bucket/dir/a.tif"),
            Ok(CopySource::S3 {
                bucket: "bucket".into(),
                key: "dir/a.tif".into()
            })
        );
        assert!(matches!(
            CopySource::parse("s3://bucket"),
            Err(CopySourceError::S3Shape(_))
        ));
        assert!(matches!(
            CopySource::parse("ftp://h/a.tif"),
            Err(CopySourceError::Scheme(_))
        ));
        assert_eq!(CopySource::parse(""), Err(CopySourceError::Empty));
    }

    #[test]
    fn the_format_hint_drops_the_query() {
        let url = CopySource::Http("https://h.example/a.TIF?sig=1".into());
        assert_eq!(url.format_hint(), "https://h.example/a.TIF");
        let s3 = CopySource::S3 {
            bucket: "b".into(),
            key: "k.tif".into(),
        };
        assert_eq!(s3.display(), "s3://b/k.tif");
        assert_eq!(s3.format_hint(), "k.tif");
    }
}
