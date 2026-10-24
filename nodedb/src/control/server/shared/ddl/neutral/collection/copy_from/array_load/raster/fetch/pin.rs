// SPDX-License-Identifier: BUSL-1.1

//! `PinnedObject`: the version of one remote object a load reads. The first
//! answer pins the object's size and ETag. Every later answer must match,
//! so an object that changes mid-load fails with `55000` instead of mixing
//! two versions.

use std::sync::OnceLock;

use super::error::FetchError;

/// The size and ETag one answer reports for an object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectVersion {
    pub size: u64,
    pub etag: Option<String>,
}

/// The version of one object, pinned by the first answer.
#[derive(Debug, Default)]
pub struct PinnedObject {
    pinned: OnceLock<ObjectVersion>,
}

impl PinnedObject {
    /// Nothing pinned yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// The pinned version, once an answer pinned it.
    pub fn get(&self) -> Option<&ObjectVersion> {
        self.pinned.get()
    }

    /// The pinned size, once an answer pinned it.
    pub fn size(&self) -> Option<u64> {
        self.get().map(|v| v.size)
    }

    /// The pinned ETag, once an answer that carries one pinned it.
    pub fn etag(&self) -> Option<&str> {
        self.get().and_then(|v| v.etag.as_deref())
    }

    /// Pin `seen` when nothing is pinned, or check it against the pinned
    /// version. An answer without an ETag matches any ETag.
    pub fn pin(&self, source_name: &str, seen: ObjectVersion) -> Result<(), FetchError> {
        let pinned = self.pinned.get_or_init(|| seen.clone());
        let what = if pinned.size != seen.size {
            format!("its size went from {} to {} bytes", pinned.size, seen.size)
        } else {
            match (&pinned.etag, &seen.etag) {
                (Some(a), Some(b)) if a != b => "its ETag changed".to_string(),
                _ => return Ok(()),
            }
        };
        Err(FetchError::Changed {
            source_name: source_name.to_string(),
            what,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn version(size: u64, etag: Option<&str>) -> ObjectVersion {
        ObjectVersion {
            size,
            etag: etag.map(str::to_string),
        }
    }

    #[test]
    fn the_first_answer_pins_and_later_answers_must_match() {
        let pin = PinnedObject::new();
        assert_eq!(pin.size(), None);
        pin.pin("s", version(20, Some("\"v1\"")))
            .expect("first pins");
        assert_eq!(pin.size(), Some(20));
        assert_eq!(pin.etag(), Some("\"v1\""));
        pin.pin("s", version(20, Some("\"v1\"")))
            .expect("same version");
        pin.pin("s", version(20, None))
            .expect("no ETag matches any");
        let size = pin.pin("s", version(21, Some("\"v1\""))).expect_err("size");
        assert_eq!(size.sqlstate(), "55000");
        assert!(size.to_string().contains("20 to 21"), "{size}");
        let etag = pin.pin("s", version(20, Some("\"v2\""))).expect_err("ETag");
        assert!(etag.to_string().contains("ETag changed"), "{etag}");
    }
}
