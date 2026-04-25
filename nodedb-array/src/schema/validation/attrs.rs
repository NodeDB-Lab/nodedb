//! Attribute-list validation.
//!
//! Rules:
//! - At least one attribute.
//! - Attribute names are unique.
//! - Attribute names do not collide with dimension names — but that
//!   cross-check happens at the schema level (the builder calls
//!   [`super::dims::check`] first, then this).

use std::collections::HashSet;

use crate::error::{ArrayError, ArrayResult};
use crate::schema::attr_spec::AttrSpec;

pub fn check(array: &str, attrs: &[AttrSpec]) -> ArrayResult<()> {
    if attrs.is_empty() {
        return Err(ArrayError::InvalidSchema {
            array: array.to_string(),
            detail: "at least one attribute is required".to_string(),
        });
    }
    let mut seen = HashSet::with_capacity(attrs.len());
    for a in attrs {
        if !seen.insert(a.name.as_str()) {
            return Err(ArrayError::InvalidAttr {
                array: array.to_string(),
                attr: a.name.clone(),
                detail: "duplicate attribute name".to_string(),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::attr_spec::AttrType;

    #[test]
    fn rejects_empty_attr_list() {
        assert!(check("a", &[]).is_err());
    }

    #[test]
    fn rejects_duplicate_attr_names() {
        let attrs = vec![
            AttrSpec::new("v", AttrType::Int64, false),
            AttrSpec::new("v", AttrType::Int64, false),
        ];
        assert!(check("a", &attrs).is_err());
    }

    #[test]
    fn accepts_well_formed_attrs() {
        let attrs = vec![
            AttrSpec::new("variant", AttrType::String, false),
            AttrSpec::new("qual", AttrType::Float64, true),
        ];
        assert!(check("a", &attrs).is_ok());
    }
}
