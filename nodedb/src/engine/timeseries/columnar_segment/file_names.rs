// SPDX-License-Identifier: BUSL-1.1

//! File names for one partition's per-column files.
//!
//! A column name has no length bound that fits a path component, so each
//! column file is named by a hash of the name: the first 128 bits of SHA-256,
//! hex-encoded, plus `.col` for the data and `.sym` for a dictionary. The name
//! itself is stored in the partition's `schema.json`. The writer refuses a
//! partition in which two column names hash to one file name.

use nodedb_types::hashed_file_stem;

fn stem(col_name: &str) -> String {
    hashed_file_stem(col_name.as_bytes())
}

/// The data file of column `col_name`: `{hash}.col`.
pub fn column_file_name(col_name: &str) -> String {
    format!("{}.col", stem(col_name))
}

/// The dictionary file of column `col_name`: `{hash}.sym`.
pub fn symbol_file_name(col_name: &str) -> String {
    format!("{}.sym", stem(col_name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_fixed_length_and_plain() {
        let long = "c".repeat(1000);
        for name in ["value", "", "a/b", "../x", long.as_str()] {
            let col = column_file_name(name);
            assert_eq!(col.len(), 36);
            assert!(nodedb_types::is_plain_path_component(&col), "{col}");
            assert_eq!(symbol_file_name(name).len(), 36);
        }
    }

    #[test]
    fn distinct_names_get_distinct_files() {
        assert_ne!(column_file_name("value"), column_file_name("value2"));
        assert_ne!(column_file_name("value"), symbol_file_name("value"));
    }
}
