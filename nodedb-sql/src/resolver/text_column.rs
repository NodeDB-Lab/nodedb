// SPDX-License-Identifier: Apache-2.0

//! Whether a column can hold the text a full-text search reads.

use nodedb_types::text_search::TextColumnFault;

use super::columns::ResolvedTable;
use crate::error::{Result, SqlError};
use crate::types::SqlDataType;

impl ResolvedTable {
    /// Accept `column` as the field of `function(column, q)`: a declared
    /// string column, or any name on an open-schema collection.
    pub fn check_text_column(&self, function: &str, column: &str) -> Result<()> {
        let fault = match self.info.columns.iter().find(|c| c.name == column) {
            Some(c) if c.data_type == SqlDataType::String => return Ok(()),
            Some(c) => TextColumnFault::NotText {
                data_type: c
                    .raw_type
                    .clone()
                    .unwrap_or_else(|| format!("{:?}", c.data_type)),
            },
            None if self.info.open_schema => return Ok(()),
            None => TextColumnFault::Undeclared,
        };
        Err(SqlError::TextColumn {
            function: function.to_owned(),
            collection: self.name.clone(),
            column: column.to_owned(),
            fault,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{CollectionInfo, ColumnInfo, EngineType};

    fn column(name: &str, data_type: SqlDataType, raw: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.into(),
            data_type,
            nullable: true,
            is_primary_key: false,
            default: None,
            raw_type: Some(raw.into()),
            int_width: None,
            float_width: None,
        }
    }

    fn strict() -> ResolvedTable {
        let info = CollectionInfo {
            name: "articles".into(),
            engine: EngineType::DocumentStrict,
            columns: vec![
                column("title", SqlDataType::String, "TEXT"),
                column("views", SqlDataType::Int64, "INT"),
            ],
            primary_key: None,
            has_auto_tier: false,
            indexes: Vec::new(),
            bitemporal: false,
            primary: nodedb_types::PrimaryEngine::Document,
            vector_primary: None,
            partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
            open_schema: false,
        };
        ResolvedTable {
            name: "articles".into(),
            alias: None,
            info,
        }
    }

    #[test]
    fn a_text_column_is_accepted() {
        assert_eq!(strict().check_text_column("text_match", "title"), Ok(()));
    }

    #[test]
    fn an_int_column_is_not_text() {
        let err = strict().check_text_column("bm25_score", "views");
        assert_eq!(
            err,
            Err(SqlError::TextColumn {
                function: "bm25_score".into(),
                collection: "articles".into(),
                column: "views".into(),
                fault: TextColumnFault::NotText {
                    data_type: "INT".into()
                },
            })
        );
    }

    #[test]
    fn an_undeclared_column_names_the_collection() {
        let Err(SqlError::TextColumn {
            collection, fault, ..
        }) = strict().check_text_column("text_match", "ghost")
        else {
            panic!("expected a TextColumn error");
        };
        assert_eq!(collection, "articles");
        assert_eq!(fault, TextColumnFault::Undeclared);
    }

    #[test]
    fn an_open_schema_collection_accepts_any_name() {
        let mut table = strict();
        table.info.open_schema = true;
        assert_eq!(table.check_text_column("text_match", "ghost"), Ok(()));
    }
}
