// SPDX-License-Identifier: BUSL-1.1

//! The stable wire name of each [`DdlColType`], for a value that travels
//! between nodes: a `RETURNING` contract's announced columns, and the column
//! types of a frozen reply.

use super::shaped::DdlColType;

impl DdlColType {
    /// The type's wire name.
    pub fn wire_name(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Int8 => "int8",
            Self::Int4 => "int4",
            Self::Int2 => "int2",
            Self::Float8 => "float8",
            Self::Float4 => "float4",
            Self::Bool => "bool",
            Self::Bytea => "bytea",
            Self::Json => "json",
            Self::Jsonb => "jsonb",
            Self::Timestamp => "timestamp",
            Self::Timestamptz => "timestamptz",
            Self::Varchar => "varchar",
            Self::Float4Array => "float4[]",
            Self::Float8Array => "float8[]",
            Self::Numeric => "numeric",
            Self::Uuid => "uuid",
        }
    }

    /// The type a wire name names. `None` for a name no type has.
    pub fn from_wire_name(name: &str) -> Option<Self> {
        const ALL: [DdlColType; 17] = [
            DdlColType::Text,
            DdlColType::Int8,
            DdlColType::Int4,
            DdlColType::Int2,
            DdlColType::Float8,
            DdlColType::Float4,
            DdlColType::Bool,
            DdlColType::Bytea,
            DdlColType::Json,
            DdlColType::Jsonb,
            DdlColType::Timestamp,
            DdlColType::Timestamptz,
            DdlColType::Varchar,
            DdlColType::Float4Array,
            DdlColType::Float8Array,
            DdlColType::Numeric,
            DdlColType::Uuid,
        ];
        ALL.into_iter().find(|ty| ty.wire_name() == name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_wire_name_reads_back_as_its_type() {
        for ty in [
            DdlColType::Text,
            DdlColType::Int8,
            DdlColType::Timestamptz,
            DdlColType::Float4Array,
            DdlColType::Uuid,
        ] {
            assert_eq!(DdlColType::from_wire_name(ty.wire_name()), Some(ty));
        }
        assert_eq!(DdlColType::from_wire_name("int16"), None);
    }
}
