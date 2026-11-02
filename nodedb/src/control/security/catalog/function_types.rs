// SPDX-License-Identifier: BUSL-1.1

//! Type definitions for user-defined function catalog storage.

use nodedb_types::id::DatabaseId;

use super::dependencies::Dependency;

/// Parameter definition for a user-defined function.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct FunctionParam {
    pub name: String,
    /// SQL type name: "TEXT", "INT", "FLOAT", "BOOLEAN", "BIGINT", "DOUBLE", etc.
    pub data_type: String,
}

/// Volatility classification for user-defined functions.
///
/// Maps directly to DataFusion's `Volatility` enum.
/// - `Immutable`: result depends only on inputs — the optimizer can constant-fold
///   calls where all arguments are literals.
/// - `Stable`: result may change across statements but not within one query
///   (e.g., depends on session state like `current_user()`).
/// - `Volatile`: result can change between any two calls — never cached
///   (e.g., depends on `random()` or `now()`).
///
/// The classification is set at `CREATE FUNCTION` time (auto-inferred as `Immutable`
/// by default, or explicitly overridden). It is stored in the catalog and used both
/// by the `ScalarUDF` registration (DataFusion respects it for constant folding) and
/// by the inlining `AnalyzerRule` (see `inline_rewrite.rs` for details).
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
#[repr(u8)]
#[msgpack(c_enum)]
pub enum FunctionVolatility {
    #[default]
    Immutable = 0,
    Stable = 1,
    Volatile = 2,
}

impl FunctionVolatility {
    /// Parse from SQL keyword (case-insensitive).
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "IMMUTABLE" => Some(Self::Immutable),
            "STABLE" => Some(Self::Stable),
            "VOLATILE" => Some(Self::Volatile),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Immutable => "IMMUTABLE",
            Self::Stable => "STABLE",
            Self::Volatile => "VOLATILE",
        }
    }
}

/// Security mode for user-defined functions.
///
/// - `Invoker` (default): body executes with the **caller's** credentials.
///   Subqueries are subject to the caller's GRANT/DENY and RLS policies.
/// - `Definer`: body executes with the **function owner's** credentials.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
#[repr(u8)]
#[msgpack(c_enum)]
pub enum FunctionSecurity {
    #[default]
    Invoker = 0,
    Definer = 1,
}

/// Function implementation language.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
#[repr(u8)]
#[msgpack(c_enum)]
pub enum FunctionLanguage {
    /// SQL expression or procedural SQL (default).
    #[default]
    Sql = 0,
    /// WebAssembly module.
    Wasm = 1,
}

impl FunctionLanguage {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sql => "SQL",
            Self::Wasm => "WASM",
        }
    }
}

/// Serializable user-defined function record for redb storage.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub struct StoredFunction {
    pub tenant_id: u64,
    /// Database namespace.
    pub database_id: DatabaseId,
    pub name: String,
    pub parameters: Vec<FunctionParam>,
    /// SQL return type name (e.g. "TEXT", "FLOAT", "BOOLEAN").
    pub return_type: String,
    /// Original SQL body (everything after AS in CREATE FUNCTION).
    /// For SQL functions: expression or procedural block.
    /// For WASM functions: empty (binary stored separately).
    pub body_sql: String,
    /// Compiled SQL expression for procedural bodies.
    pub compiled_body_sql: Option<String>,
    pub volatility: FunctionVolatility,
    /// Security mode: INVOKER (default) or DEFINER.
    pub security: FunctionSecurity,
    /// Implementation language: SQL (default) or WASM.
    pub language: FunctionLanguage,
    /// SHA-256 hash of the WASM binary (for WASM functions only).
    /// Used to look up the binary in the `_system.wasm_modules` table.
    pub wasm_hash: Option<String>,
    /// WASM bytes carried only in a replicated `PutFunction` proposal.
    /// Catalog persistence clears this field after the content-addressed blob
    /// has been installed locally by the entry applier.
    pub wasm_module: Option<Vec<u8>>,
    /// Complete dependency list carried with every replicated function
    /// definition.
    pub dependencies: Vec<Dependency>,
    /// Fuel budget for WASM functions.
    pub wasm_fuel: u64,
    /// Memory limit for WASM functions in bytes.
    pub wasm_memory: usize,
    pub owner: String,
    pub created_at: u64,
    /// Monotonic descriptor version, stamped by the metadata applier.
    pub descriptor_version: u64,
    /// HLC stamped by the metadata applier at commit time.
    pub modification_hlc: nodedb_types::Hlc,
}
