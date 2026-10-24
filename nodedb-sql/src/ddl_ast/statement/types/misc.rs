// SPDX-License-Identifier: Apache-2.0

//! Miscellaneous DDL/DML statements.

use crate::ddl_ast::statement::copy_source::CopySource;
use crate::ddl_ast::statement::credential::CredentialSpec;
use crate::ddl_ast::statement::maintenance::{
    CopyFormat, CopyRasterOptions, CopyStdinFormat, CopyToSource,
};

/// The text form of a `CREATE SPATIAL_REF_SYS` definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrsTextFormat {
    /// A PROJ string: `PROJ '+proj=...'`.
    Proj,
    /// PROJJSON: `PROJJSON '{...}'`.
    ProjJson,
    /// WKT2: `WKT2 'PROJCRS[...]'`.
    Wkt2,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MiscStmt {
    // ── Miscellaneous ────────────────────────────────────────────
    ShowAuditLog,
    ShowConstraints {
        collection: String,
    },
    ShowTypeGuards {
        collection: String,
    },

    // ── User-defined CRSs ────────────────────────────────────────
    /// `CREATE SPATIAL_REF_SYS <code> (NAME '<name>', PROJ|PROJJSON|WKT2 '<text>')`
    CreateSpatialRefSys {
        srid: u64,
        name: String,
        format: CrsTextFormat,
        definition: String,
    },
    /// `DROP SPATIAL_REF_SYS <code>`
    DropSpatialRefSys {
        srid: u64,
    },

    // ── Credentials ──────────────────────────────────────────────
    /// `CREATE CREDENTIAL <name> (TYPE 's3' | 'bearer' | 'sas', ...)`
    CreateCredential {
        name: String,
        spec: CredentialSpec,
    },
    /// `DROP CREDENTIAL [IF EXISTS] <name>`
    DropCredential {
        name: String,
        if_exists: bool,
    },
    /// `SHOW CREDENTIALS`: names and types, never a secret.
    ShowCredentials,

    // ── Bulk import ──────────────────────────────────────────────
    /// `COPY <collection> FROM '<path>' [WITH (FORMAT ..., DELIMITER ..., HEADER ...)]`
    /// `COPY <array> FROM '<source>' | ('<source>', ...) WITH (FORMAT geotiff, ...)`
    ///
    /// Bulk import from a server file, or for a raster from HTTP(S) or S3
    /// sources too. `CopyFromStdin` is the streaming form, and `CopyToFile`
    /// the export.
    CopyFromFile {
        collection: String,
        /// One source, or one per band group of a multi-source raster.
        sources: Vec<CopySource>,
        format: Option<CopyFormat>,
        delimiter: Option<char>,
        header: bool,
        /// GeoTIFF options. Empty unless `format` is `GeoTiff`.
        raster: CopyRasterOptions,
    },

    /// `COPY <name> FROM STDIN [WITH (FORMAT csv|text, DELIMITER '<c>', HEADER [bool])]`
    ///
    /// Streaming bulk import: the client sends the rows over the pgwire
    /// COPY protocol. `header` defaults to false, as in PostgreSQL.
    CopyFromStdin {
        collection: String,
        format: CopyStdinFormat,
        delimiter: Option<char>,
        header: bool,
    },

    // ── Bulk export ──────────────────────────────────────────────
    /// `COPY <collection> TO '<path>' [WITH (FORMAT ..., DELIMITER ..., HEADER ...)]`
    /// `COPY (SELECT ...) TO '<path>' [WITH (...)]`
    ///
    /// Server-side file-path bulk export. Streams scan results to a file.
    CopyToFile {
        /// The source: either a bare collection name or a SELECT query.
        source: CopyToSource,
        path: String,
        format: Option<CopyFormat>,
        delimiter: Option<char>,
        header: bool,
    },
}
