// SPDX-License-Identifier: BUSL-1.1

//! Which encoding a collection's sparse-store rows use, resolved from the
//! collection's registered kind.
//!
//! Three encodings share the sparse store: schemaless document bodies (standard
//! MessagePack), strict document bodies (Binary Tuples), and vector-primary
//! metadata sidecars (`zerompk` TAGGED `HashMap<String, Value>`). A tagged map
//! and a plain document map are both valid MessagePack maps and begin with the
//! same map header, so no inspection of the stored bytes can separate them: a
//! reader that sniffs necessarily mis-decodes one of them, and returns
//! `[4,"alice"]` where the client asked for `alice`.
//!
//! So the decision is made once, here, from `doc_configs` — the registry the
//! DDL register broadcast and the boot seed both populate — and every reader
//! of a sparse body takes the answer as a parameter.

use nodedb_physical::physical_plan::StorageMode;

use super::core_loop::CoreLoop;
use crate::types::{DatabaseId, TenantId};

/// How the bytes of a sparse-store row are encoded. See the module docs for
/// why this is never derived from the bytes themselves.
pub(in crate::data::executor) enum SparseBodyFormat {
    /// Schemaless document body: standard msgpack, or JSON that the
    /// normalizer transcodes.
    Document,
    /// Strict document body: a Binary Tuple decoded against this schema.
    Strict(nodedb_types::columnar::StrictSchema),
    /// Vector-primary metadata sidecar: `zerompk` TAGGED
    /// `HashMap<String, Value>`, written verbatim by the vector upsert handler.
    VectorSidecar,
}

/// A borrowed [`SparseBodyFormat`], the form every row converter takes.
///
/// The owned enum has to carry a `StrictSchema` because it is resolved once per
/// query out of the registry, but a per-row converter only ever reads that
/// schema. Passing the owned enum down would make each call site that has a
/// bare `&StrictSchema` — a scan stage that already resolved one, an audit read
/// that carries it as `Option<&StrictSchema>` — clone a whole schema per row, or
/// (worse) re-implement the Document/Strict branch locally to dodge the clone.
/// The borrowed view removes both, so there stays exactly ONE implementation of
/// the three-way decision.
#[derive(Clone, Copy)]
pub(in crate::data::executor) enum SparseBodyFormatRef<'a> {
    /// See [`SparseBodyFormat::Document`].
    Document,
    /// See [`SparseBodyFormat::Strict`].
    Strict(&'a nodedb_types::columnar::StrictSchema),
    /// See [`SparseBodyFormat::VectorSidecar`].
    VectorSidecar,
}

impl SparseBodyFormat {
    /// Borrow this format for handing to a row converter.
    pub(in crate::data::executor) fn as_format_ref(&self) -> SparseBodyFormatRef<'_> {
        match self {
            SparseBodyFormat::Document => SparseBodyFormatRef::Document,
            SparseBodyFormat::Strict(schema) => SparseBodyFormatRef::Strict(schema),
            SparseBodyFormat::VectorSidecar => SparseBodyFormatRef::VectorSidecar,
        }
    }
}

impl<'a> SparseBodyFormatRef<'a> {
    /// The format of a document body whose only open question is whether a
    /// strict schema applies.
    ///
    /// Read stages that have already excluded the sidecar encoding — an audit
    /// or `AS OF` read, or a scan whose fetch stage reports the schema of the
    /// bodies it produced — hold exactly an `Option<&StrictSchema>`. This maps
    /// it, so those stages state their input rather than re-deciding how a
    /// Binary Tuple or a schemaless body is decoded.
    pub(in crate::data::executor) fn from_schema(
        schema: Option<&'a nodedb_types::columnar::StrictSchema>,
    ) -> Self {
        match schema {
            Some(schema) => SparseBodyFormatRef::Strict(schema),
            None => SparseBodyFormatRef::Document,
        }
    }
}

impl CoreLoop {
    /// Resolve the sparse-body encoding a collection's rows use.
    ///
    /// Vector-primary is checked first: such a collection also carries a
    /// storage mode (its sidecar rows are not strict tuples), and the sidecar
    /// encoding is the one that actually describes the bytes on disk.
    ///
    /// An unregistered collection resolves to `Document`, which is what the
    /// read path did before any of these markers existed.
    pub(in crate::data::executor) fn sparse_body_format(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
    ) -> SparseBodyFormat {
        let key = (database_id, tenant_id, collection.to_string());
        let Some(config) = self.doc_configs.get(&key) else {
            return SparseBodyFormat::Document;
        };
        if config.vector_primary.is_some() {
            return SparseBodyFormat::VectorSidecar;
        }
        match config.storage_mode {
            StorageMode::Strict { ref schema } => SparseBodyFormat::Strict(schema.clone()),
            StorageMode::Schemaless => SparseBodyFormat::Document,
        }
    }
}
