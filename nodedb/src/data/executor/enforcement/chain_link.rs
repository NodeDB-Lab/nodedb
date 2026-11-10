// SPDX-License-Identifier: BUSL-1.1

//! Writing a hash-chain link into the body a write stores.
//!
//! `build_stored_body` calls in here once every expression of the row is
//! evaluated. The link covers the row exactly as stored, and no expression is
//! evaluated a second time.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::enforcement::hash_chain::{self, ChainHead, GENESIS_HASH};
use crate::data::executor::handlers::point::apply_put::stored_body::{StoredBody, StoredBodyInput};
use crate::engine::document::store::CollectionConfig;
use crate::types::{DatabaseId, TenantId};

impl CoreLoop {
    /// `value` with the link after `head` written into its chain fields.
    ///
    /// `value` is the canonical body with every expression evaluated.
    /// `encode` turns a canonical body into stored bytes. The contents are
    /// `value` with a placeholder link, encoded, decoded under the storage
    /// mode, and stripped of the chain fields. The chain fields never reach the
    /// contents, so the placeholder yields the contents of the final body.
    pub(in crate::data::executor) fn link_value(
        &self,
        config: &CollectionConfig,
        value: &[u8],
        head: &ChainHead,
        encode: &dyn Fn(&[u8]) -> crate::Result<Vec<u8>>,
    ) -> crate::Result<Vec<u8>> {
        let mut doc = doc_format::decode_document_value(value)?;
        let placeholder = head.next(GENESIS_HASH.to_string());
        let probe = encode(&native_link_body(&mut doc, &placeholder)?)?;
        let view = self.decode_stored_document(config, &probe)?;
        let contents = hash_chain::canonical_contents(&view);
        let link = head.next(hash_chain::compute_chain_hash(
            &head.hash,
            placeholder.seq,
            &contents,
        ));
        native_link_body(&mut doc, &link)
    }

    /// The stored body of a row re-applied with its durable bytes `stored`.
    pub(in crate::data::executor) fn kept_stored_body(
        &self,
        config_key: &(DatabaseId, TenantId, String),
        stored: &[u8],
    ) -> crate::Result<StoredBody> {
        let config =
            self.doc_configs
                .get(config_key)
                .ok_or_else(|| crate::Error::CollectionNotFound {
                    tenant_id: config_key.1,
                    collection: config_key.2.clone(),
                })?;
        let view = self.decode_stored_document(config, stored)?;
        Ok(StoredBody {
            value: doc_format::encode_to_msgpack(&view),
            stored: stored.to_vec(),
        })
    }

    /// The canonical contents `value` stores under `surrogate`: built
    /// and encoded as a write builds it, decoded, without the chain fields.
    /// Reads only; nothing is written.
    pub(in crate::data::executor) fn stored_contents_of(
        &self,
        config_key: &(DatabaseId, TenantId, String),
        surrogate: nodedb_types::Surrogate,
        value: &[u8],
    ) -> crate::Result<Vec<u8>> {
        // The decoded view hides the bitemporal timestamps, so only the
        // encoding choice matters here, never the stamp values.
        let bitemporal = self
            .apply_scope
            .bitemporal_stamps
            .contains_key(&surrogate.as_u32())
            || self.is_bitemporal(config_key.0.as_u64(), config_key.1.as_u64(), &config_key.2);
        let stored = self
            .build_stored_body(StoredBodyInput {
                config_key,
                surrogate,
                value,
                bitemporal,
                sys_from: 0,
                valid_from_ms: i64::MIN,
                valid_until_ms: i64::MAX,
                // The contents a write stores, derived the same way; the write
                // itself judged the guards.
                guards: crate::data::executor::enforcement::typeguard::GuardScope::InjectOnly,
            })?
            .stored;
        let config =
            self.doc_configs
                .get(config_key)
                .ok_or_else(|| crate::Error::CollectionNotFound {
                    tenant_id: config_key.1,
                    collection: config_key.2.clone(),
                })?;
        let view = self.decode_stored_document(config, &stored)?;
        Ok(hash_chain::canonical_contents(&view))
    }
}

/// Inject chain metadata without converting user fields through JSON.
fn native_link_body(doc: &mut nodedb_types::Value, link: &ChainHead) -> crate::Result<Vec<u8>> {
    let nodedb_types::Value::Object(fields) = doc else {
        return Err(crate::Error::Serialization {
            format: "document".into(),
            detail: "hash-chain document body must be an object".into(),
        });
    };
    fields.insert(
        hash_chain::CHAIN_HASH_FIELD.into(),
        nodedb_types::Value::String(link.hash.clone()),
    );
    let seq = i64::try_from(link.seq).map_err(|error| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("hash-chain sequence {}: {error}", link.seq),
    })?;
    fields.insert(
        hash_chain::CHAIN_SEQ_FIELD.into(),
        nodedb_types::Value::Integer(seq),
    );
    zerompk::to_msgpack_vec(doc).map_err(|error| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: error.to_string(),
    })
}
