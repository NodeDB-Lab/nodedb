// SPDX-License-Identifier: BUSL-1.1

//! The config pins of a committed redo, as a core receives them.
//!
//! - [`judged_config_from_pin`] decodes one pin, with no catalog, into the
//!   config a validate judges under.
//! - [`check_written_pinned`] refuses a pinned record that writes a
//!   document or KV collection it carries no pin of.
//!
//! Every replica holds the same pin bytes, so each refusal here is final.

use nodedb_physical::physical_plan::{
    ConfigPin, CrsPin, DocConfigWire, PinnedWrite, RedoPins, RedoSumTargets,
};
use nodedb_types::{CollectionKey, DatabaseId, QualifiedCollection, TenantId};

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::handlers::document::write::WireConfigFields;
use crate::engine::document::store::CollectionConfig;

/// A config pin a core cannot judge under.
#[derive(Debug, thiserror::Error)]
pub(in crate::data::executor) enum ConfigPinError {
    #[error("the config pin of {collection} does not decode: {detail}")]
    Undecodable { collection: String, detail: String },
    #[error(
        "the config pin of {collection} carries digest {pinned}, but its wire bytes hash to \
         {derived}"
    )]
    DigestMismatch {
        collection: String,
        pinned: u64,
        derived: u64,
    },
    #[error("the pinned committed redo writes {collection} but carries no config pin of it")]
    Unpinned { collection: String },
    #[error("the CRS pin does not decode: {detail}")]
    CrsUndecodable { detail: String },
    #[error("the CRS pin carries digest {pinned}, but its bytes hash to {derived}")]
    CrsDigestMismatch { pinned: u64, derived: u64 },
}

impl From<ConfigPinError> for ErrorCode {
    fn from(e: ConfigPinError) -> Self {
        Self::RejectedPrevalidation {
            reason: e.to_string(),
        }
    }
}

impl From<ConfigPinError> for crate::Error {
    fn from(e: ConfigPinError) -> Self {
        Self::RejectedPrevalidation {
            constraint: "config_pin".into(),
            reason: e.to_string(),
        }
    }
}

/// Refuse `pin` unless `derived`, the hash of its raw wire bytes, is its
/// digest. Every path checks a config pin by this one rule, before any
/// decode. The proposer pins canonical bytes, so a pin that passes holds
/// them.
pub(in crate::data::executor) fn check_config_pin_hash(
    pin: &ConfigPin,
    derived: u64,
) -> Result<(), ConfigPinError> {
    if derived == pin.digest {
        return Ok(());
    }
    Err(ConfigPinError::DigestMismatch {
        collection: pin.key.collection.clone(),
        pinned: pin.digest,
        derived,
    })
}

/// Refuse a CRS `pin` unless `derived`, the hash of its raw bytes, is its
/// digest: the CRS form of [`check_config_pin_hash`].
pub(in crate::data::executor) fn check_crs_pin_hash(
    pin: &CrsPin,
    derived: u64,
) -> Result<(), ConfigPinError> {
    if derived == pin.digest {
        return Ok(());
    }
    Err(ConfigPinError::CrsDigestMismatch {
        pinned: pin.digest,
        derived,
    })
}

/// The config `pin` fixes. `derived` is the hash of its raw wire bytes,
/// checked by [`check_config_pin_hash`] before the decode. The config
/// starts from `current`, the core's entry for the pin's key, or a fresh
/// config when the core holds none. The wire form's fields and the pin's
/// digest overwrite it.
pub(in crate::data::executor) fn judged_config_from_pin(
    current: Option<&CollectionConfig>,
    pin: &ConfigPin,
    derived: u64,
) -> Result<CollectionConfig, ConfigPinError> {
    check_config_pin_hash(pin, derived)?;
    let collection = pin.key.collection.as_str();
    let undecodable = |detail: String| ConfigPinError::Undecodable {
        collection: collection.to_string(),
        detail,
    };
    let wire: DocConfigWire =
        zerompk::from_msgpack(pin.wire.as_slice()).map_err(|e| undecodable(e.to_string()))?;
    // A type guard violation names the collection the user wrote.
    let key = CollectionKey::from_qualified_str(pin.key.database_id, collection)
        .map_err(|e| undecodable(e.to_string()))?;
    let mut config = current
        .cloned()
        .unwrap_or_else(|| CollectionConfig::new(collection));
    WireConfigFields::from_wire(wire, key.name()).overwrite(&mut config);
    config.digest = pin.digest;
    Ok(config)
}

/// Refuse a pinned record that makes one of `writes`, each a write kind
/// and a database-qualified collection, or folds into a target
/// `sum_targets` resolves, with no pin of it. A record with no pins passes.
/// `PinnedWrite` names what requires a pin, as the proposer reads it.
pub(in crate::data::executor) fn check_written_pinned<'a>(
    pins: &RedoPins,
    database_id: DatabaseId,
    tenant_id: TenantId,
    writes: impl IntoIterator<Item = (PinnedWrite, &'a str)>,
    sum_targets: &[RedoSumTargets],
) -> Result<(), ConfigPinError> {
    if !pins.pinned() {
        return Ok(());
    }
    let unpinned = |collection: &str| {
        pins.config_named(database_id, tenant_id, collection)
            .is_none()
    };
    if let Some((_, collection)) = writes
        .into_iter()
        .find(|(write, collection)| write.requires_pin() && unpinned(collection))
    {
        return Err(ConfigPinError::Unpinned {
            collection: collection.to_string(),
        });
    }
    // A binding names its target bare. The pin keys it by its qualified
    // name.
    let targets = sum_targets
        .iter()
        .flat_map(|targets| &targets.resolved)
        .filter(|_| PinnedWrite::SumTarget.requires_pin());
    for target in targets {
        let qualified = QualifiedCollection::new(database_id, &target.target_collection);
        if unpinned(qualified.as_str()) {
            return Err(ConfigPinError::Unpinned {
                collection: qualified.as_str().to_string(),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_physical::physical_plan::{
        ConfigPinKey, CrsPin, DeclaredColumn, EnforcementOptions, RegisteredIndex,
        RegisteredIndexState, StorageMode, TextFieldSet,
    };

    use super::*;
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::data::executor::handlers::document::write::RegisterDocumentCollectionParams;

    const TID: u64 = 1;
    const COLLECTION: &str = "orders";

    fn key() -> ConfigPinKey {
        ConfigPinKey {
            database_id: DatabaseId::DEFAULT,
            tenant_id: TenantId::new(TID),
            collection: COLLECTION.into(),
        }
    }

    /// A wire form that sets every field it holds.
    fn wire() -> DocConfigWire {
        let mut enforcement = EnforcementOptions {
            append_only: true,
            ..EnforcementOptions::default()
        };
        enforcement
            .type_guards
            .push(nodedb_types::TypeGuardFieldDef {
                field: "sku".into(),
                type_expr: "STRING".into(),
                required: true,
                check_expr: None,
                default_expr: None,
                value_expr: None,
            });
        DocConfigWire {
            bitemporal: true,
            crdt_enabled: false,
            enforcement,
            storage_mode: StorageMode::Schemaless,
            index_paths: vec![RegisteredIndex {
                name: "by_sku".into(),
                path: "$.sku".into(),
                unique: true,
                case_insensitive: false,
                state: RegisteredIndexState::Ready,
                predicate: None,
            }],
            declared_key: Some("sku".into()),
            vector_primary: None,
            declared_columns: vec![
                DeclaredColumn::from_declared("qty", "INTEGER").expect("numeric column"),
            ],
            vector_fields: vec![("emb".into(), 3)],
        }
    }

    fn pin_of(wire: &DocConfigWire) -> ConfigPin {
        let (bytes, digest) = wire.pin_parts().expect("pin parts");
        ConfigPin {
            key: key(),
            digest,
            wire: bytes.into(),
        }
    }

    /// [`judged_config_from_pin`] with the hash of the pin's raw bytes.
    fn decode(
        current: Option<&CollectionConfig>,
        pin: &ConfigPin,
    ) -> Result<CollectionConfig, ConfigPinError> {
        judged_config_from_pin(
            current,
            pin,
            nodedb_physical::physical_plan::pin_digest(pin.wire.as_slice()),
        )
    }

    fn pins(configs: Vec<ConfigPin>) -> RedoPins {
        RedoPins {
            configs: configs.into_iter().map(Arc::new).collect(),
            crs: Some(Arc::new(CrsPin {
                digest: 1,
                bytes: vec![0x90].into(),
            })),
        }
    }

    /// A pin decode and a `Register` of the same wire form give one config.
    #[test]
    fn a_pin_decodes_to_the_config_register_builds() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req_tx, _resp_rx) = make_core_with_dir(dir.path());
        let pin = pin_of(&wire());
        // The pin holds the canonical form, which Register takes as is.
        let canonical: DocConfigWire = zerompk::from_msgpack(pin.wire.as_slice()).expect("decode");
        let response = core.execute_register_document_collection(
            &make_default_task(),
            RegisterDocumentCollectionParams {
                tid: TID,
                collection: COLLECTION,
                indexes: &canonical.index_paths,
                crdt_enabled: false,
                storage_mode: &canonical.storage_mode,
                enforcement: &canonical.enforcement,
                bitemporal: canonical.bitemporal,
                conflict_policy: None,
                timeseries: None,
                vector_primary: canonical.vector_primary.as_ref(),
                vector_fields: &canonical.vector_fields,
                declared_columns: &canonical.declared_columns,
                declared_key: canonical.declared_key.as_deref(),
                text_fields: &TextFieldSet::default(),
                incarnation: nodedb_types::Hlc::ZERO,
                digest: pin.digest,
            },
        );
        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        let registered = core
            .doc_configs
            .get(&(DatabaseId::DEFAULT, TenantId::new(TID), COLLECTION.into()))
            .expect("registered config")
            .clone();

        assert_eq!(decode(None, &pin).expect("decode"), registered);
        assert_eq!(decode(Some(&registered), &pin).expect("decode"), registered);
    }

    /// The decode keeps what the wire form does not fix and overwrites
    /// what it does.
    #[test]
    fn a_pin_overwrites_only_the_fields_its_wire_form_fixes() {
        let mut current = CollectionConfig::new(COLLECTION).with_crdt();
        current.incarnation = nodedb_types::Hlc::new(9, 0);
        current.digest = 5;
        let pin = pin_of(&wire());
        let judged = decode(Some(&current), &pin).expect("decode");
        // The wire fixes `crdt_enabled`, so the pin's value replaces the current one.
        assert!(!judged.crdt_enabled);
        assert_eq!(judged.incarnation, nodedb_types::Hlc::new(9, 0));
        assert!(judged.bitemporal);
        assert_eq!(judged.declared_key.as_deref(), Some("sku"));
        assert_eq!(judged.declared_columns, wire().declared_columns);
        assert_eq!(judged.vector_fields, wire().vector_fields);
        assert_eq!(judged.type_guards.len(), 1);
        assert_eq!(judged.digest, pin.digest);
    }

    #[test]
    fn a_pin_whose_digest_does_not_match_is_refused() {
        let mut pin = pin_of(&wire());
        pin.digest = pin.digest.wrapping_add(1);
        let refused = decode(None, &pin).expect_err("refused");
        assert!(matches!(refused, ConfigPinError::DigestMismatch { .. }));
        assert!(matches!(
            ErrorCode::from(refused),
            ErrorCode::RejectedPrevalidation { .. }
        ));
    }

    #[test]
    fn a_pin_whose_bytes_do_not_decode_is_refused() {
        // The bytes hash to the digest, so the decode refuses them.
        let mut pin = pin_of(&wire());
        pin.wire = vec![0xc1].into();
        pin.digest = nodedb_physical::physical_plan::pin_digest(pin.wire.as_slice());
        assert!(matches!(
            decode(None, &pin),
            Err(ConfigPinError::Undecodable { .. })
        ));
    }

    #[test]
    fn a_pinned_record_must_pin_every_written_collection() {
        let pinned = pins(vec![pin_of(&wire())]);
        let at = |write, collection| {
            check_written_pinned(
                &pinned,
                DatabaseId::DEFAULT,
                TenantId::new(TID),
                [(PinnedWrite::DocumentRows, COLLECTION), (write, collection)],
                &[],
            )
        };
        assert!(at(PinnedWrite::DocumentRows, COLLECTION).is_ok());
        for write in [PinnedWrite::DocumentRows, PinnedWrite::KvRows] {
            assert!(matches!(
                at(write, "ghost"),
                Err(ConfigPinError::Unpinned { collection }) if collection == "ghost"
            ));
        }
        // A write that requires no pin passes unpinned.
        assert!(at(PinnedWrite::Other, "ghost").is_ok());
        // Another tenant's collection of the same name has no pin here.
        assert!(
            check_written_pinned(
                &pinned,
                DatabaseId::DEFAULT,
                TenantId::new(2),
                [(PinnedWrite::DocumentRows, COLLECTION)],
                &[]
            )
            .is_err()
        );
        // A record with no pins skips the check.
        assert!(
            check_written_pinned(
                &RedoPins::default(),
                DatabaseId::DEFAULT,
                TenantId::new(TID),
                [(PinnedWrite::KvRows, "ghost")],
                &[],
            )
            .is_ok()
        );
    }

    fn sum_targets(target: &str) -> Vec<RedoSumTargets> {
        vec![RedoSumTargets {
            collection: COLLECTION.into(),
            resolved: vec![nodedb_physical::physical_plan::ResolvedSumTarget::new(
                target,
                "a1",
                nodedb_types::Surrogate::new(1),
            )],
        }]
    }

    /// A pinned record pins every sum target it folds into, as the
    /// proposer pins them.
    #[test]
    fn a_pinned_record_with_an_unpinned_sum_target_is_refused() {
        let pinned = pins(vec![pin_of(&wire())]);
        let judged = |target: &str| {
            check_written_pinned(
                &pinned,
                DatabaseId::DEFAULT,
                TenantId::new(TID),
                [(PinnedWrite::DocumentRows, COLLECTION)],
                &sum_targets(target),
            )
        };
        assert!(judged(COLLECTION).is_ok());
        assert!(matches!(
            judged("accounts"),
            Err(ConfigPinError::Unpinned { collection }) if collection == "accounts"
        ));
        // A record with no pins folds into any target.
        assert!(
            check_written_pinned(
                &RedoPins::default(),
                DatabaseId::DEFAULT,
                TenantId::new(TID),
                std::iter::empty::<(PinnedWrite, &str)>(),
                &sum_targets("accounts"),
            )
            .is_ok()
        );
    }
}
