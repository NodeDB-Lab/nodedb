// SPDX-License-Identifier: BUSL-1.1

//! Runtime witnesses for the config consulted during body materialization.

use crate::engine::document::store::CollectionConfig;
use std::sync::Arc;

#[derive(Clone)]
pub(super) enum BodyConfigWitness {
    Unregistered,
    Registered(Arc<CollectionConfig>),
}

impl BodyConfigWitness {
    pub(super) fn capture(config: Option<&CollectionConfig>) -> Self {
        match config {
            None => Self::Unregistered,
            Some(config) => Self::Registered(Arc::new(config.clone())),
        }
    }

    pub(super) fn matches(&self, current: Option<&CollectionConfig>) -> bool {
        match (self, current) {
            (Self::Unregistered, None) => true,
            (Self::Registered(left), Some(right)) => &**left == right,
            _ => false,
        }
    }
}
