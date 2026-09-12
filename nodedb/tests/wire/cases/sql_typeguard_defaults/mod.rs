// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for typeguard DEFAULT/VALUE expressions and VALIDATE TYPEGUARD.
//!
//! Verifies that:
//! - DEFAULT injects a value when the field is absent
//! - DEFAULT does not overwrite user-provided values
//! - VALUE always overwrites, even when user provides a value
//! - REQUIRED + DEFAULT = field is always present
//! - Cross-field VALUE expressions resolve other document fields

mod convert;
mod defaults;
mod validate;
