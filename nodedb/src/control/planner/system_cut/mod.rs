// SPDX-License-Identifier: BUSL-1.1

//! The per-statement system-time cut: every `AS OF SYSTEM TIME` of a
//! statement pinned to one ordinal under one read cut.

pub mod pin;
pub mod sites;

pub use pin::{CutPinning, pin_statement_cuts};
pub use sites::{for_each_cut_site, for_each_cut_site_ref, plan_scopes};
