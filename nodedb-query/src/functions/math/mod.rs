// SPDX-License-Identifier: Apache-2.0

//! Math scalar functions.

mod dispatch;
mod float;
mod numeric;
mod numeric_scale;
mod wide_sqrt;

pub(super) use dispatch::try_eval;
