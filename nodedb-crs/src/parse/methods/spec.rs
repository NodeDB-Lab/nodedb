// SPDX-License-Identifier: Apache-2.0

//! The method and parameter specs every family table builds from.

use super::super::model::{DEGREE_RAD, normalize_name};

/// What a parameter measures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ParamKind {
    /// Degrees in PROJ.
    Angle,
    /// Metres in PROJ.
    Length,
    /// A unitless factor.
    Scale,
}

impl ParamKind {
    /// The factor of the unit PROJ reads this kind in: degree, metre or
    /// unity.
    pub(crate) fn canonical_factor(self) -> f64 {
        match self {
            ParamKind::Angle => DEGREE_RAD,
            ParamKind::Length | ParamKind::Scale => 1.0,
        }
    }
}

/// Where a parameter's value goes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum Target {
    /// Each PROJ key takes the value. Writers emit the first key.
    Keys(&'static [&'static str]),
    /// PROJ fixes the value. A reader refuses any other.
    Fixed(f64),
}

/// One EPSG parameter of a method.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct ParamSpec {
    pub name: &'static str,
    pub code: Option<u32>,
    pub kind: ParamKind,
    pub target: Target,
}

/// A value a reader derives when the text leaves it out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Derive {
    None,
    /// Polar Stereographic (variant B) has no origin latitude: the pole on
    /// the side of the standard parallel.
    PolarLatitudeFromStandardParallel,
}

/// One EPSG method.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct MethodSpec {
    pub name: &'static str,
    pub code: Option<u32>,
    /// The PROJ `+proj` value.
    pub proj: &'static str,
    /// PROJ tokens the method implies, such as `no_uoff` or `sweep=x`.
    pub flags: &'static [&'static str],
    pub params: &'static [ParamSpec],
    pub derive: Derive,
}

impl MethodSpec {
    /// The parameter with EPSG `code`, else the one named `name`.
    pub(crate) fn param(&self, code: Option<u32>, name: &str) -> Option<&'static ParamSpec> {
        let code = code.or_else(|| param_code_alias(name));
        if let Some(code) = code
            && let Some(found) = self.params.iter().find(|p| p.code == Some(code))
        {
            return Some(found);
        }
        let normalized = normalize_name(name);
        self.params
            .iter()
            .find(|p| normalize_name(p.name) == normalized)
    }
}

/// PROJ flags a parameter can carry under the `PROJ ` prefix.
pub(crate) const FLAG_KEYS: &[&str] = &["no_rot"];

/// The EPSG code of a parameter name PROJ wrote in another version.
fn param_code_alias(name: &str) -> Option<u32> {
    match normalize_name(name).as_str() {
        "azimuthatprojectioncentre" => Some(8813),
        "scalefactoratprojectioncentre" => Some(8815),
        _ => None,
    }
}

pub(super) const fn param(
    name: &'static str,
    code: u32,
    kind: ParamKind,
    key: &'static [&'static str],
) -> ParamSpec {
    ParamSpec {
        name,
        code: Some(code),
        kind,
        target: Target::Keys(key),
    }
}

pub(super) const fn fixed(name: &'static str, code: u32, kind: ParamKind, value: f64) -> ParamSpec {
    ParamSpec {
        name,
        code: Some(code),
        kind,
        target: Target::Fixed(value),
    }
}

pub(super) use ParamKind::{Angle, Length, Scale};

pub(super) const fn method(
    name: &'static str,
    code: Option<u32>,
    proj: &'static str,
    flags: &'static [&'static str],
    params: &'static [ParamSpec],
) -> MethodSpec {
    MethodSpec {
        name,
        code,
        proj,
        flags,
        params,
        derive: Derive::None,
    }
}
