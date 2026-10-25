// SPDX-License-Identifier: Apache-2.0

//! Axis order and direction of a CRS, as PROJ `+axis` spells it.
//!
//! The native order is east, north, up: longitude, latitude, height for a
//! geographic CRS, and easting, northing, height for a projected one.
//! `+axis=xyz` names the direction of each CRS ordinate with one letter
//! from `e w n s u d`. PROJ's `axisswap` step defines the mapping:
//! CRS ordinate `i` is `sign(i) · native[axis(i)]`.

use core::fmt;

use serde::{Deserialize, Serialize};

use crate::error::CrsParseError;

/// The direction of one CRS ordinate.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum AxisDirection {
    East,
    West,
    North,
    South,
    Up,
    Down,
}

impl AxisDirection {
    /// The direction PROJ writes as `letter`.
    pub fn from_letter(letter: char) -> Option<AxisDirection> {
        match letter {
            'e' => Some(AxisDirection::East),
            'w' => Some(AxisDirection::West),
            'n' => Some(AxisDirection::North),
            's' => Some(AxisDirection::South),
            'u' => Some(AxisDirection::Up),
            'd' => Some(AxisDirection::Down),
            _ => None,
        }
    }

    /// The PROJ letter of this direction.
    pub fn letter(self) -> char {
        match self {
            AxisDirection::East => 'e',
            AxisDirection::West => 'w',
            AxisDirection::North => 'n',
            AxisDirection::South => 's',
            AxisDirection::Up => 'u',
            AxisDirection::Down => 'd',
        }
    }

    /// Index of the native ordinate this direction reads: 0 east, 1 north,
    /// 2 up.
    pub fn native_index(self) -> usize {
        match self {
            AxisDirection::East | AxisDirection::West => 0,
            AxisDirection::North | AxisDirection::South => 1,
            AxisDirection::Up | AxisDirection::Down => 2,
        }
    }

    /// +1 along the native direction, −1 against it.
    pub fn sign(self) -> f64 {
        match self {
            AxisDirection::East | AxisDirection::North | AxisDirection::Up => 1.0,
            AxisDirection::West | AxisDirection::South | AxisDirection::Down => -1.0,
        }
    }
}

/// The direction of each of the three CRS ordinates.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct AxisOrder {
    pub x: AxisDirection,
    pub y: AxisDirection,
    pub z: AxisDirection,
}

impl AxisOrder {
    /// East, north, up: the native order, PROJ `+axis=enu`.
    pub const ENU: AxisOrder = AxisOrder {
        x: AxisDirection::East,
        y: AxisDirection::North,
        z: AxisDirection::Up,
    };

    /// Parse a PROJ `+axis` value: three letters from `e w n s u d`, one
    /// per native axis. A repeated axis, such as `ewu`, is refused, as PROJ
    /// refuses it.
    pub fn parse(text: &str) -> Result<AxisOrder, CrsParseError> {
        let bad = || CrsParseError::invalid("axis", text);
        let mut letters = text.chars().map(AxisDirection::from_letter);
        let mut next = || letters.next().flatten().ok_or_else(bad);
        let order = AxisOrder {
            x: next()?,
            y: next()?,
            z: next()?,
        };
        if text.chars().count() != 3 {
            return Err(bad());
        }
        let [a, b, c] = order.directions().map(AxisDirection::native_index);
        if a == b || a == c || b == c {
            return Err(bad());
        }
        Ok(order)
    }

    /// Whether this is the native order.
    pub fn is_enu(&self) -> bool {
        *self == AxisOrder::ENU
    }

    /// The directions of x, y and z.
    pub fn directions(&self) -> [AxisDirection; 3] {
        [self.x, self.y, self.z]
    }

    /// Native east, north, up ordinates to CRS ordinates.
    pub fn from_native(&self, native: [f64; 3]) -> [f64; 3] {
        self.directions()
            .map(|d| d.sign() * native[d.native_index()])
    }

    /// CRS ordinates to native east, north, up ordinates.
    pub fn to_native(&self, ordinates: [f64; 3]) -> [f64; 3] {
        let mut native = [0.0; 3];
        for (d, v) in self.directions().into_iter().zip(ordinates) {
            native[d.native_index()] = d.sign() * v;
        }
        native
    }
}

impl Default for AxisOrder {
    fn default() -> Self {
        AxisOrder::ENU
    }
}

impl fmt::Display for AxisOrder {
    /// The PROJ `+axis` value, such as `enu`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for d in self.directions() {
            fmt::Write::write_char(f, d.letter())?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_every_permutation_and_sign() {
        let mut accepted = 0;
        for a in "ewnsud".chars() {
            for b in "ewnsud".chars() {
                for c in "ewnsud".chars() {
                    let text: String = [a, b, c].iter().collect();
                    if let Ok(order) = AxisOrder::parse(&text) {
                        assert_eq!(order.to_string(), text);
                        accepted += 1;
                    }
                }
            }
        }
        // 3! axis orders times 2³ signs.
        assert_eq!(accepted, 48);
    }

    #[test]
    fn refuses_repeats_length_and_letters() {
        for bad in ["", "en", "enuu", "ewu", "nsu", "enn", "xyz", "eNu", "enü"] {
            assert!(
                matches!(
                    AxisOrder::parse(bad),
                    Err(CrsParseError::InvalidValue { .. })
                ),
                "{bad}"
            );
        }
    }

    #[test]
    fn swaps_and_negates_like_axisswap() {
        let wsu = AxisOrder::parse("wsu").unwrap();
        assert_eq!(wsu.from_native([1.0, 2.0, 3.0]), [-1.0, -2.0, 3.0]);
        let swu = AxisOrder::parse("swu").unwrap();
        assert_eq!(swu.from_native([1.0, 2.0, 3.0]), [-2.0, -1.0, 3.0]);
        let neu = AxisOrder::parse("neu").unwrap();
        assert_eq!(neu.from_native([1.0, 2.0, 3.0]), [2.0, 1.0, 3.0]);
        let und = AxisOrder::parse("dne").unwrap();
        assert_eq!(und.from_native([1.0, 2.0, 3.0]), [-3.0, 2.0, 1.0]);
        for order in [wsu, swu, neu, und, AxisOrder::ENU] {
            let v = [7.5, -3.25, 11.0];
            assert_eq!(order.to_native(order.from_native(v)), v, "{order}");
        }
        assert!(AxisOrder::default().is_enu());
    }
}
