// SPDX-License-Identifier: Apache-2.0

//! `skip_value`: advance past one MessagePack value without decoding it.

pub use nodedb_types::msgpack_span::skip_value;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msgpack_scan::reader::read_str;

    #[test]
    fn skip_value_resolves_and_adversarial_str_is_rejected() {
        let buf = [0xdbu8, 0xff, 0xff, 0xff, 0xff, b'x', b'y'];
        assert_eq!(skip_value(&buf, 0), None);
        assert_eq!(read_str(&buf, 0), None);
    }
}
