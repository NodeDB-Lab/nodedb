// SPDX-License-Identifier: BUSL-1.1

//! Persisted tenant identity and ownership fallback.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredTenant {
    pub tenant_id: u64,
    pub name: String,
    pub created_at: u64,
    pub is_active: bool,
    /// Authoritative principal that receives objects when an owner is dropped.
    /// `CREATE TENANT` always names one.
    pub admin_username: String,
}

impl zerompk::ToMessagePack for StoredTenant {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_array_len(5)?;
        zerompk::ToMessagePack::write(&self.tenant_id, writer)?;
        zerompk::ToMessagePack::write(&self.name, writer)?;
        zerompk::ToMessagePack::write(&self.created_at, writer)?;
        zerompk::ToMessagePack::write(&self.is_active, writer)?;
        zerompk::ToMessagePack::write(&self.admin_username, writer)
    }
}

impl<'de> zerompk::FromMessagePack<'de> for StoredTenant {
    fn read<R: zerompk::Read<'de>>(reader: &mut R) -> zerompk::Result<Self> {
        let len = reader.read_array_len()?;
        if len != 5 {
            return Err(zerompk::Error::ArrayLengthMismatch {
                expected: 5,
                actual: len,
            });
        }
        Ok(Self {
            tenant_id: zerompk::FromMessagePack::read(reader)?,
            name: zerompk::FromMessagePack::read(reader)?,
            created_at: zerompk::FromMessagePack::read(reader)?,
            is_active: zerompk::FromMessagePack::read(reader)?,
            admin_username: zerompk::FromMessagePack::read(reader)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A four-field tenant row without `admin_username`.
    #[derive(zerompk::ToMessagePack)]
    struct TenantWithoutAdmin {
        tenant_id: u64,
        name: String,
        created_at: u64,
        is_active: bool,
    }

    #[test]
    fn a_row_without_admin_is_refused() {
        let bytes = zerompk::to_msgpack_vec(&TenantWithoutAdmin {
            tenant_id: 7,
            name: "acme".to_string(),
            created_at: 11,
            is_active: true,
        })
        .unwrap();

        assert!(zerompk::from_msgpack::<StoredTenant>(&bytes).is_err());
    }

    #[test]
    fn a_full_row_round_trips() {
        let tenant = StoredTenant {
            tenant_id: 7,
            name: "acme".to_string(),
            created_at: 11,
            is_active: true,
            admin_username: "admin".to_string(),
        };
        let bytes = zerompk::to_msgpack_vec(&tenant).unwrap();
        assert_eq!(
            zerompk::from_msgpack::<StoredTenant>(&bytes).unwrap(),
            tenant
        );
    }
}
