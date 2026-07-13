use crate::control::security::identity::Permission;

use super::AuthorizationRequirement;

pub(super) fn requirement_order(
    left: &AuthorizationRequirement,
    right: &AuthorizationRequirement,
) -> std::cmp::Ordering {
    match (left, right) {
        (
            AuthorizationRequirement::Tenant { permission: left },
            AuthorizationRequirement::Tenant { permission: right },
        ) => permission_rank(*left).cmp(&permission_rank(*right)),
        (AuthorizationRequirement::Tenant { .. }, AuthorizationRequirement::Collection { .. }) => {
            std::cmp::Ordering::Less
        }
        (AuthorizationRequirement::Collection { .. }, AuthorizationRequirement::Tenant { .. }) => {
            std::cmp::Ordering::Greater
        }
        (
            AuthorizationRequirement::Collection {
                collection: left_collection,
                permission: left_permission,
            },
            AuthorizationRequirement::Collection {
                collection: right_collection,
                permission: right_permission,
            },
        ) => left_collection.cmp(right_collection).then_with(|| {
            permission_rank(*left_permission).cmp(&permission_rank(*right_permission))
        }),
    }
}

fn permission_rank(permission: Permission) -> u8 {
    match permission {
        Permission::Read => 0,
        Permission::Write => 1,
        Permission::Create => 2,
        Permission::Drop => 3,
        Permission::Alter => 4,
        Permission::Admin => 5,
        Permission::Monitor => 6,
        Permission::Execute => 7,
        Permission::Backup => 8,
    }
}
