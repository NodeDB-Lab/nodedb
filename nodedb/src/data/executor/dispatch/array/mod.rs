//! Array engine dispatch — wires `PhysicalPlan::Array(ArrayOp)` to the
//! Data-Plane `ArrayEngine` via the shared `ArrayCatalogHandle`.

pub mod aggregate;
pub mod convert;
pub mod elementwise;
pub mod encode;
pub mod entry;
pub mod mutate;
pub mod open;
pub mod read;

#[cfg(test)]
mod tests_dispatch;
