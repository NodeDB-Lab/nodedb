//! Tree operations: `CREATE GRAPH INDEX`, `TREE_SUM`, `TREE_CHILDREN`.
//!
//! These build on the existing CSR graph engine for hierarchical aggregation
//! over self-referential collections (e.g. chart of accounts with parent_id).
//!
//! Syntax:
//! ```sql
//! CREATE GRAPH INDEX account_tree ON accounts (parent_id -> id);
//! SELECT TREE_SUM(balance, account_tree, 'assets');
//! SELECT TREE_CHILDREN(account_tree, 'expenses');
//! ```

pub mod children;
pub mod create_index;
pub mod parse;
pub mod sum;

pub use children::tree_children;
pub use create_index::create_graph_index;
pub use sum::tree_sum;
