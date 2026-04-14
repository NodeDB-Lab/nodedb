//! Write-time evaluation of stored generated (computed) columns.
//!
//! Called during INSERT and UPDATE to materialize generated column values.
//! Handles topological ordering when generated columns depend on each other.

use crate::bridge::envelope::ErrorCode;
use crate::bridge::physical_plan::GeneratedColumnSpec;

/// Evaluate all generated columns and inject computed values into the document.
///
/// Evaluates in dependency order: if column A depends on column B (which is
/// also generated), B is evaluated first. Cycles are detected and rejected.
pub fn evaluate_generated_columns(
    doc: &mut serde_json::Value,
    specs: &[GeneratedColumnSpec],
) -> Result<(), ErrorCode> {
    if specs.is_empty() {
        return Ok(());
    }

    // Topological sort: evaluate columns whose dependencies are all non-generated
    // or already evaluated first.
    let order = topological_sort(specs)?;

    for idx in order {
        let spec = &specs[idx];
        let doc_val = nodedb_types::Value::from(doc.clone());
        let result = spec.expr.eval(&doc_val);
        let computed = serde_json::Value::from(result);
        if let Some(obj) = doc.as_object_mut() {
            obj.insert(spec.name.clone(), computed);
        }
    }

    Ok(())
}

/// Check that an UPDATE doesn't directly modify any generated column.
///
/// Returns an error if any of the update field names matches a generated column.
/// Generic over the payload type so both `Vec<u8>` literal updates and the
/// new `UpdateValue` carrier can share one implementation.
pub fn check_generated_readonly<V>(
    update_fields: &[(String, V)],
    specs: &[GeneratedColumnSpec],
) -> Result<(), ErrorCode> {
    for (field, _) in update_fields {
        if specs.iter().any(|s| s.name == *field) {
            return Err(ErrorCode::RejectedConstraint {
                constraint: format!(
                    "cannot UPDATE generated column '{field}': \
                     generated columns are computed automatically"
                ),
            });
        }
    }
    Ok(())
}

/// Check if any of the updated fields are dependencies of generated columns.
///
/// Returns `true` if generated columns need recomputation after this UPDATE.
pub fn needs_recomputation<V>(
    update_fields: &[(String, V)],
    specs: &[GeneratedColumnSpec],
) -> bool {
    for (field, _) in update_fields {
        for spec in specs {
            if spec.depends_on.contains(field) {
                return true;
            }
        }
    }
    false
}

/// Topological sort of generated column specs by dependency order.
///
/// Returns indices into `specs` in evaluation order. Detects cycles.
fn topological_sort(specs: &[GeneratedColumnSpec]) -> Result<Vec<usize>, ErrorCode> {
    let n = specs.len();
    let mut in_degree = vec![0usize; n];
    let mut adj: Vec<Vec<usize>> = vec![Vec::new(); n];

    // Build dependency graph: if spec[i] depends on spec[j].name, add edge j→i.
    let name_to_idx: std::collections::HashMap<&str, usize> = specs
        .iter()
        .enumerate()
        .map(|(i, s)| (s.name.as_str(), i))
        .collect();

    for (i, spec) in specs.iter().enumerate() {
        for dep in &spec.depends_on {
            if let Some(&j) = name_to_idx.get(dep.as_str()) {
                adj[j].push(i);
                in_degree[i] += 1;
            }
        }
    }

    // Kahn's algorithm.
    let mut queue: Vec<usize> = (0..n).filter(|&i| in_degree[i] == 0).collect();
    let mut order = Vec::with_capacity(n);

    while let Some(node) = queue.pop() {
        order.push(node);
        for &next in &adj[node] {
            in_degree[next] -= 1;
            if in_degree[next] == 0 {
                queue.push(next);
            }
        }
    }

    if order.len() != n {
        return Err(ErrorCode::RejectedConstraint {
            constraint: "cycle detected in generated column dependencies".into(),
        });
    }

    Ok(order)
}
