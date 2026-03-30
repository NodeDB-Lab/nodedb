//! User-defined SQL expression function — DataFusion `ScalarUDFImpl`.
//!
//! Wraps a user's SQL expression body as a vectorized Arrow function.
//! The body is compiled to a `PhysicalExpr` on first invocation and cached
//! for subsequent calls. Each invocation evaluates the expression on an
//! Arrow `RecordBatch` built from the input `ColumnarValue`s.
//!
//! This provides correct vectorized execution even without the inlining
//! `AnalyzerRule` (which additionally enables optimizer visibility).

use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, OnceLock};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::Result as DfResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion::physical_expr::PhysicalExpr;

use crate::control::security::catalog::{FunctionVolatility, StoredFunction};
use crate::control::server::pgwire::ddl::function::sql_type_to_arrow;

/// A user-defined SQL expression function registered with DataFusion.
///
/// Created from a [`StoredFunction`] loaded from the system catalog.
/// The SQL body is lazily compiled to a `PhysicalExpr` on first invocation.
#[derive(Debug)]
pub struct UserDefinedFunction {
    name: String,
    param_names: Vec<String>,
    param_types: Vec<DataType>,
    return_type: DataType,
    body_sql: String,
    signature: Signature,
    /// Lazily compiled physical expression from the SQL body.
    compiled: OnceLock<Arc<dyn PhysicalExpr>>,
    /// Arrow schema for the parameter RecordBatch.
    param_schema: Arc<Schema>,
}

// Manual impls because `OnceLock<Arc<dyn PhysicalExpr>>` doesn't derive these.
// Two UserDefinedFunctions are equal iff they have the same name + body + params.
impl PartialEq for UserDefinedFunction {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.body_sql == other.body_sql
            && self.param_names == other.param_names
            && self.param_types == other.param_types
            && self.return_type == other.return_type
    }
}
impl Eq for UserDefinedFunction {}

impl Hash for UserDefinedFunction {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.body_sql.hash(state);
        self.param_names.hash(state);
    }
}

impl UserDefinedFunction {
    /// Create from a stored function definition.
    ///
    /// Returns `None` if any parameter or return type cannot be mapped to Arrow.
    pub fn from_stored(func: &StoredFunction) -> Option<Self> {
        let return_type = sql_type_to_arrow(&func.return_type)?;

        let mut param_names = Vec::with_capacity(func.parameters.len());
        let mut param_types = Vec::with_capacity(func.parameters.len());
        let mut fields = Vec::with_capacity(func.parameters.len());

        for p in &func.parameters {
            let dt = sql_type_to_arrow(&p.data_type)?;
            fields.push(Field::new(&p.name, dt.clone(), true));
            param_names.push(p.name.clone());
            param_types.push(dt);
        }

        let volatility = match func.volatility {
            FunctionVolatility::Immutable => Volatility::Immutable,
            FunctionVolatility::Stable => Volatility::Stable,
            FunctionVolatility::Volatile => Volatility::Volatile,
        };

        let type_sig = if param_types.is_empty() {
            TypeSignature::Exact(vec![])
        } else {
            TypeSignature::Exact(param_types.clone())
        };

        let signature = Signature::one_of(vec![type_sig], volatility);
        let param_schema = Arc::new(Schema::new(fields));

        // Use compiled SQL for procedural bodies, raw SQL for expression bodies.
        let effective_body = func
            .compiled_body_sql
            .clone()
            .unwrap_or_else(|| func.body_sql.clone());

        Some(Self {
            name: func.name.clone(),
            param_names,
            param_types,
            return_type,
            body_sql: effective_body,
            signature,
            compiled: OnceLock::new(),
            param_schema,
        })
    }

    /// Access the effective body SQL (compiled for procedural, raw for expression).
    /// Used by the inlining transform.
    pub fn body_sql(&self) -> &str {
        &self.body_sql
    }

    /// Parameter names (used by the inlining `AnalyzerRule`).
    pub fn param_names(&self) -> &[String] {
        &self.param_names
    }

    /// Compile the body SQL into a `PhysicalExpr`.
    ///
    /// Uses a temporary DataFusion session to parse and plan the expression
    /// with the parameter schema as context.
    fn compile(&self) -> DfResult<Arc<dyn PhysicalExpr>> {
        use datafusion::common::DFSchema;
        use datafusion::execution::context::SessionContext;
        use datafusion::physical_expr::create_physical_expr;
        use datafusion::prelude::SessionConfig;

        let config = SessionConfig::new()
            .with_information_schema(false)
            .with_default_catalog_and_schema("nodedb", "public");
        let ctx = SessionContext::new_with_config(config);

        // Register system UDFs so body can reference them.
        crate::control::planner::context::register_udfs_on(&ctx);

        let df_schema = DFSchema::try_from(self.param_schema.as_ref().clone())?;

        // Strip leading SELECT if present.
        let expr_sql = self.body_sql.trim();
        let expr_sql = if expr_sql.to_uppercase().starts_with("SELECT ") {
            &expr_sql["SELECT ".len()..]
        } else {
            expr_sql
        };

        let state = ctx.state();
        let logical_expr = state.create_logical_expr(expr_sql, &df_schema)?;

        let execution_props = datafusion::execution::context::ExecutionProps::new();
        let physical_expr = create_physical_expr(&logical_expr, &df_schema, &execution_props)?;

        Ok(physical_expr)
    }

    /// Get or lazily compile the physical expression.
    fn get_compiled(&self) -> DfResult<Arc<dyn PhysicalExpr>> {
        if let Some(expr) = self.compiled.get() {
            return Ok(Arc::clone(expr));
        }
        let compiled = self.compile()?;
        // Race is fine — all compilations produce equivalent results.
        let _ = self.compiled.set(compiled.clone());
        Ok(compiled)
    }
}

impl ScalarUDFImpl for UserDefinedFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(self.return_type.clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let compiled = self.get_compiled()?;
        let num_rows = args.number_rows;

        if self.param_types.is_empty() {
            // No parameters — evaluate as a constant expression on a single-row batch.
            let empty_schema = Arc::new(Schema::empty());
            let batch = RecordBatch::new_empty(empty_schema);
            return compiled.evaluate(&batch);
        }

        // Build a RecordBatch from input ColumnarValues.
        let mut columns = Vec::with_capacity(args.args.len());
        for (i, arg) in args.args.iter().enumerate() {
            let array = match arg {
                ColumnarValue::Array(arr) => Arc::clone(arr),
                ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(num_rows)?,
            };
            // Ensure the array type matches what the schema expects.
            if array.data_type() != self.param_schema.field(i).data_type() {
                let cast = datafusion::arrow::compute::cast(
                    &array,
                    self.param_schema.field(i).data_type(),
                )?;
                columns.push(cast);
            } else {
                columns.push(array);
            }
        }

        let batch = RecordBatch::try_new(Arc::clone(&self.param_schema), columns)?;
        compiled.evaluate(&batch)
    }
}
