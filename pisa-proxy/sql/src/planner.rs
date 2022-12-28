use datafusion_expr::LogicalPlan;
use datafusion_sql::planner::ContextProvider;
use datafusion_common::{
    field_not_found, Column, DFSchema, DFSchemaRef, DataFusionError, Result, ScalarValue,
};

use mysql_parser::ast;

/// SQL query planner
pub struct SqlToRel<'a, S: ContextProvider> {
    schema_provider: &'a S,
}

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    /// Create a new query planner
    pub fn new_with_options(schema_provider: &'a S) -> Self {
        SqlToRel {
            schema_provider,
        }
    }

    /// Generate a logical plan from a SQL statement
    pub fn stmt_to_plan(&self, stmt: ast::SqlStmt) -> Result<LogicalPlan> {
        match stmt {
            ast::SqlStmt::SelectStmt(stmt) => self.select_stmt_to_plan(stmt),
            _ => todo!()
        }
    }

    pub fn select_stmt_to_plan(&self, stmt: ast::SelectStmt) -> Result<LogicalPlan> {
        match stmt {
            ast::SelectStmt::Query(q) => self.query_to_plan(*q),
            _ => todo!()
        }
    }

    fn query_to_plan(&self, query: ast::Query) -> Result<LogicalPlan> {
        Err(DataFusionError::NotImplemented(
            "Only `SHOW CREATE TABLE  ...` statement is supported".to_string(),
        ))
    }
}