use std::{any::Any, sync::Arc};

use arrow_schema::SchemaRef;
use datafusion::common::Result as DFResult;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::{
    datasource::TableProvider, execution::context::SessionState, physical_plan::ExecutionPlan,
};
use serde::{Deserialize, Serialize};

use crate::physical::ArroyoMemExec;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogicalBatchInput {
    pub table_name: String,
    pub schema: SchemaRef,
}

#[async_trait::async_trait]

impl TableProvider for LogicalBatchInput {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    #[doc = " Get the type of this table for metadata/catalog purposes."]
    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    /// Create an ExecutionPlan that will scan the table.
    /// The table provider will be usually responsible for grouping
    /// the source data into partitions that can be efficiently
    /// parallelized or distributed.
    async fn scan(
        &self,
        _state: &SessionState,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        // limit can be used to reduce the amount scanned
        // from the datasource as a performance optimization.
        // If set, it contains the amount of rows needed by the `LogicalPlan`,
        // The datasource should return *at least* this number of rows if available.
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ArroyoMemExec::new(
            self.table_name.clone(),
            self.schema.clone(),
        )))
    }
}
