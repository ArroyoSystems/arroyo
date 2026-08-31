use std::{collections::HashMap, sync::Arc};

use arrow_schema::Schema;
use datafusion::{
    catalog::TableProvider,
    common::{DataFusionError, TableReference, not_impl_err},
    execution::FunctionRegistry,
};
use datafusion_substrait::{
    extensions::Extensions,
    logical_plan::consumer::{SubstraitConsumer, from_substrait_plan_with_consumer},
    substrait::proto::Plan,
};
use prost::Message;

use crate::{
    ArroyoSchemaProvider, CompiledSql, Insert, PlannerError, SqlConfig, compile_logical_plans,
    fields_with_qualifiers, logical::LogicalBatchInput, planner_error, planning_session_state,
};

/// Determines where the root of a Substrait plan is written.
#[derive(Clone, Debug)]
pub enum SubstraitSink {
    Preview,
    Named(String),
}

struct ArroyoSubstraitConsumer<'a> {
    extensions: Extensions,
    schema_provider: &'a ArroyoSchemaProvider,
}

#[async_trait::async_trait]
impl SubstraitConsumer for ArroyoSubstraitConsumer<'_> {
    async fn resolve_table_ref(
        &self,
        table_ref: &TableReference,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        let Some(table) = self.schema_provider.get_table(table_ref.table()) else {
            return Ok(None);
        };

        let schema = Arc::new(Schema::new_with_metadata(
            table.get_fields(),
            HashMap::new(),
        ));
        Ok(Some(Arc::new(LogicalBatchInput {
            table_name: table_ref.to_string(),
            schema,
        })))
    }

    fn get_extensions(&self) -> &Extensions {
        &self.extensions
    }

    fn get_function_registry(&self) -> &impl FunctionRegistry {
        self.schema_provider
    }
}

/// Compile a decoded Substrait plan into an Arroyo logical program.
pub async fn get_program_from_substrait_plan(
    plan: &Plan,
    mut schema_provider: ArroyoSchemaProvider,
    sink: SubstraitSink,
    _config: SqlConfig,
) -> Result<CompiledSql, PlannerError> {
    let result = async {
        let extensions = Extensions::try_from(&plan.extensions)?;
        if !extensions.type_variations.is_empty() {
            return not_impl_err!("Type variation extensions are not supported");
        }

        let consumer = ArroyoSubstraitConsumer {
            extensions,
            schema_provider: &schema_provider,
        };
        let logical_plan = from_substrait_plan_with_consumer(&consumer, plan).await?;

        let insert = match sink {
            SubstraitSink::Preview => Insert::Anonymous { logical_plan },
            SubstraitSink::Named(sink_name) => {
                let table = schema_provider.get_table_mut(&sink_name).ok_or_else(|| {
                    DataFusionError::Plan(format!("Connection {sink_name} not found"))
                })?;
                table.set_inferred_fields(fields_with_qualifiers(logical_plan.schema()))?;
                Insert::InsertQuery {
                    sink_name,
                    logical_plan,
                }
            }
        };

        compile_logical_plans(vec![insert], schema_provider, planning_session_state()).await
    }
    .await;

    result.map_err(planner_error)
}

/// Decode Substrait protobuf bytes and compile them into an Arroyo logical program.
pub async fn get_program_from_substrait_bytes(
    bytes: &[u8],
    schema_provider: ArroyoSchemaProvider,
    sink: SubstraitSink,
    config: SqlConfig,
) -> Result<CompiledSql, PlannerError> {
    let plan = Plan::decode(bytes).map_err(|error| {
        planner_error(DataFusionError::Plan(format!(
            "Invalid Substrait protobuf: {error}"
        )))
    })?;

    get_program_from_substrait_plan(&plan, schema_provider, sink, config).await
}

#[cfg(test)]
mod tests {
    use arroyo_connectors::nexmark::NexmarkTable;
    use arroyo_operator::connector::Connection;
    use arroyo_rpc::api_types::connections::{
        ConnectionSchema, ConnectionType, FieldType, SourceField,
    };
    use datafusion_substrait::logical_plan::producer::to_substrait_plan;
    use prost::Message;

    use super::*;
    use crate::{
        parse_sql,
        test::{get_test_schema_provider, get_test_schema_provider_named},
    };

    const SOURCE_UUID: &str = "9f0c30f1-2a41-4aa9-b07f-a56745c83e90";
    const FILTER_SOURCE_UUID: &str = "2a2d2bf8-b03b-4f32-a53a-2ed07fa183ab";

    fn get_filter_test_schema_provider() -> ArroyoSchemaProvider {
        let mut schema_provider = ArroyoSchemaProvider::new();
        schema_provider.add_connector_table(Connection::new(
            Some(1),
            "nexmark",
            FILTER_SOURCE_UUID.to_string(),
            ConnectionType::Source,
            ConnectionSchema {
                format: None,
                bad_data: None,
                framing: None,
                fields: vec![SourceField {
                    name: "a".to_string(),
                    field_type: FieldType::String,
                    required: false,
                    sql_name: None,
                    metadata_key: None,
                }],
                definition: None,
                inferred: None,
                primary_keys: Default::default(),
            },
            &NexmarkTable {
                event_rate: 10.0,
                runtime: Some(10.0 * 1_000_000.0),
            },
            "Interoperability test source".to_string(),
        ));
        schema_provider
    }

    #[tokio::test]
    async fn compiles_substrait_plan_with_named_table() {
        let mut schema_provider = get_test_schema_provider();
        let statement = parse_sql("SELECT bid.auction FROM nexmark")
            .unwrap()
            .remove(0);
        let insert = Insert::try_from_statement(&statement, &mut schema_provider).unwrap();
        let Insert::Anonymous { logical_plan } = insert else {
            panic!("query should produce an anonymous logical plan");
        };

        let substrait_plan = to_substrait_plan(&logical_plan, &planning_session_state()).unwrap();
        let compiled = get_program_from_substrait_plan(
            &substrait_plan,
            schema_provider,
            SubstraitSink::Preview,
            SqlConfig::default(),
        )
        .await
        .unwrap();

        assert!(compiled.program.graph.node_count() >= 2);
        assert_eq!(compiled.connection_ids, vec![1]);
    }

    #[tokio::test]
    async fn compiles_substrait_bytes_with_uuid_named_table() {
        let mut schema_provider = get_test_schema_provider_named(SOURCE_UUID);
        let statement = parse_sql(&format!(
            "SELECT bid.auction FROM \"{SOURCE_UUID}\" WHERE bid.price > 100"
        ))
        .unwrap()
        .remove(0);
        let insert = Insert::try_from_statement(&statement, &mut schema_provider).unwrap();
        let Insert::Anonymous { logical_plan } = insert else {
            panic!("query should produce an anonymous logical plan");
        };

        let substrait_plan = to_substrait_plan(&logical_plan, &planning_session_state()).unwrap();
        let compiled = get_program_from_substrait_bytes(
            &substrait_plan.encode_to_vec(),
            schema_provider,
            SubstraitSink::Preview,
            SqlConfig::default(),
        )
        .await
        .unwrap();

        assert!(compiled.program.graph.node_count() >= 2);
        assert_eq!(compiled.connection_ids, vec![1]);
    }

    #[tokio::test]
    async fn compiles_filter_fixture() {
        let plan: Plan =
            serde_json::from_str(include_str!("test/fixtures/filter.substrait.json")).unwrap();

        let compiled = get_program_from_substrait_bytes(
            &plan.encode_to_vec(),
            get_filter_test_schema_provider(),
            SubstraitSink::Preview,
            SqlConfig::default(),
        )
        .await
        .unwrap();

        assert!(compiled.program.graph.node_count() >= 2);
        assert_eq!(compiled.connection_ids, vec![1]);
    }

    #[tokio::test]
    async fn reports_invalid_substrait_protobuf() {
        let error = get_program_from_substrait_bytes(
            &[0xff],
            get_test_schema_provider(),
            SubstraitSink::Preview,
            SqlConfig::default(),
        )
        .await
        .unwrap_err();

        assert_eq!(error.diagnostics.len(), 1);
        assert!(
            error.diagnostics[0]
                .message
                .contains("Invalid Substrait protobuf")
        );
        assert!(error.diagnostics[0].span.is_none());
    }
}
