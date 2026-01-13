mod plan_tests;

use arrow_schema::DataType;
use arroyo_connectors::{
    EmptyConfig,
    nexmark::{NexmarkConnector, NexmarkTable},
};
use arroyo_operator::connector::Connector;
use arroyo_udf_host::parse::NullableType;
use test_log::test;

use crate::{ArroyoSchemaProvider, SqlConfig, parse_and_get_program};

fn get_test_schema_provider() -> ArroyoSchemaProvider {
    let mut schema_provider = ArroyoSchemaProvider::new();

    let nexmark = (NexmarkConnector {})
        .from_config(
            Some(1),
            "nexmark",
            EmptyConfig {},
            NexmarkTable {
                event_rate: 10.0,
                runtime: Some(10.0 * 1_000_000.0),
            },
            None,
        )
        .unwrap();

    schema_provider.add_connector_table(nexmark);

    schema_provider
}

#[test(tokio::test)]
async fn test_udf() {
    let mut schema_provider = get_test_schema_provider();

    schema_provider
        .add_rust_udf("#[udf] fn my_sqr(x: i64) -> i64 { x * x }", "")
        .unwrap();

    schema_provider
        .add_rust_udf(
            "#[udf] fn my_sqr_opt(x: i64) -> Option<i64> { Some(x * x) }",
            "",
        )
        .unwrap();

    let def = schema_provider.udf_defs.get("my_sqr").unwrap();
    assert_eq!(def.ret, NullableType::not_null(DataType::Int64));

    let def = schema_provider.udf_defs.get("my_sqr_opt").unwrap();
    assert_eq!(def.ret, NullableType::null(DataType::Int64));

    let sql = "SELECT my_sqr(bid.auction), my_sqr_opt(bid.auction) FROM nexmark";
    parse_and_get_program(sql, schema_provider, SqlConfig::default())
        .await
        .unwrap();
}
