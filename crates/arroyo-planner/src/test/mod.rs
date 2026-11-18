mod plan_tests;

use arrow_schema::DataType;
use arroyo_connectors::{
    EmptyConfig,
    nexmark::{NexmarkConnector, NexmarkTable},
};
use arroyo_datastream::logical::LogicalEdgeType;
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

/// Returns the names of the fields each shuffle edge in the planned program routes on
async fn shuffle_routing_keys(sql: &str) -> Vec<Vec<String>> {
    let program = parse_and_get_program(sql, get_test_schema_provider(), SqlConfig::default())
        .await
        .unwrap()
        .program;

    program
        .graph
        .edge_weights()
        .filter(|e| e.edge_type == LogicalEdgeType::Shuffle)
        .map(|e| {
            let fields = e.schema.schema.fields();
            e.schema
                .routing_keys()
                .into_iter()
                .flatten()
                .map(|i| fields[*i].name().clone())
                .collect()
        })
        .collect()
}

#[test(tokio::test)]
async fn test_iceberg_partition_shuffle_routes_on_partition_keys() {
    let sql = r#"
    create table impulse with (connector = 'impulse', event_rate = 100);

    create table sink (
        id TEXT,
        ts TIMESTAMP(6) NOT NULL,
        pg BIGINT,
        cluster TEXT
    ) with (
        connector = 'iceberg',
        'catalog.type' = 'rest',
        'catalog.rest.url' = 'http://localhost:8118/catalog',
        type = 'sink',
        table_name = 'events',
        format = 'parquet',
        'shuffle_by_partition.enabled' = true
    ) PARTITIONED BY (day(ts), bucket(pg, 32), identity(cluster));

    insert into sink
    select cast(counter as TEXT) as id, row_time() as ts, counter as pg,
        cast(subtask_index as TEXT) as cluster
    from impulse;
    "#;

    assert_eq!(
        shuffle_routing_keys(sql).await,
        vec![vec!["__key_0", "__key_1", "__key_2"]]
    );
}

#[test(tokio::test)]
async fn test_filesystem_partition_shuffle_routes_on_partition_key() {
    let sql = r#"
    create table impulse with (connector = 'impulse', event_rate = 100);

    create table sink (
        id TEXT,
        cluster TEXT
    ) with (
        connector = 'filesystem',
        path = 's3://my-bucket/events',
        type = 'sink',
        format = 'parquet',
        'partitioning.fields' = [cluster],
        'shuffle_by_partition.enabled' = true
    );

    insert into sink
    select cast(counter as TEXT) as id, cast(subtask_index as TEXT) as cluster
    from impulse;
    "#;

    assert_eq!(shuffle_routing_keys(sql).await, vec![vec!["__key_0"]]);
}
