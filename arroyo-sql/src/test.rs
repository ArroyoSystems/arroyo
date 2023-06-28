use arroyo_connectors::{
    nexmark::{NexmarkConnector, NexmarkTable},
    Connector, EmptyConfig,
};

use crate::{parse_and_get_program, ArroyoSchemaProvider, SqlConfig};

#[tokio::test]
async fn test_parse() {
    let schema_provider = get_test_schema_provider();

    let sql = "
  WITH bids as (SELECT bid.auction as auction, bid.datetime as datetime
    FROM (select bid from  nexmark) where bid is not null)
    SELECT AuctionBids.auction as auction, AuctionBids.num as count
    FROM (
      SELECT
        B1.auction,
        HOP(INTERVAL '2' SECOND, INTERVAL '10' SECOND) as window,
        count(*) AS num

      FROM bids B1
      GROUP BY
        1,2
    ) AS AuctionBids
    JOIN (
      SELECT
        max(num) AS maxn,
        window
      FROM (
        SELECT
          count(*) AS num,
          HOP(INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS window
        FROM bids B2
        GROUP BY
          B2.auction,2
        ) AS CountBids
      GROUP BY 2
    ) AS MaxBids
    ON
       AuctionBids.num = MaxBids.maxn
       and AuctionBids.window = MaxBids.window;";

    parse_and_get_program(sql, schema_provider, SqlConfig::default())
        .await
        .unwrap();
}

#[tokio::test]
async fn test_program_compilation() {
    let schema_provider = get_test_schema_provider();

    let sql = "
    SELECT * FROM (
    SELECT ROW_NUMBER()  OVER (
        PARTITION BY window
        ORDER BY count DESC) as row_number, auction FROM (
      SELECT       bid.auction as auction,
    hop(INTERVAL '10' minute, INTERVAL '20' minute ) as window,
    count(*) as count
  FROM (SELECT bid from nexmark where bid is not null)
  GROUP BY 1, 2)) where row_number = 1 ";

    parse_and_get_program(sql, schema_provider, SqlConfig::default())
        .await
        .unwrap();
}

#[tokio::test]
async fn test_table_alias() {
    let schema_provider = get_test_schema_provider();

    let sql = "SELECT P1.bid FROM nexmark as P1";

    parse_and_get_program(sql, schema_provider, SqlConfig::default())
        .await
        .unwrap();
}

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

#[tokio::test]
async fn test_window_function() {
    let schema_provider = get_test_schema_provider();

    let sql = "SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY window
        ORDER BY count DESC) as row_num
    FROM (SELECT count(*) as count,
        hop(interval '2 seconds', interval '10 seconds') as window
            FROM nexmark
            group by window)) WHERE row_num <= 5";

    parse_and_get_program(sql, schema_provider, SqlConfig::default())
        .await
        .unwrap();
}

#[tokio::test]
async fn test_no_updating_window_functions() {
    let schema_provider = get_test_schema_provider();
    let sql = "SELECT *, row_number() OVER (partition by bid.auction order by bid.datetime desc) as row_num
     FROM nexmark where bid is not null";
    let err = parse_and_get_program(sql, schema_provider, SqlConfig::default())
        .await
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "window functions have to be partitioned by a time window"
    );
}

#[tokio::test]
async fn test_no_virtual_fields_updating() {
    let schema_provider = get_test_schema_provider();
    let sql =  "CREATE table debezium_source (
        bids_auction int,
        price int,
        auctions_id int,
        initial_bid int,
        date_string text,
        datetime datetime GENERATED ALWAYS AS (CAST(date_string as timestamp)),
        watermark datetime GENERATED ALWAYS AS (CAST(date_string as timestamp) - interval '1 second')
      ) WITH (
        connector = 'kafka',
        bootstrap_servers = 'localhost:9092',
        type = 'source',
        topic = 'updating',
        format = 'debezium_json'
      );
      SELECT * FROM debezium_source";
    let err = parse_and_get_program(sql, schema_provider, SqlConfig::default())
        .await
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "can't read from a source with virtual fields and update mode."
    );
}

#[tokio::test]
async fn test_udf() {
    let mut schema_provider = get_test_schema_provider();

    schema_provider
        .add_rust_udf("fn my_sqr(x: i64) -> i64 { x * x }")
        .unwrap();

    let sql = "SELECT my_sqr(bid.auction) FROM nexmark";
    parse_and_get_program(sql, schema_provider, SqlConfig::default())
        .await
        .unwrap();
}
