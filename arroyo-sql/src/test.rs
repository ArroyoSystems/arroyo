use std::collections::HashMap;

use arrow_schema::{DataType, TimeUnit};
use arroyo_datastream::{NexmarkSource, Source};

use crate::{
    parse_and_get_program,
    types::{StructDef, StructField, TypeDef},
    ArroyoSchemaProvider, SqlConfig,
};

fn test_schema() -> Vec<StructField> {
    vec![
        StructField {
            name: "bid".to_string(),
            alias: None,
            data_type: TypeDef::StructDef(
                StructDef {
                    name: Some("arroyo_types::nexmark::Bid".to_string()),
                    fields: vec![
                        StructField {
                            name: "auction".to_string(),
                            alias: None,
                            data_type: TypeDef::DataType(DataType::UInt64, false),
                        },
                        StructField {
                            name: "datetime".to_string(),
                            alias: None,
                            data_type: TypeDef::DataType(
                                DataType::Timestamp(TimeUnit::Millisecond, None),
                                false,
                            ),
                        },
                    ],
                },
                true,
            ),
        },
        StructField {
            name: "auction".to_string(),
            alias: None,
            data_type: TypeDef::StructDef(
                StructDef {
                    name: Some("arroyo_types::nexmark::Auction".to_string()),
                    fields: vec![
                        StructField {
                            name: "auction".to_string(),
                            alias: None,
                            data_type: TypeDef::DataType(DataType::UInt64, false),
                        },
                        StructField {
                            name: "datetime".to_string(),
                            alias: None,
                            data_type: TypeDef::DataType(
                                DataType::Timestamp(TimeUnit::Millisecond, None),
                                false,
                            ),
                        },
                    ],
                },
                true,
            ),
        },
    ]
}

#[tokio::test]
async fn test_parse() {
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

    let mut schema_provider = ArroyoSchemaProvider::new();
    schema_provider.add_source_with_type(
        1,
        "nexmark".to_string(),
        test_schema(),
        NexmarkSource {
            first_event_rate: 10,
            num_events: Some(100),
        }
        .as_operator(),
        Some("arroyo_types::nexmark::NexmarkEvent".to_string()),
    );

    parse_and_get_program(sql, schema_provider, SqlConfig::default())
        .await
        .unwrap();
}

#[tokio::test]
async fn test_program_compilation() {
    let mut schema_provider = ArroyoSchemaProvider::new();
    schema_provider.add_source_with_type(
        1,
        "nexmark".to_string(),
        test_schema(),
        NexmarkSource {
            first_event_rate: 10,
            num_events: Some(100),
        }
        .as_operator(),
        Some("arroyo_types::nexmark::NexmarkEvent".to_string()),
    );

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
    let mut schema_provider = ArroyoSchemaProvider::new();
    schema_provider.add_source_with_type(
        1,
        "nexmark".to_string(),
        test_schema(),
        NexmarkSource {
            first_event_rate: 10,
            num_events: Some(100),
        }
        .as_operator(),
        Some("arroyo_types::nexmark::NexmarkEvent".to_string()),
    );

    let sql = "SELECT P1.bid FROM nexmark as P1";

    parse_and_get_program(sql, schema_provider, SqlConfig::default())
        .await
        .unwrap();
}

#[tokio::test]
async fn test_window_function() {
    let mut schema_provider = ArroyoSchemaProvider::new();
    schema_provider.add_source_with_type(
        1,
        "nexmark".to_string(),
        test_schema(),
        NexmarkSource {
            first_event_rate: 10,
            num_events: Some(100),
        }
        .as_operator(),
        Some("arroyo_types::nexmark::NexmarkEvent".to_string()),
    );

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
