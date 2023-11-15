use arroyo_connectors::{
    kafka::{KafkaConfig, KafkaConnector, KafkaTable, ReadMode},
    Connection, Connector,
};
use arroyo_rpc::{
    api_types::connections::{ConnectionSchema, SchemaDefinition},
    formats::{AvroFormat, Format, JsonFormat, TimestampFormat},
};

use anyhow::Result;
use arroyo_sql::{avro::convert_avro_schema, json_schema::convert_json_schema};

pub fn get_json_schema_source() -> Result<Connection> {
    let json_schema = r##"
    {
      "type": "object",
      "title": "ksql.orders",
      "properties": {
        "itemid": {
          "type": "string",
          "connect.index": 2
        },
        "address": {
          "type": "object",
          "title": "ksql.address",
          "connect.index": 4,
          "properties": {
            "zipcode": {
              "type": "integer",
              "connect.index": 2,
              "connect.type": "int64"
            },
            "city": {
              "type": "string",
              "connect.index": 0
            },
            "state": {
              "type": "string",
              "connect.index": 1
            },
            "nested": {
                "type": "object",
                "properties": {
                    "a": {
                        "type": "integer"
                    }
                }
            }
          }
        },
        "orderid": {
          "type": "integer",
          "connect.index": 1,
          "connect.type": "int32"
        },
        "orderunits": {
          "type": "number",
          "connect.index": 3,
          "connect.type": "float64"
        },
        "ordertime": {
          "type": "string",
          "format": "date-time",
          "connect.index": 0,
          "connect.type": "timestamp"
        }
      }
    }            "##;

    let definition = SchemaDefinition::JsonSchema(json_schema.to_string());
    let struct_fields = convert_json_schema("kafka_json_schema", json_schema).unwrap();
    let connection_schema = ConnectionSchema::try_new(
        Some(Format::Json(JsonFormat {
            confluent_schema_registry: false,
            include_schema: false,
            debezium: false,
            unstructured: false,
            timestamp_format: TimestampFormat::RFC3339,
        })),
        None,
        None,
        struct_fields
            .into_iter()
            .map(|field| field.try_into().unwrap())
            .collect(),
        Some(definition),
    )?;
    let config = KafkaConfig {
        authentication: arroyo_connectors::kafka::KafkaConfigAuthentication::None {},
        bootstrap_servers: "localhost:9092".try_into().unwrap(),
        schema_registry: None,
    };
    let table = KafkaTable {
        topic: "test_topic".to_string(),
        type_: arroyo_connectors::kafka::TableType::Source {
            group_id: None,
            offset: arroyo_connectors::kafka::SourceOffset::Earliest,
            read_mode: Some(ReadMode::ReadUncommitted),
        },
    };
    KafkaConnector {}.from_config(
        Some(2),
        "kafka_json_schema",
        config,
        table,
        Some(&connection_schema),
    )
}

pub fn get_avro_source() -> Result<Connection> {
    let avro_schema = r#"
    {
"connect.name": "pizza_orders.pizza_orders",
"fields": [
{
  "name": "store_id",
  "type": "int"
},
{
  "name": "store_order_id",
  "type": "int"
},
{
  "name": "coupon_code",
  "type": "int"
},
{
  "name": "date",
  "type": {
    "connect.name": "org.apache.kafka.connect.data.Date",
    "connect.version": 1,
    "logicalType": "date",
    "type": "int"
  }
},
{
  "name": "status",
  "type": "string"
},
{
  "name": "order_lines",
  "type": {
    "items": {
      "connect.name": "pizza_orders.order_line",
      "fields": [
        {
          "name": "product_id",
          "type": "int"
        },
        {
          "name": "category",
          "type": "string"
        },
        {
          "name": "quantity",
          "type": "int"
        },
        {
          "name": "unit_price",
          "type": "double"
        },
        {
          "name": "net_price",
          "type": "double"
        }
      ],
      "name": "order_line",
      "type": "record"
    },
    "type": "array"
  }
}
],
"name": "pizza_orders",
"namespace": "pizza_orders",
"type": "record"
}"#;
    let definition = SchemaDefinition::AvroSchema(avro_schema.to_string());
    let struct_fields = convert_avro_schema("kafka_avro_schema", avro_schema).unwrap();
    let mut format = AvroFormat::new(true, false, false);
    format.add_reader_schema(apache_avro::Schema::parse_str(avro_schema).unwrap());
    let connection_schema = ConnectionSchema::try_new(
        Some(Format::Avro(format)),
        None,
        None,
        struct_fields
            .into_iter()
            .map(|field| field.try_into().unwrap())
            .collect(),
        Some(definition),
    )?;
    let config = KafkaConfig {
        authentication: arroyo_connectors::kafka::KafkaConfigAuthentication::None {},
        bootstrap_servers: "localhost:9092".try_into().unwrap(),
        schema_registry: None,
    };
    let table = KafkaTable {
        topic: "test_topic".to_string(),
        type_: arroyo_connectors::kafka::TableType::Source {
            group_id: None,
            offset: arroyo_connectors::kafka::SourceOffset::Earliest,
            read_mode: Some(ReadMode::ReadUncommitted),
        },
    };
    KafkaConnector {}.from_config(
        Some(3),
        "kafka_avro_schema",
        config,
        table,
        Some(&connection_schema),
    )
}

#[test]
pub fn get_avro() -> Result<()> {
    get_avro_source().unwrap();
    Ok(())
}
