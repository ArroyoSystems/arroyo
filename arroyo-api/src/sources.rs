use std::fmt::{Display, Formatter};

use arrow::datatypes::TimeUnit;
use cornucopia_async::GenericClient;
use deadpool_postgres::Pool;
use http::StatusCode;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tonic::Status;
use tracing::warn;

use arroyo_datastream::{SerializationMode, SourceConfig};
use arroyo_rpc::grpc::api::{
    self,
    create_source_req::{self},
    source_def::SourceType,
    source_schema::Schema,
    ConfluentSchemaReq, ConfluentSchemaResp, Connection, CreateSourceReq, DeleteSourceReq,
    JsonSchemaDef, RawJsonDef, SourceDef, SourceField, SourceMetadataResp, TestSourceMessage,
};
use arroyo_sql::{
    types::{StructDef, StructField, TypeDef},
    ArroyoSchemaProvider,
};

use crate::handle_delete;
use crate::types::public::SchemaType;
use crate::{
    connections::get_connections,
    handle_db_error,
    json_schema::{self, convert_json_schema},
    log_and_map,
    queries::api_queries,
    required_field, AuthData,
};

pub fn impulse_schema() -> SourceSchema {
    SourceSchema {
        format: SourceFormat::Native("impulse".to_string()),
        fields: vec![
            SchemaField::new("counter", SchemaFieldType::Primitive(PrimitiveType::UInt64)),
            SchemaField::new(
                "subtask_index",
                SchemaFieldType::Primitive(PrimitiveType::UInt64),
            ),
        ],
        kafka_schema: false,
    }
}

pub fn raw_schema() -> SourceSchema {
    SourceSchema {
        format: SourceFormat::RawJson,
        fields: vec![SchemaField::new(
            "value",
            SchemaFieldType::Primitive(PrimitiveType::String),
        )],
        kafka_schema: false,
    }
}

pub fn nexmark_schema() -> SourceSchema {
    use PrimitiveType::*;
    use SchemaFieldType::*;
    SourceSchema {
        format: SourceFormat::Native("nexmark".to_string()),
        fields: vec![
            SchemaField::nullable(
                "person",
                SchemaFieldType::NamedStruct(
                    "arroyo_types::nexmark::Person".to_string(),
                    vec![
                        SchemaField::new("id", Primitive(Int64)),
                        SchemaField::new("name", Primitive(String)),
                        SchemaField::new("email_address", Primitive(String)),
                        SchemaField::new("credit_card", Primitive(String)),
                        SchemaField::new("city", Primitive(String)),
                        SchemaField::new("state", Primitive(String)),
                        SchemaField::new("datetime", Primitive(UnixMillis)),
                        SchemaField::new("extra", Primitive(String)),
                    ],
                ),
            ),
            SchemaField::nullable(
                "bid",
                SchemaFieldType::NamedStruct(
                    "arroyo_types::nexmark::Bid".to_string(),
                    vec![
                        SchemaField::new("auction", Primitive(Int64)),
                        SchemaField::new("bidder", Primitive(Int64)),
                        SchemaField::new("price", Primitive(Int64)),
                        SchemaField::new("channel", Primitive(String)),
                        SchemaField::new("url", Primitive(String)),
                        SchemaField::new("datetime", Primitive(UnixMillis)),
                        SchemaField::new("extra", Primitive(String)),
                    ],
                ),
            ),
            SchemaField::nullable(
                "auction",
                SchemaFieldType::NamedStruct(
                    "arroyo_types::nexmark::Auction".to_string(),
                    vec![
                        SchemaField::new("id", Primitive(Int64)),
                        SchemaField::new("description", Primitive(String)),
                        SchemaField::new("initial_bid", Primitive(Int64)),
                        SchemaField::new("reserve", Primitive(Int64)),
                        SchemaField::new("datetime", Primitive(UnixMillis)),
                        SchemaField::new("expires", Primitive(UnixMillis)),
                        SchemaField::new("seller", Primitive(Int64)),
                        SchemaField::new("category", Primitive(Int64)),
                        SchemaField::new("extra", Primitive(String)),
                    ],
                ),
            ),
        ],
        kafka_schema: false,
    }
}

#[derive(Copy, Clone, Debug)]
pub enum PrimitiveType {
    Int32,
    Int64,
    UInt32,
    UInt64,
    F32,
    F64,
    Bool,
    String,
    Bytes,
    UnixMillis,
}

#[derive(Clone, Debug)]
pub enum SchemaFieldType {
    Primitive(PrimitiveType),
    #[allow(dead_code)]
    Struct(Vec<SchemaField>),
    NamedStruct(String, Vec<SchemaField>),
}

impl Display for SchemaFieldType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaFieldType::Primitive(p) => write!(f, "{:?}", p),
            SchemaFieldType::Struct(s) => write!(f, "{:?}", s),
            SchemaFieldType::NamedStruct(s, _) => write!(f, "{}", s),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SchemaField {
    pub name: String,
    pub typ: SchemaFieldType,
    pub nullable: bool,
}

impl SchemaField {
    fn new(name: impl Into<String>, typ: SchemaFieldType) -> Self {
        Self {
            name: name.into(),
            typ,
            nullable: false,
        }
    }

    fn nullable(name: impl Into<String>, typ: SchemaFieldType) -> Self {
        Self {
            name: name.into(),
            typ,
            nullable: true,
        }
    }
}

impl From<&SchemaField> for StructField {
    fn from(sf: &SchemaField) -> Self {
        use PrimitiveType::*;
        use SchemaFieldType::*;
        use TypeDef::DataType;
        let data_type = match &sf.typ {
            Primitive(t) => DataType(
                match t {
                    Int32 => arrow::datatypes::DataType::Int32,
                    Int64 => arrow::datatypes::DataType::Int64,
                    UInt32 => arrow::datatypes::DataType::UInt32,
                    UInt64 => arrow::datatypes::DataType::UInt64,
                    F32 => arrow::datatypes::DataType::Float32,
                    F64 => arrow::datatypes::DataType::Float64,
                    Bool => arrow::datatypes::DataType::Boolean,
                    String => arrow::datatypes::DataType::Utf8,
                    Bytes => arrow::datatypes::DataType::Binary,
                    UnixMillis => {
                        arrow::datatypes::DataType::Timestamp(TimeUnit::Millisecond, None)
                    }
                },
                sf.nullable,
            ),
            NamedStruct(name, fields) => TypeDef::StructDef(
                StructDef {
                    name: Some(name.clone()),
                    fields: fields.iter().map(|f| f.into()).collect(),
                },
                sf.nullable,
            ),
            Struct(fields) => TypeDef::StructDef(
                StructDef {
                    name: None,
                    fields: fields.iter().map(|f| f.into()).collect(),
                },
                sf.nullable,
            ),
        };
        StructField {
            name: sf.name.clone(),
            data_type,
            alias: None,
        }
    }
}

impl TryFrom<SourceField> for SchemaField {
    type Error = String;

    fn try_from(f: SourceField) -> Result<Self, Self::Error> {
        Ok(Self {
            name: f.field_name,
            typ: match f
                .field_type
                .ok_or_else(|| "field type is not set".to_string())?
                .r#type
                .ok_or_else(|| "type is not set".to_string())?
            {
                api::source_field_type::Type::Primitive(p) => SchemaFieldType::Primitive(
                    match api::PrimitiveType::from_i32(p)
                        .ok_or_else(|| format!("unknown enum variant {}", p))?
                    {
                        api::PrimitiveType::Int32 => PrimitiveType::Int32,
                        api::PrimitiveType::Int64 => PrimitiveType::Int64,
                        api::PrimitiveType::UInt32 => PrimitiveType::UInt32,
                        api::PrimitiveType::UInt64 => PrimitiveType::Int64,
                        api::PrimitiveType::F32 => PrimitiveType::F32,
                        api::PrimitiveType::F64 => PrimitiveType::F64,
                        api::PrimitiveType::Bool => PrimitiveType::Bool,
                        api::PrimitiveType::String => PrimitiveType::String,
                        api::PrimitiveType::Bytes => PrimitiveType::Bytes,
                        api::PrimitiveType::UnixMillis => PrimitiveType::UnixMillis,
                        api::PrimitiveType::UnixMicros => todo!(),
                    },
                ),
                api::source_field_type::Type::Struct(s) => SchemaFieldType::Struct(
                    s.fields
                        .into_iter()
                        .filter_map(|f| f.try_into().ok())
                        .collect(),
                ),
            },
            nullable: f.nullable,
        })
    }
}

impl TryFrom<SchemaField> for SourceField {
    type Error = String;

    fn try_from(value: SchemaField) -> Result<Self, Self::Error> {
        let typ = match &value.typ {
            SchemaFieldType::Primitive(p) => api::source_field_type::Type::Primitive(match p {
                PrimitiveType::Int32 => api::PrimitiveType::Int32,
                PrimitiveType::Int64 => api::PrimitiveType::Int64,
                PrimitiveType::UInt32 => api::PrimitiveType::UInt32,
                PrimitiveType::UInt64 => api::PrimitiveType::UInt64,
                PrimitiveType::F32 => api::PrimitiveType::F32,
                PrimitiveType::F64 => api::PrimitiveType::F64,
                PrimitiveType::Bool => api::PrimitiveType::Bool,
                PrimitiveType::String => api::PrimitiveType::String,
                PrimitiveType::Bytes => api::PrimitiveType::Bytes,
                PrimitiveType::UnixMillis => api::PrimitiveType::UnixMillis,
            }
                as i32),
            SchemaFieldType::NamedStruct(_, fields) | SchemaFieldType::Struct(fields) => {
                api::source_field_type::Type::Struct(api::StructType {
                    fields: fields
                        .iter()
                        .filter_map(|f| f.clone().try_into().ok())
                        .collect(),
                })
            }
        };

        let sql_name = match value.typ {
            SchemaFieldType::Primitive(p) => todo!(),
            SchemaFieldType::Struct(..) => None,
            SchemaFieldType::NamedStruct(..) => None,
        };

        Ok(SourceField {
            field_name: value.name,
            field_type: Some(api::SourceFieldType {
                r#type: Some(typ),
                sql_name,
            }),
            nullable: value.nullable,
        })
    }
}

#[derive(Debug, PartialEq)]
pub enum SourceFormat {
    Native(String),
    JsonFields,
    JsonSchema(String),
    RawJson,
}

pub struct SourceSchema {
    format: SourceFormat,
    fields: Vec<SchemaField>,

    // set to true to use the kafka schema serialization wire format
    // (https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#wire-format)
    kafka_schema: bool,
}

fn builtin_for_name(name: &str) -> Result<SourceSchema, String> {
    match name {
        "nexmark" => Ok(nexmark_schema()),
        "impulse" => Ok(impulse_schema()),
        _ => Err(format!("Unknown builtin schema {}", name)),
    }
}

impl SourceSchema {
    pub fn try_from(name: &str, s: api::SourceSchema) -> Result<Self, String> {
        todo!()
    }

    pub fn fields(&self) -> Vec<SchemaField> {
        self.fields.clone()
    }

    pub fn serialization_mode(&self) -> SerializationMode {
        if self.format == SourceFormat::RawJson {
            SerializationMode::RawJson
        } else if self.kafka_schema {
            SerializationMode::JsonSchemaRegistry
        } else {
            SerializationMode::Json
        }
    }
}

impl TryFrom<&SourceSchema> for api::SourceSchema {
    type Error = String;

    fn try_from(s: &SourceSchema) -> Result<Self, Self::Error> {
        Ok(api::SourceSchema {
            schema: Some(match &s.format {
                SourceFormat::Native(s) => Schema::Builtin(s.clone()),
                SourceFormat::JsonFields => Schema::JsonFields(api::JsonFieldDef {
                    fields: s
                        .fields
                        .clone()
                        .into_iter()
                        .filter_map(|f| f.try_into().ok())
                        .collect(),
                }),
                SourceFormat::JsonSchema(s) => Schema::JsonSchema(JsonSchemaDef {
                    json_schema: s.clone(),
                }),
                SourceFormat::RawJson => Schema::RawJson(api::RawJsonDef {}),
            }),
            kafka_schema_registry: s.kafka_schema,
        })
    }
}

pub struct Source {
    id: i64,
    name: String,
    schema: SourceSchema,
    config: SourceConfig,
}

impl TryFrom<SourceDef> for Source {
    type Error = String;

    fn try_from(value: SourceDef) -> Result<Self, Self::Error> {
        let schema = if let Schema::Builtin(name) =
            value.schema.as_ref().unwrap().schema.as_ref().unwrap()
        {
            builtin_for_name(name)?
        } else {
            SourceSchema::try_from(&value.name, value.schema.clone().unwrap())
                .map_err(|e| format!("Invalid schema: {}", e))?
        };

        Ok(Source {
            id: value.id,
            name: value.name,
            schema,
            config: value.source_type.unwrap().into(),
        })
    }
}

impl Source {
    // TODO: pass down restriction on kafka source QPS from auth.
    pub(crate) fn register(&self, provider: &mut ArroyoSchemaProvider, _auth: &AuthData) {
        let type_name = match &self.schema.format {
            SourceFormat::Native(native_name) => match native_name.as_str() {
                "nexmark" => Some("arroyo_types::nexmark::Event".to_string()),
                "impulse" => Some("arroyo_types::ImpulseEvent".to_string()),
                _ => None,
            },
            SourceFormat::JsonFields => None,
            SourceFormat::JsonSchema(_) => {
                Some(format!("{}::{}", self.name, json_schema::ROOT_NAME))
            }
            SourceFormat::RawJson => Some("arroyo_types::RawJson".to_string()),
        };

        let defs: Option<String> = match &self.schema.format {
            SourceFormat::Native(_) => None,
            SourceFormat::JsonFields => None,
            SourceFormat::JsonSchema(s) => Some(json_schema::get_defs(&self.name, s).unwrap()),
            SourceFormat::RawJson => None,
        };

        if let Some(defs) = defs {
            provider.add_defs(&self.name, defs);
        }

        provider.add_saved_source_with_type(
            self.id,
            self.name.clone(),
            self.schema.fields.iter().map(|f| f.into()).collect(),
            type_name,
            self.config.clone(),
            self.schema.serialization_mode(),
        );
    }
}

fn rate_limit_error(source: &str, limit: usize) -> Result<(), Status> {
    Err(Status::failed_precondition(format!(
        "Under your plan, {} source rates must be less than {}; contact us at support@arroyo.systems for to increase limits",
        source, limit)))
}

pub(crate) async fn create_source(
    req: CreateSourceReq,
    auth: AuthData,
    pool: &Pool,
) -> Result<(), Status> {
    let schema_name = format!("{}_schema", req.name);

    let mut c = pool.get().await.map_err(log_and_map)?;

    let transaction = c.transaction().await.map_err(log_and_map)?;
    transaction
        .execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", &[])
        .await
        .map_err(log_and_map)?;

    let connections = api_queries::get_connections()
        .bind(&transaction, &auth.organization_id)
        .all()
        .await
        .map_err(log_and_map)?;
    // insert schema
    let schema = req
        .schema
        .or_else(|| {
            match req.type_oneof {
                Some(create_source_req::TypeOneof::Impulse { .. }) => {
                    Some(Schema::Builtin("impulse".to_string()))
                }
                Some(create_source_req::TypeOneof::Nexmark { .. }) => {
                    Some(Schema::Builtin("nexmark".to_string()))
                }
                _ => None,
            }
            .map(|s| api::SourceSchema {
                schema: Some(s),
                kafka_schema_registry: false,
            })
        })
        .ok_or_else(|| required_field("schema"))?;

    let (schema_type, config) = match schema
        .schema
        .ok_or_else(|| required_field("schema.schema"))?
    {
        Schema::Builtin(name) => {
            builtin_for_name(&name).map_err(Status::invalid_argument)?;
            (SchemaType::builtin, serde_json::to_value(&name).unwrap())
        }
        Schema::JsonSchema(js) => {
            // try to convert the schema to ensure it's valid
            convert_json_schema(&req.name, &js.json_schema).map_err(Status::invalid_argument)?;

            // parse the schema into a value
            (SchemaType::json_schema, serde_json::to_value(&js).unwrap())
        }
        Schema::JsonFields(fields) => (
            SchemaType::json_fields,
            serde_json::to_value(fields).unwrap(),
        ),
        Schema::RawJson(_) => (SchemaType::raw_json, serde_json::to_value(()).unwrap()),
        Schema::Protobuf(_) => todo!(),
    };

    let schema_id = api_queries::create_schema()
        .bind(
            &transaction,
            &auth.organization_id,
            &auth.user_id,
            &schema_name,
            &schema.kafka_schema_registry,
            &schema_type,
            &config,
        )
        .one()
        .await
        .map_err(|err| handle_db_error("schema", err))?;

    // insert source
    let (source_type, config, connection_id) =
        match req.type_oneof.ok_or_else(|| required_field("type"))? {
            create_source_req::TypeOneof::Impulse(impulse) => {
                if impulse.events_per_second > auth.org_metadata.max_impulse_qps as f32 {
                    return rate_limit_error("impulse", auth.org_metadata.max_impulse_qps as usize);
                }

                (
                    "impulse".to_string(),
                    serde_json::to_value(impulse).unwrap(),
                    None,
                )
            }
            create_source_req::TypeOneof::File(_) => {
                return Err(Status::failed_precondition("This source is not supported"));
            }
            create_source_req::TypeOneof::Nexmark(nexmark) => {
                if nexmark.events_per_second > auth.org_metadata.max_nexmark_qps as u32 {
                    return rate_limit_error("impulse", auth.org_metadata.max_impulse_qps as usize);
                }

                (
                    "nexmark".to_string(),
                    serde_json::to_value(nexmark).unwrap(),
                    None,
                )
            }
        };

    api_queries::create_source()
        .bind(
            &transaction,
            &auth.organization_id,
            &auth.user_id,
            &req.name,
            &source_type,
            &config,
            &schema_id,
            &connection_id,
        )
        .await
        .map_err(|err| handle_db_error("source", err))?;

    transaction.commit().await.map_err(log_and_map)?;
    Ok(())
}

pub(crate) async fn get_sources<E: GenericClient>(
    auth: &AuthData,
    client: &E,
) -> Result<Vec<SourceDef>, Status> {
    let res = api_queries::get_sources()
        .bind(client, &auth.organization_id)
        .all()
        .await
        .map_err(log_and_map)?;

    let defs = res
        .into_iter()
        .map(|rec| {
            let schema = match rec.schema_type {
                SchemaType::builtin => {
                    Schema::Builtin(serde_json::from_value(rec.schema_config.unwrap()).unwrap())
                }
                SchemaType::json_schema => {
                    Schema::JsonSchema(serde_json::from_value(rec.schema_config.unwrap()).unwrap())
                }
                SchemaType::json_fields => {
                    Schema::JsonFields(serde_json::from_value(rec.schema_config.unwrap()).unwrap())
                }
                SchemaType::raw_json => Schema::RawJson(api::RawJsonDef {}),
            };

            let source_schema = api::SourceSchema {
                kafka_schema_registry: rec.kafka_schema_registry,
                schema: Some(schema),
            };

            let ss = SourceSchema::try_from(&rec.source_name, source_schema.clone()).unwrap();

            let source_type = match rec.source_type.as_str() {
                "nexmark" => {
                    SourceType::Nexmark(serde_json::from_value(rec.source_config.unwrap()).unwrap())
                }
                "impulse" => {
                    SourceType::Impulse(serde_json::from_value(rec.source_config.unwrap()).unwrap())
                }
                "file" => {
                    SourceType::File(serde_json::from_value(rec.source_config.unwrap()).unwrap())
                }
                _ => {
                    todo!()
                }
            };

            SourceDef {
                id: rec.source_id,
                name: rec.source_name,
                schema: Some(source_schema),
                consumers: rec.consumer_count as i32,
                sql_fields: ss
                    .fields()
                    .into_iter()
                    .map(|f| f.try_into().unwrap())
                    .collect(),
                source_type: Some(source_type),
            }
        })
        .collect();

    Ok(defs)
}

pub(crate) async fn get_source_metadata(
    req: CreateSourceReq,
    auth: AuthData,
    client: &impl GenericClient,
) -> Result<SourceMetadataResp, Status> {
    let connections = get_connections(&auth, client).await?;

    let partitions = match req.type_oneof.ok_or_else(|| required_field("type"))? {
        create_source_req::TypeOneof::Impulse(_) => 1,
        create_source_req::TypeOneof::File(_) => 1,
        create_source_req::TypeOneof::Nexmark(_) => 1,
    };

    Ok(SourceMetadataResp {
        partitions: partitions as u32,
    })
}

pub(crate) async fn delete_source(
    req: DeleteSourceReq,
    auth: AuthData,
    client: &impl GenericClient,
) -> Result<(), Status> {
    let deleted = api_queries::delete_source()
        .bind(client, &auth.organization_id, &req.name)
        .await
        .map_err(|e| handle_delete("source", "pipelines", e))?;

    if deleted == 0 {
        return Err(Status::not_found(format!(
            "No source with name {}",
            req.name
        )));
    }

    Ok(())
}

pub(crate) async fn get_confluent_schema(
    req: ConfluentSchemaReq,
) -> Result<ConfluentSchemaResp, Status> {
    // TODO: ensure only external URLs can be hit
    let url = format!(
        "{}/subjects/{}-value/versions/latest",
        req.endpoint, req.topic
    );
    let resp: serde_json::Value = reqwest::get(url)
        .await
        .map_err(|e| {
            warn!("Got error response from schema registry: {:?}", e);
            match e.status() {
                Some(StatusCode::NOT_FOUND) => Status::failed_precondition(format!(
                    "Could not find value schema for topic '{}'",
                    req.topic
                )),
                Some(code) => {
                    Status::failed_precondition(format!("Schema registry returned error: {}", code))
                }
                None => {
                    warn!(
                        "Unknown error connecting to schema registry {}: {:?}",
                        req.endpoint, e
                    );
                    Status::failed_precondition(format!(
                        "Could not connect to Schema Registry at {}: unknown error",
                        req.endpoint
                    ))
                }
            }
        })?
        .json()
        .await
        .map_err(|e| {
            warn!("Invalid json from schema registry: {:?}", e);
            Status::failed_precondition("Schema registry returned invalid JSON".to_string())
        })?;

    let schema_type = resp
        .get("schemaType")
        .ok_or_else(|| {
            Status::failed_precondition("Missing 'schemaType' field in schema registry response")
        })?
        .as_str();

    if schema_type != Some("JSON") {
        return Err(Status::failed_precondition(
            "Only JSON is supported currently",
        ));
    }

    let schema = resp
        .get("schema")
        .ok_or_else(|| {
            Status::failed_precondition("Missing 'schema' field in schema registry response")
        })?
        .as_str()
        .ok_or_else(|| {
            Status::failed_precondition(
                "'schema' field in schema registry response is not a string",
            )
        })?;

    if let Err(e) = convert_json_schema(&req.topic, schema) {
        warn!(
            "Schema from schema registry is not valid: '{}': {}",
            schema, e
        );
        return Err(Status::failed_precondition(format!(
            "Schema is not a valid json schema: {}",
            e
        )));
    }

    Ok(ConfluentSchemaResp {
        schema: schema.to_string(),
    })
}
