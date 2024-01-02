use crate::{avro, json, FramingIterator, SchemaData};
use arroyo_rpc::formats::{AvroFormat, Format, Framing};
use arroyo_rpc::schema_resolver::{FailingSchemaResolver, FixedSchemaResolver, SchemaResolver};
use arroyo_types::SourceError;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::Mutex;

fn deserialize_raw_string<T: DeserializeOwned>(msg: &[u8]) -> Result<T, String> {
    let json = json! {
        { "value": String::from_utf8_lossy(msg) }
    };
    Ok(serde_json::from_value(json).unwrap())
}

#[derive(Clone)]
pub struct DataDeserializer<T: SchemaData> {
    format: Arc<Format>,
    framing: Option<Arc<Framing>>,
    schema_registry: Arc<Mutex<HashMap<u32, apache_avro::schema::Schema>>>,
    schema_resolver: Arc<dyn SchemaResolver + Sync>,
    _t: PhantomData<T>,
}

impl<T: SchemaData> DataDeserializer<T> {
    pub fn new(format: Format, framing: Option<Framing>) -> Self {
        let resolver = if let Format::Avro(AvroFormat {
            reader_schema: Some(schema),
            ..
        }) = &format
        {
            Arc::new(FixedSchemaResolver::new(0, schema.clone().into()))
                as Arc<dyn SchemaResolver + Sync>
        } else {
            Arc::new(FailingSchemaResolver::new()) as Arc<dyn SchemaResolver + Sync>
        };

        Self::with_schema_resolver(format, framing, resolver)
    }

    pub fn with_schema_resolver(
        format: Format,
        framing: Option<Framing>,
        schema_resolver: Arc<dyn SchemaResolver + Sync>,
    ) -> Self {
        Self {
            format: Arc::new(format),
            framing: framing.map(|f| Arc::new(f)),
            schema_registry: Arc::new(Mutex::new(HashMap::new())),
            schema_resolver,
            _t: PhantomData,
        }
    }

    pub async fn deserialize_slice<'a>(
        &mut self,
        msg: &'a [u8],
    ) -> impl Iterator<Item = Result<T, SourceError>> + 'a + Send {
        match &*self.format {
            Format::Avro(avro) => {
                let schema_registry = self.schema_registry.clone();
                let schema_resolver = self.schema_resolver.clone();
                match avro::deserialize_slice_avro(avro, schema_registry, schema_resolver, msg)
                    .await
                {
                    Ok(iter) => Box::new(iter),
                    Err(e) => Box::new(
                        vec![Err(SourceError::other(
                            "Avro error",
                            format!("Avro deserialization failed: {}", e),
                        ))]
                        .into_iter(),
                    )
                        as Box<dyn Iterator<Item = Result<T, SourceError>> + Send>,
                }
            }
            _ => {
                let new_self = self.clone();
                Box::new(
                    FramingIterator::new(self.framing.clone(), msg)
                        .map(move |t| new_self.deserialize_single(t)),
                ) as Box<dyn Iterator<Item = Result<T, SourceError>> + Send>
            }
        }
    }

    pub fn get_format(&self) -> Arc<Format> {
        self.format.clone()
    }

    pub fn deserialize_single(&self, msg: &[u8]) -> Result<T, SourceError> {
        match &*self.format {
            Format::Json(json) => json::deserialize_slice_json(json, msg),
            Format::Avro(_) => unreachable!("avro should be handled by here"),
            Format::Parquet(_) => todo!("parquet is not supported as an input format"),
            Format::RawString(_) => deserialize_raw_string(msg),
        }
        .map_err(|e| SourceError::bad_data(format!("Failed to deserialize: {:?}", e)))
    }
}
