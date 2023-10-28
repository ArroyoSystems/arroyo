use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::Arc,
    time::Duration,
};

use anyhow::Result;
use anyhow::{anyhow, bail};
use arrow::datatypes::{DataType, IntervalMonthDayNanoType};
use arrow::{
    array::Decimal128Array,
    datatypes::{Field, IntervalDayTimeType},
};
use arrow_schema::{IntervalUnit, TimeUnit, DECIMAL128_MAX_PRECISION, DECIMAL_DEFAULT_SCALE};
use arroyo_rpc::{
    formats::{Format, JsonFormat, TimestampFormat},
    primitive_to_sql,
};
use datafusion::sql::sqlparser::ast::{DataType as SQLDataType, ExactNumberInfo, TimezoneInfo};

use arroyo_rpc::api_types::connections::{
    FieldType, PrimitiveType, SourceField, SourceFieldType, StructType,
};
use datafusion_common::ScalarValue;
use proc_macro2::{Ident, TokenStream};
use quote::quote;
use regex::Regex;
use syn::PathArguments::AngleBracketed;
use syn::{parse_quote, parse_str, GenericArgument, Type};

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
pub struct StructDef {
    pub name: Option<String>,
    generated: bool,
    pub fields: Vec<StructField>,
    pub format: Option<Format>,
}
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
pub struct StructPair {
    pub left: StructDef,
    pub right: StructDef,
}

impl StructDef {
    pub fn for_fields(fields: Vec<StructField>) -> Self {
        Self {
            name: None,
            generated: true,
            fields,
            format: None,
        }
    }

    pub fn for_name(name: Option<String>, fields: Vec<StructField>) -> Self {
        Self {
            generated: name.is_none(),
            name,
            fields,
            format: None,
        }
    }

    pub fn for_format(fields: Vec<StructField>, format: Option<Format>) -> Self {
        Self {
            name: None,
            generated: true,
            fields,
            format,
        }
    }

    pub fn new(
        name: Option<String>,
        generated: bool,
        fields: Vec<StructField>,
        format: Option<Format>,
    ) -> Self {
        Self {
            name,
            generated,
            fields,
            format,
        }
    }

    pub fn with_format(&self, format: Option<Format>) -> Self {
        let mut new = self.clone();
        new.format = format;
        new
    }

    pub fn struct_name(&self) -> String {
        match &self.name {
            Some(name) => name.clone(),
            None => {
                let mut s = DefaultHasher::new();
                self.hash(&mut s);
                format!("generated_struct_{}", s.finish())
            }
        }
    }

    pub fn struct_name_ident(&self) -> String {
        let name = self.struct_name();
        name.replace(":", "_")
    }

    pub fn def(&self, is_key: bool) -> String {
        let fields = self.fields.iter().map(|field| field.def(&self.format));
        let schema_name: Type = parse_str(&self.struct_name()).unwrap();
        let extra_derives = if is_key {
            quote!(#[derive(Eq,  Hash,  Ord)])
        } else {
            quote!()
        };
        quote! (
            #extra_derives
            #[derive(Clone, Debug, bincode::Encode, bincode::Decode, PartialEq,  PartialOrd, serde::Serialize, serde::Deserialize)]
            pub struct #schema_name {
                #(#fields)
                ,*
            }
        )
        .to_string()
    }

    pub fn all_structs(&self) -> Vec<StructDef> {
        if self.name.is_some() {
            return vec![];
        }
        let mut result = vec![self.clone()];
        result.extend(
            self.fields
                .iter()
                .filter_map(|field| match &field.data_type {
                    TypeDef::StructDef(details, _) => Some(details.all_structs()),
                    TypeDef::DataType(_, _) => None,
                })
                .flatten()
                .collect::<Vec<_>>(),
        );
        result
    }

    pub fn all_structs_including_named(&self) -> Vec<StructDef> {
        let mut result = vec![self.clone()];
        result.extend(
            self.fields
                .iter()
                .filter_map(|field| match &field.data_type {
                    TypeDef::StructDef(details, _) => Some(details.all_structs_including_named()),
                    TypeDef::DataType(_, _) => None,
                })
                .flatten()
                .collect::<Vec<_>>(),
        );
        result
    }

    pub fn all_names(&self) -> Vec<String> {
        if self.name.is_some() {
            return vec![];
        }
        let mut result = vec![self.struct_name()];
        result.extend(
            self.fields
                .iter()
                .filter_map(|field| match &field.data_type {
                    TypeDef::StructDef(details, _) => Some(details.all_names()),
                    TypeDef::DataType(_, _) => None,
                })
                .flatten()
                .collect::<Vec<_>>(),
        );
        result
    }

    pub fn get_type(&self) -> Type {
        parse_str(&self.struct_name()).unwrap()
    }

    pub fn get_field(&self, alias: Option<String>, name: &String) -> Result<StructField> {
        let field = self
            .fields
            .iter()
            .find(|field| field.name().eq(name) && field.alias.eq(&alias));
        match field {
            Some(field) => Ok(field.clone()),
            None => self
                .fields
                .iter()
                .find(|field| field.name().eq(name))
                .cloned()
                .ok_or_else(|| anyhow!("no field {:?} for struct {:?}", name, self)),
        }
    }
    // this is a hack
    pub fn truncated_return_type(&self, terms: usize) -> StructDef {
        let fields = self.fields.iter().take(terms).cloned().collect();
        StructDef::for_fields(fields)
    }

    pub fn generate_serializer_items(&self) -> TokenStream {
        let struct_type = self.get_type();
        let builder_name = format!("{}RecordBatchBuilder", self.struct_name_ident());
        let builder_ident: Ident = parse_str(&builder_name).expect(&builder_name);

        let fields = &self.fields;
        let field_definitions: Vec<TokenStream> = fields
            .iter()
            .map(|field| {
                let field_type = field.to_array_builder_type();
                let field_name = field.field_array_ident();
                quote! { #field_name: #field_type }
            })
            .collect();

        let schema_initializations: Vec<_> = fields
            .iter()
            .map(|field| {
                let field: Field = field.clone().into();
                StructField::get_field_literal(&field, field.is_nullable())
            })
            .collect();

        let field_initializations: Vec<TokenStream> = fields
            .iter()
            .map(|field| {
                let field_name = field.field_array_ident();
                let field_initialization = field.create_array_builder();
                quote! { #field_name : #field_initialization}
            })
            .collect();

        let field_appends: Vec<TokenStream> =
            fields.iter().map(|field| field.field_append()).collect();
        let field_nulls: Vec<TokenStream> = fields
            .iter()
            .map(|field| field.append_null_field())
            .collect();

        let field_flushes: Vec<TokenStream> =
            fields.iter().map(|field| field.field_flush()).collect();

        let nullable_schema_initializations: Vec<_> = fields
            .iter()
            .map(|field| {
                let field: Field = field.clone().into();
                StructField::get_field_literal(&field, true)
            })
            .collect();

        let schema_data_impl = if self.generated {
            // generate a SchemaData impl but only for generated types
            let name = self.struct_name();

            let to_raw_string = if self.fields.len() == 1
                && matches!(
                    self.fields[0].data_type,
                    TypeDef::DataType(DataType::Utf8, _)
                ) {
                let field = &self.fields[0].field_ident();
                if self.fields[0].nullable() {
                    quote! {
                        self.#field.as_ref().map(|v| v.as_bytes().to_vec())
                    }
                } else {
                    quote! {
                        Some(self.#field.as_bytes().to_vec())
                    }
                }
            } else {
                quote! { unimplemented!("to_raw_string is not implemented for this type") }
            };

            Some(quote! {
                impl arroyo_worker::SchemaData for #struct_type {
                    fn name() -> &'static str {
                        #name
                    }

                    fn schema() -> arrow::datatypes::Schema {
                        let fields: Vec<arrow::datatypes::Field> = vec![#(#schema_initializations,)*];
                        arrow::datatypes::Schema::new(fields)
                    }

                    fn to_raw_string(&self) -> Option<Vec<u8>> {
                        #to_raw_string
                    }
                }
            })
        } else {
            None
        };

        quote! {
            #schema_data_impl

            #[derive(Debug)]
            pub struct #builder_ident {
                schema: std::sync::Arc<arrow::datatypes::Schema>,
                #(#field_definitions,)*
            }

            impl Default for #builder_ident {
                fn default() -> Self {
                    let fields: Vec<arrow::datatypes::Field> = vec![#(#schema_initializations,)*];
                    #builder_ident {
                        schema: std::sync::Arc::new(arrow::datatypes::Schema::new(fields)),
                        #(#field_initializations,)*
                    }
                }
            }

            impl #builder_ident {
                fn nullable() -> Self {
                    let fields :Vec<arrow::datatypes::Field> = vec![#(#nullable_schema_initializations,)*];
                    #builder_ident {
                        schema: std::sync::Arc::new(arrow::datatypes::Schema::new(fields)),
                        #(#field_initializations,)*
                    }
                }
            }

            impl arroyo_types::RecordBatchBuilder for #builder_ident {
                type Data = #struct_type;
                fn add_data(&mut self, data: Option<#struct_type>) {
                    match data {
                        Some(data) => {#(#field_appends;)*},
                        None => {#(#field_nulls;)*},
                    }
                }

                fn flush(&mut self) -> arrow_array::RecordBatch {
                    arrow_array::RecordBatch::try_new(self.schema.clone(), vec![#(#field_flushes,)*]).unwrap()
                }

                fn schema(&self) -> std::sync::Arc<arrow::datatypes::Schema> {
                    self.schema.clone()
                }
            }
        }
    }

    pub(crate) fn field_types_match(&self, other: &StructDef) -> bool {
        if self.fields.len() != other.fields.len() {
            return false;
        }
        self.fields
            .iter()
            .zip(other.fields.iter())
            .all(|(a, b)| a.data_type == b.data_type)
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
pub struct StructField {
    pub name: String,
    field_name: String,
    ident: String,
    pub alias: Option<String>,
    pub data_type: TypeDef,
    pub renamed_from: Option<String>,
    pub original_type: Option<String>,
}

impl StructField {
    pub fn new(name: String, alias: Option<String>, data_type: TypeDef) -> Self {
        if let TypeDef::DataType(DataType::Struct(_), _) = &data_type {
            panic!("can't use DataType::Struct as a struct field, should be a StructType");
        }

        let qualified_name = match &alias {
            Some(alias) => format!("{}_{}", alias, name),
            None => name.clone(),
        };

        let re = Regex::new("[^a-zA-Z0-9_]").unwrap();
        let field_name = re.replace_all(&qualified_name, "_").to_string();

        let (ident, renamed_from) = match parse_str::<Ident>(&field_name) {
            Ok(_) => (field_name.clone(), None),
            Err(_) => (format!("r#_{}", field_name), Some(name.clone())),
        };

        Self {
            name,
            field_name,
            alias,
            data_type,
            ident,
            renamed_from,
            original_type: None,
        }
    }

    pub fn with_rename(
        name: String,
        alias: Option<String>,
        data_type: TypeDef,
        renamed_from: Option<String>,
        original_type: Option<String>,
    ) -> Self {
        Self {
            name: name.clone(),
            field_name: name.clone(),
            ident: name.clone(),
            alias,
            data_type,
            renamed_from,
            original_type,
        }
    }
}

/* this returns a duration with the same length as the postgres interval. */
pub fn interval_month_day_nanos_to_duration(serialized_value: i128) -> Duration {
    let (month, day, nanos) = IntervalMonthDayNanoType::to_parts(serialized_value);
    let years = month / 12;
    let extra_month = month % 12;
    let year_hours = 1461 * years * 24 / 4;
    let days_to_seconds = ((year_hours + 24 * (day + 30 * extra_month)) as u64) * 60 * 60;
    let nanos = nanos as u64;
    std::time::Duration::from_secs(days_to_seconds) + std::time::Duration::from_nanos(nanos)
}

impl From<StructField> for Field {
    fn from(struct_field: StructField) -> Self {
        let (dt, nullable) = match struct_field.data_type {
            TypeDef::StructDef(s, nullable) => (
                DataType::Struct(
                    s.fields
                        .into_iter()
                        .map(|f| {
                            let field: Field = f.into();
                            Arc::new(field)
                        })
                        .collect(),
                ),
                nullable,
            ),
            TypeDef::DataType(dt, nullable) => (dt, nullable),
        };
        Field::new(&struct_field.name, dt, nullable)
    }
}

impl From<SourceField> for StructField {
    fn from(f: SourceField) -> Self {
        let t = match f.field_type.r#type {
            FieldType::Primitive(pt) => TypeDef::DataType(
                match pt {
                    PrimitiveType::Int32 => DataType::Int32,
                    PrimitiveType::Int64 => DataType::Int64,
                    PrimitiveType::UInt32 => DataType::UInt32,
                    PrimitiveType::UInt64 => DataType::UInt64,
                    PrimitiveType::F32 => DataType::Float32,
                    PrimitiveType::F64 => DataType::Float64,
                    PrimitiveType::Bool => DataType::Boolean,
                    PrimitiveType::String => DataType::Utf8,
                    PrimitiveType::Bytes => DataType::Binary,
                    PrimitiveType::UnixMillis => DataType::Timestamp(TimeUnit::Millisecond, None),
                    PrimitiveType::UnixMicros => DataType::Timestamp(TimeUnit::Microsecond, None),
                    PrimitiveType::UnixNanos => DataType::Timestamp(TimeUnit::Nanosecond, None),
                    PrimitiveType::DateTime => DataType::Timestamp(TimeUnit::Microsecond, None),
                    PrimitiveType::Json => DataType::Utf8,
                },
                f.nullable,
            ),
            FieldType::Struct(s) => TypeDef::StructDef(
                StructDef::for_name(
                    s.name.clone(),
                    s.fields.into_iter().map(|t| t.into()).collect(),
                ),
                f.nullable,
            ),
        };

        StructField::new(f.field_name, None, t)
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
pub enum TypeDef {
    StructDef(StructDef, bool),
    DataType(DataType, bool),
}

fn rust_to_arrow(typ: &Type) -> std::result::Result<DataType, ()> {
    match typ {
        Type::Path(pat) => {
            let path: Vec<String> = pat
                .path
                .segments
                .iter()
                .map(|s| s.ident.to_string())
                .collect();

            match path.join("::").as_str() {
                "bool" => Ok(DataType::Boolean),
                "i8" => Ok(DataType::Int8),
                "i16" => Ok(DataType::Int16),
                "i32" => Ok(DataType::Int32),
                "i64" => Ok(DataType::Int64),
                "u8" => Ok(DataType::UInt8),
                "u16" => Ok(DataType::UInt16),
                "u32" => Ok(DataType::UInt32),
                "u64" => Ok(DataType::UInt64),
                "f16" => Ok(DataType::Float16),
                "f32" => Ok(DataType::Float32),
                "f64" => Ok(DataType::Float64),
                "String" => Ok(DataType::Utf8),
                "Vec<u8>" => Ok(DataType::Binary),
                "SystemTime" | "std::time::SystemTime" => {
                    Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
                }
                "Duration" | "std::time::Duration" => Ok(DataType::Duration(TimeUnit::Microsecond)),
                _ => Err(()),
            }
        }
        _ => Err(()),
    }
}

impl TryFrom<&Type> for TypeDef {
    type Error = ();

    fn try_from(typ: &Type) -> std::result::Result<Self, Self::Error> {
        match typ {
            Type::Path(pat) => {
                let last = pat.path.segments.last().unwrap();
                if last.ident == "Option" {
                    let AngleBracketed(args) = &last.arguments else {
                        return Err(());
                    };

                    let GenericArgument::Type(inner) = args.args.first().ok_or(())? else {
                        return Err(());
                    };

                    Ok(TypeDef::DataType(rust_to_arrow(inner)?, true))
                } else {
                    Ok(TypeDef::DataType(rust_to_arrow(typ)?, false))
                }
            }
            _ => Err(()),
        }
    }
}

impl TypeDef {
    pub fn is_optional(&self) -> bool {
        match self {
            TypeDef::StructDef(_, optional) => *optional,
            TypeDef::DataType(_, optional) => *optional,
        }
    }

    pub fn to_optional(self) -> Self {
        match self {
            TypeDef::StructDef(t, _) => TypeDef::StructDef(t, true),
            TypeDef::DataType(t, _) => TypeDef::DataType(t, true),
        }
    }

    // TODO: rename this
    pub fn return_type(&self) -> Type {
        if self.is_optional() {
            parse_str(&format!("Option<{}>", self.type_string())).unwrap()
        } else {
            parse_str(&self.type_string()).unwrap()
        }
    }
    pub fn type_string(&self) -> String {
        match self {
            TypeDef::StructDef(details, _) => details.struct_name(),
            TypeDef::DataType(data_type, _) => StructField::data_type_name(data_type),
        }
    }

    pub fn as_datatype(&self) -> Option<&DataType> {
        match self {
            TypeDef::StructDef(_, _) => None,
            TypeDef::DataType(dt, _) => Some(dt),
        }
    }

    pub fn is_float(&self) -> bool {
        match self {
            TypeDef::DataType(dt, _) => matches!(
                dt,
                DataType::Float16 | DataType::Float32 | DataType::Float64
            ),
            _ => false,
        }
    }

    pub fn get_literal(scalar: &ScalarValue) -> syn::Expr {
        if scalar.is_null() {
            return parse_quote!(None);
        }
        match scalar {
            ScalarValue::Null => parse_quote!(None),
            ScalarValue::Boolean(Some(value)) => parse_quote!(#value),
            ScalarValue::Float32(Some(value)) => parse_quote!(#value),
            ScalarValue::Float64(Some(value)) => parse_quote!(#value),
            ScalarValue::Decimal128(Some(value), precision, scale) => parse_str(
                &Decimal128Array::from_value(*value, 1)
                    .with_precision_and_scale(*precision, *scale)
                    .unwrap()
                    .value_as_string(0),
            )
            .unwrap(),
            ScalarValue::Int8(Some(value)) => parse_quote!(#value),
            ScalarValue::Int16(Some(value)) => parse_quote!(#value),
            ScalarValue::Int32(Some(value)) => parse_quote!(#value),
            ScalarValue::Int64(Some(value)) => parse_quote!(#value),
            ScalarValue::UInt8(Some(value)) => parse_quote!(#value),
            ScalarValue::UInt16(Some(value)) => parse_quote!(#value),
            ScalarValue::UInt32(Some(value)) => parse_quote!(#value),
            ScalarValue::UInt64(Some(value)) => parse_quote!(#value),
            ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => {
                parse_quote!(#value.to_string())
            }
            ScalarValue::Binary(Some(bin)) => parse_str(&format!("{:?}", bin)).unwrap(),
            ScalarValue::LargeBinary(_) => todo!(),
            ScalarValue::List(_, _) => todo!(),
            ScalarValue::Date32(Some(val)) => parse_str(&format!(
                "std::time::UNIX_EPOCH + std::time::Duration::from_days({})",
                val
            ))
            .unwrap(),
            ScalarValue::Date64(Some(val)) => parse_str(&format!(
                "std::time::UNIX_EPOCH + std::time::Duration::from_millis({})",
                val
            ))
            .unwrap(),
            ScalarValue::TimestampSecond(Some(secs), _) => {
                parse_quote!(std::time::UNIX_EPOCH + std::time::Duration::from_secs(#secs as u64))
            }
            ScalarValue::TimestampMillisecond(Some(millis), _) => {
                parse_quote!(std::time::UNIX_EPOCH + std::time::Duration::from_millis(#millis as u64))
            }
            ScalarValue::TimestampMicrosecond(Some(micros), _) => {
                parse_quote!(std::time::UNIX_EPOCH + std::time::Duration::from_micros(#micros as u64))
            }
            ScalarValue::TimestampNanosecond(Some(nanos), _) => {
                parse_quote!(std::time::UNIX_EPOCH + std::time::Duration::from_nanos(#nanos as u64))
            }
            ScalarValue::IntervalYearMonth(_) => todo!(),
            ScalarValue::IntervalDayTime(Some(val)) => {
                let (days, ms) = IntervalDayTimeType::to_parts(*val);
                parse_str(&format!(
                    "std::time::Duration::from_days({}) + std::time::Duration::from_millis({})",
                    days, ms
                ))
                .unwrap()
            }
            ScalarValue::IntervalDayTime(None) => todo!(),
            ScalarValue::IntervalMonthDayNano(Some(val)) => {
                let duration = interval_month_day_nanos_to_duration(*val);
                let seconds = duration.as_secs();
                let nanos = duration.subsec_nanos() as u64;
                parse_quote!((std::time::Duration::from_secs(#seconds) + std::time::Duration::from_nanos(#nanos)))
            }
            ScalarValue::Struct(_, _) => todo!(),
            ScalarValue::Dictionary(_, _) => todo!(),
            _ => todo!(),
        }
    }

    pub(crate) fn as_nullable(&self) -> Self {
        self.with_nullity(true)
    }

    pub(crate) fn with_nullity(&self, nullity: bool) -> Self {
        match self {
            TypeDef::StructDef(struct_def, _) => TypeDef::StructDef(struct_def.clone(), nullity),
            TypeDef::DataType(data_type, _) => TypeDef::DataType(data_type.clone(), nullity),
        }
    }
}

impl StructField {
    pub fn name(&self) -> String {
        self.name.to_string()
    }

    pub fn field_name(&self) -> String {
        self.field_name.clone()
    }

    pub fn qualified_name(&self) -> String {
        match &self.alias {
            Some(alias) => format!("{}_{}", alias, self.name),
            None => self.name.clone(),
        }
    }
    pub fn field_array_ident(&self) -> Ident {
        let field = &self.ident;
        parse_str(&format!("{}_array", field)).unwrap()
    }

    pub fn field_ident(&self) -> Ident {
        parse_str(&self.ident).unwrap_or_else(|e| panic!("invalid ident {}: {:?}", self.ident, e))
    }

    fn def(&self, format: &Option<Format>) -> TokenStream {
        let name: Ident = self.field_ident();
        let type_string = self.get_type();
        // special case time fields

        let mut attributes = vec![];
        if let Some(original) = &self.renamed_from {
            attributes.push(quote! {
                #[serde(rename = #original)]
            });
        }

        if let TypeDef::DataType(DataType::Timestamp(_, _), nullable) = self.data_type {
            match format.as_ref().map(|t| &*t) {
                Some(Format::Json(JsonFormat {
                    timestamp_format: TimestampFormat::UnixMillis,
                    ..
                })) => {
                    if nullable {
                        attributes.push(quote! {
                            #[serde(default)]
                            #[serde(with = "arroyo_worker::formats::json::opt_timestamp_as_millis")]
                        });
                    } else {
                        attributes.push(quote! {
                            #[serde(with = "arroyo_worker::formats::json::timestamp_as_millis")]
                        });
                    }
                }
                _ => {
                    if nullable {
                        attributes.push(quote! {
                            #[serde(default)]
                            #[serde(with = "arroyo_worker::formats::json::opt_timestamp_as_rfc3339")]
                        });
                    } else {
                        attributes.push(quote!(
                            #[serde(with = "arroyo_worker::formats::json::timestamp_as_rfc3339")]
                        ));
                    }
                }
            }
        } else if let Some("json") = self.original_type.as_ref().map(|i| i.as_str()) {
            if self.nullable() {
                attributes.push(quote!(
                    #[serde(default)]
                    #[serde(deserialize_with = "arroyo_worker::deserialize_raw_json_opt")]
                ))
            } else {
                attributes.push(quote! {
                    #[serde(deserialize_with = "arroyo_worker::deserialize_raw_json")]
                })
            }
        }

        quote! {
            #(#attributes )*
            pub #name: #type_string
        }
    }

    pub fn get_type(&self) -> Type {
        let type_string = match &self.data_type {
            TypeDef::StructDef(details, true) => {
                format!("Option<{}>", details.struct_name())
            }
            TypeDef::StructDef(details, false) => details.struct_name(),
            TypeDef::DataType(data_type, true) => {
                format!("Option<{}>", Self::data_type_name(data_type))
            }
            TypeDef::DataType(data_type, false) => Self::data_type_name(data_type),
        };
        parse_str(&type_string).unwrap()
    }

    fn get_field_literal(field: &Field, parent_nullable: bool) -> TokenStream {
        let name = field.name();
        let data_type = Self::get_data_type_literal(field.data_type(), parent_nullable);
        let nullable = field.is_nullable() || parent_nullable;
        quote!(arrow::datatypes::Field::new(#name, #data_type, #nullable))
    }

    fn get_data_type_literal(
        data_type: &arrow_schema::DataType,
        parent_nullable: bool,
    ) -> TokenStream {
        match data_type {
            DataType::Null => quote!(arrow::datatypes::DataType::Null),
            DataType::Boolean => quote!(arrow::datatypes::DataType::Boolean),
            DataType::Int8 => quote!(arrow::datatypes::DataType::Int8),
            DataType::Int16 => quote!(arrow::datatypes::DataType::Int16),
            DataType::Int32 => quote!(arrow::datatypes::DataType::Int32),
            DataType::Int64 => quote!(arrow::datatypes::DataType::Int64),
            DataType::UInt8 => quote!(arrow::datatypes::DataType::UInt8),
            DataType::UInt16 => quote!(arrow::datatypes::DataType::UInt16),
            DataType::UInt32 => quote!(arrow::datatypes::DataType::UInt32),
            DataType::UInt64 => quote!(arrow::datatypes::DataType::UInt64),
            DataType::Float16 => quote!(arrow::datatypes::DataType::Float16),
            DataType::Float32 => quote!(arrow::datatypes::DataType::Float32),
            DataType::Float64 => quote!(arrow::datatypes::DataType::Float64),
            DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None) => {
                quote!(arrow::datatypes::DataType::Timestamp(
                    arrow::datatypes::TimeUnit::Millisecond,
                    None
                ))
            }
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None) => {
                quote!(arrow::datatypes::DataType::Timestamp(
                    arrow::datatypes::TimeUnit::Microsecond,
                    None
                ))
            }
            DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None) => {
                quote!(arrow::datatypes::DataType::Timestamp(
                    arrow::datatypes::TimeUnit::Nanosecond,
                    None
                ))
            }
            DataType::Timestamp(_, _) => todo!(),
            DataType::Date32 => todo!(),
            DataType::Date64 => todo!(),
            DataType::Time32(_) => todo!(),
            DataType::Time64(_) => todo!(),
            DataType::Duration(_) => todo!(),
            DataType::Interval(_) => todo!(),
            DataType::Binary => todo!(),
            DataType::FixedSizeBinary(_) => todo!(),
            DataType::LargeBinary => todo!(),
            DataType::Utf8 => quote!(arrow::datatypes::DataType::Utf8),
            DataType::LargeUtf8 => todo!(),
            DataType::List(_) => todo!(),
            DataType::FixedSizeList(_, _) => todo!(),
            DataType::LargeList(_) => todo!(),
            DataType::Struct(struct_fields) => {
                if struct_fields.is_empty() {
                    return quote!(arrow::datatypes::DataType::Struct(
                        arrow::datatypes::Fields::default()
                    ));
                }
                let fields: Vec<_> = struct_fields
                    .iter()
                    .map(|field| Self::get_field_literal(field, parent_nullable))
                    .collect();
                quote!(arrow::datatypes::DataType::Struct(
                    vec![#(#fields),*].into()
                ))
            }
            DataType::Union(_, _) => todo!(),
            DataType::Dictionary(_, _) => todo!(),
            DataType::Decimal128(_, _) => todo!(),
            DataType::Decimal256(_, _) => todo!(),
            DataType::Map(_, _) => todo!(),
            DataType::RunEndEncoded(_, _) => todo!(),
        }
    }

    fn field_append(&self) -> TokenStream {
        let field_name = self.field_ident();
        let field_array_name = self.field_array_ident();
        match &self.data_type {
            TypeDef::StructDef(_, false) => {
                quote!(self.#field_array_name.add_data(Some(data.#field_name)))
            }
            TypeDef::StructDef(_, true) => {
                quote!(self.#field_array_name.add_data(data.#field_name))
            }
            TypeDef::DataType(
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                true,
            ) => {
                quote!(self.#field_array_name.append_option(data.#field_name.map(|time| arroyo_types::to_millis(time) as i64)))
            }
            TypeDef::DataType(
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                false,
            ) => {
                quote!(self.#field_array_name.append_value(arroyo_types::to_millis(data.#field_name) as i64))
            }
            TypeDef::DataType(
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
                true,
            ) => {
                quote!(self.#field_array_name.append_option(data.#field_name.map(|time| arroyo_types::to_micros(time) as i64)))
            }
            TypeDef::DataType(
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
                false,
            ) => {
                quote!(self.#field_array_name.append_value(arroyo_types::to_micros(data.#field_name) as i64))
            }
            TypeDef::DataType(
                DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
                false,
            ) => {
                quote!(self.#field_array_name.append_value(arroyo_types::to_nanos(data.#field_name) as i64))
            }
            TypeDef::DataType(
                DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
                true,
            ) => {
                quote!(self.#field_array_name.append_option(data.#field_name.map(|time| arroyo_types::to_nanos(time) as i64)))
            }
            TypeDef::DataType(_, true) => {
                quote!(self.#field_array_name.append_option(data.#field_name))
            }
            TypeDef::DataType(_, false) => {
                quote!(self.#field_array_name.append_value(data.#field_name))
            }
        }
    }

    pub(crate) fn data_type_name(data_type: &DataType) -> String {
        match data_type {
            DataType::Null => todo!(),
            DataType::Boolean => "bool".to_string(),
            DataType::Int8 => "i8".to_string(),
            DataType::Int16 => "i16".to_string(),
            DataType::Int32 => "i32".to_string(),
            DataType::Int64 => "i64".to_string(),
            DataType::UInt8 => "u8".to_string(),
            DataType::UInt16 => "u16".to_string(),
            DataType::UInt32 => "u32".to_string(),
            DataType::UInt64 => "u64".to_string(),
            DataType::Float16 => "f16".to_string(),
            DataType::Float32 => "f32".to_string(),
            DataType::Float64 => "f64".to_string(),
            DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64 => {
                "std::time::SystemTime".to_string()
            }
            DataType::Time32(_) => todo!(),
            DataType::Time64(_) => todo!(),
            DataType::Duration(_) | DataType::Interval(_) => "std::time::Duration".to_string(),
            DataType::Binary => todo!(),
            DataType::FixedSizeBinary(_) => todo!(),
            DataType::LargeBinary => todo!(),
            DataType::Utf8 => "String".to_string(),
            DataType::LargeUtf8 => todo!(),
            DataType::List(field) => {
                let list_data_type = Self::data_type_name(field.data_type());
                if field.is_nullable() {
                    format!("Vec<Option<{}>>", list_data_type)
                } else {
                    format!("Vec<{}>", list_data_type)
                }
            }
            DataType::FixedSizeList(_, _) => todo!(),
            DataType::LargeList(_) => todo!(),
            DataType::Struct(_) => unreachable!(),
            DataType::Union(_, _) => todo!(),
            DataType::Dictionary(_, _) => todo!(),
            DataType::Decimal128(_, _) => todo!(),
            DataType::Decimal256(_, _) => todo!(),
            DataType::Map(_, _) => todo!(),
            DataType::RunEndEncoded(_, _) => todo!(),
        }
    }

    fn create_array_builder(&self) -> TokenStream {
        match &self.data_type {
            TypeDef::StructDef(struct_type, false) => {
                let builder_name = format!("{}RecordBatchBuilder", struct_type.struct_name_ident());
                let builder_ident: Ident = parse_str(&builder_name).expect(&builder_name);
                quote!(#builder_ident::default())
            }
            TypeDef::StructDef(struct_type, true) => {
                let builder_name = format!("{}RecordBatchBuilder", struct_type.struct_name_ident());
                let builder_ident: Ident = parse_str(&builder_name).expect(&builder_name);
                quote!(#builder_ident::nullable())
            }
            TypeDef::DataType(data_type, _) => match data_type {
                DataType::Null => todo!(),
                DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float16
                | DataType::Float32
                | DataType::Float64
                | DataType::Timestamp(_, None) => {
                    let builder_type = self.to_array_builder_type();
                    quote!(#builder_type::with_capacity(1024))
                }
                DataType::Timestamp(_, _) => todo!(),
                DataType::Date32 => todo!(),
                DataType::Date64 => todo!(),
                DataType::Time32(_) => todo!(),
                DataType::Time64(_) => todo!(),
                DataType::Duration(_) => todo!(),
                DataType::Interval(_) => todo!(),
                DataType::Binary => todo!(),
                DataType::FixedSizeBinary(_) => todo!(),
                DataType::LargeBinary => todo!(),
                DataType::Utf8 => quote!(arrow_array::builder::GenericByteBuilder::<
                    arrow_array::types::GenericStringType<i32>,
                >::new()),
                DataType::LargeUtf8 => todo!(),
                DataType::List(_) => todo!(),
                DataType::FixedSizeList(_, _) => todo!(),
                DataType::LargeList(_) => todo!(),
                DataType::Struct(_) => todo!(),
                DataType::Union(_, _) => todo!(),
                DataType::Dictionary(_, _) => todo!(),
                DataType::Decimal128(_, _) => todo!(),
                DataType::Decimal256(_, _) => todo!(),
                DataType::Map(_, _) => todo!(),
                DataType::RunEndEncoded(_, _) => todo!(),
            },
        }
    }

    fn to_array_builder_type(&self) -> Type {
        let tokens = match &self.data_type {
            TypeDef::StructDef(struct_type, _) => {
                let builder_name = format!("{}RecordBatchBuilder", struct_type.struct_name_ident());
                parse_str(&builder_name).unwrap()
            }
            TypeDef::DataType(data_type, _) => match data_type {
                DataType::Null => todo!(),
                DataType::Boolean => quote!(arrow_array::builder::BooleanBuilder),
                DataType::Int8 => {
                    quote!(arrow_array::builder::PrimitiveBuilder::<arrow_array::types::Int8Type>)
                }
                DataType::Int16 => {
                    quote!(arrow_array::builder::PrimitiveBuilder::<arrow_array::types::Int16Type>)
                }
                DataType::Int32 => {
                    quote!(arrow_array::builder::PrimitiveBuilder::<arrow_array::types::Int32Type>)
                }
                DataType::Int64 => {
                    quote!(arrow_array::builder::PrimitiveBuilder::<arrow_array::types::Int64Type>)
                }
                DataType::UInt8 => {
                    quote!(arrow_array::builder::PrimitiveBuilder::<arrow_array::types::UInt8Type>)
                }
                DataType::UInt16 => {
                    quote!(arrow_array::builder::PrimitiveBuilder::<arrow_array::types::UInt16Type>)
                }
                DataType::UInt32 => {
                    quote!(arrow_array::builder::PrimitiveBuilder::<arrow_array::types::UInt32Type>)
                }
                DataType::UInt64 => {
                    quote!(arrow_array::builder::PrimitiveBuilder::<arrow_array::types::UInt64Type>)
                }
                DataType::Float16 => {
                    quote!(
                        arrow_array::builder::PrimitiveBuilder::<arrow_array::types::Float16Type>
                    )
                }
                DataType::Float32 => {
                    quote!(
                        arrow_array::builder::PrimitiveBuilder::<arrow_array::types::Float32Type>
                    )
                }
                DataType::Float64 => {
                    quote!(
                        arrow_array::builder::PrimitiveBuilder::<arrow_array::types::Float64Type>
                    )
                }
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None) => quote!(
                    arrow_array::builder::PrimitiveBuilder::<
                        arrow_array::types::TimestampMillisecondType,
                    >
                ),
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None) => quote!(
                    arrow_array::builder::PrimitiveBuilder::<
                        arrow_array::types::TimestampMicrosecondType,
                    >
                ),
                DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None) => quote!(
                    arrow_array::builder::PrimitiveBuilder::<
                        arrow_array::types::TimestampNanosecondType,
                    >
                ),
                DataType::Date32 => todo!(),
                DataType::Date64 => todo!(),
                DataType::Time32(_) => todo!(),
                DataType::Time64(_) => todo!(),
                DataType::Duration(_) => todo!(),
                DataType::Interval(_) => todo!(),
                DataType::Binary => todo!(),
                DataType::FixedSizeBinary(_) => todo!(),
                DataType::LargeBinary => todo!(),
                DataType::Utf8 => {
                    quote!(
                        arrow_array::builder::GenericByteBuilder<
                            arrow_array::types::GenericStringType<i32>,
                        >
                    )
                }
                DataType::LargeUtf8 => todo!(),
                DataType::List(_) => todo!(),
                DataType::FixedSizeList(_, _) => todo!(),
                DataType::LargeList(_) => todo!(),
                DataType::Struct(_) => todo!(),
                DataType::Union(_, _) => todo!(),
                DataType::Dictionary(_, _) => todo!(),
                DataType::Decimal128(_, _) => todo!(),
                DataType::Decimal256(_, _) => todo!(),
                _ => todo!("{:?}", self),
            },
        };
        parse_str(&tokens.to_string()).unwrap()
    }

    pub fn get_return_expression(&self, parent_ident: TokenStream) -> TokenStream {
        let ident: Ident = parse_str(&self.field_name()).unwrap();
        quote!(#parent_ident.#ident.clone())
    }

    pub fn as_nullable(&self) -> Self {
        StructField::new(
            self.name.clone(),
            self.alias.clone(),
            self.data_type.as_nullable(),
        )
    }

    pub(crate) fn nullable(&self) -> bool {
        self.data_type.is_optional()
    }

    fn append_null_field(&self) -> TokenStream {
        let array_field = self.field_array_ident();
        match self.data_type {
            TypeDef::StructDef(_, _) => quote!(self.#array_field.add_data(None)),
            TypeDef::DataType(_, _) => quote!(self.#array_field.append_null()),
        }
    }

    fn field_flush(&self) -> TokenStream {
        let array_field = self.field_array_ident();
        match self.data_type {
            TypeDef::StructDef(_, _) => {
                quote!({
                    let struct_array: arrow_array::StructArray = self.#array_field.flush().into();
                    std::sync::Arc::new(struct_array)
                })
            }
            TypeDef::DataType(_, _) => quote!(std::sync::Arc::new(self.#array_field.finish())),
        }
    }
}

pub(crate) fn data_type_as_syn_type(data_type: &DataType) -> syn::Type {
    match data_type {
        DataType::Null => parse_quote!(std::option::Option<()>),
        DataType::Boolean => parse_quote!(bool),
        DataType::Int8 => parse_quote!(i8),
        DataType::Int16 => parse_quote!(i16),
        DataType::Int32 => parse_quote!(i32),
        DataType::Int64 => parse_quote!(i64),
        DataType::UInt8 => parse_quote!(u8),
        DataType::UInt16 => parse_quote!(u16),
        DataType::UInt32 => parse_quote!(u32),
        DataType::UInt64 => parse_quote!(u64),
        DataType::Float16 => parse_quote!(f16),
        DataType::Float32 => parse_quote!(f32),
        DataType::Float64 => parse_quote!(f64),
        DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64 => {
            parse_quote!(std::time::SystemTime)
        }
        DataType::Time32(_) => todo!(),
        DataType::Time64(_) => todo!(),
        DataType::Duration(_) | DataType::Interval(_) => {
            parse_quote!(std::time::Duration)
        }
        DataType::Binary => todo!(),
        DataType::FixedSizeBinary(_) => todo!(),
        DataType::LargeBinary => todo!(),
        DataType::Utf8 => parse_quote!(String),
        DataType::LargeUtf8 => todo!(),
        DataType::List(field) => {
            let list_data_type = data_type_as_syn_type(field.data_type());
            if field.is_nullable() {
                parse_quote!(Vec<Option<#list_data_type>>)
            } else {
                parse_quote!(Vec<#list_data_type>)
            }
        }
        DataType::FixedSizeList(_, _) => todo!(),
        DataType::LargeList(_) => todo!(),
        DataType::Struct(_) => unreachable!(),
        DataType::Union(_, _) => todo!(),
        DataType::Dictionary(_, _) => todo!(),
        DataType::Decimal128(_, _) => todo!(),
        DataType::Decimal256(_, _) => todo!(),
        DataType::Map(_, _) => todo!(),
        DataType::RunEndEncoded(_, _) => todo!(),
    }
}

// Pulled from DataFusion

pub(crate) fn convert_data_type(sql_type: &SQLDataType) -> Result<DataType> {
    match sql_type {
        SQLDataType::Array(Some(inner_sql_type)) => {
            let data_type = convert_simple_data_type(inner_sql_type)?;

            Ok(DataType::List(Arc::new(Field::new(
                "field", data_type, true,
            ))))
        }
        SQLDataType::Array(None) => {
            bail!("Arrays with unspecified type is not supported".to_string())
        }
        other => convert_simple_data_type(other),
    }
}

fn convert_simple_data_type(sql_type: &SQLDataType) -> Result<DataType> {
    match sql_type {
        SQLDataType::Boolean => Ok(DataType::Boolean),
        SQLDataType::TinyInt(_) => Ok(DataType::Int8),
        SQLDataType::SmallInt(_) => Ok(DataType::Int16),
        SQLDataType::Int(_) | SQLDataType::Integer(_) => Ok(DataType::Int32),
        SQLDataType::BigInt(_) => Ok(DataType::Int64),
        SQLDataType::UnsignedTinyInt(_) => Ok(DataType::UInt8),
        SQLDataType::UnsignedSmallInt(_) => Ok(DataType::UInt16),
        SQLDataType::UnsignedInt(_) | SQLDataType::UnsignedInteger(_) => Ok(DataType::UInt32),
        SQLDataType::UnsignedBigInt(_) => Ok(DataType::UInt64),
        SQLDataType::Float(_) => Ok(DataType::Float32),
        SQLDataType::Real => Ok(DataType::Float32),
        SQLDataType::Double | SQLDataType::DoublePrecision => Ok(DataType::Float64),
        SQLDataType::Char(_)
        | SQLDataType::Varchar(_)
        | SQLDataType::Text
        | SQLDataType::String => Ok(DataType::Utf8),
        SQLDataType::Timestamp(None, TimezoneInfo::None) | SQLDataType::Datetime(_) => {
            Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
        }
        SQLDataType::Date => Ok(DataType::Date32),
        SQLDataType::Time(None, tz_info) => {
            if matches!(tz_info, TimezoneInfo::None)
                || matches!(tz_info, TimezoneInfo::WithoutTimeZone)
            {
                Ok(DataType::Time64(TimeUnit::Nanosecond))
            } else {
                // We dont support TIMETZ and TIME WITH TIME ZONE for now
                bail!(format!("Unsupported Timestamp SQL type {sql_type:?}"))
            }
        }
        SQLDataType::Numeric(exact_number_info) | SQLDataType::Decimal(exact_number_info) => {
            let (precision, scale) = match *exact_number_info {
                ExactNumberInfo::None => (None, None),
                ExactNumberInfo::Precision(precision) => (Some(precision), None),
                ExactNumberInfo::PrecisionAndScale(precision, scale) => {
                    (Some(precision), Some(scale))
                }
            };
            make_decimal_type(precision, scale)
        }
        SQLDataType::Bytea => Ok(DataType::Binary),
        SQLDataType::Interval => Ok(DataType::Interval(IntervalUnit::MonthDayNano)),
        SQLDataType::JSON => bail!("JSON data type is not supported yet".to_string()),
        _ => bail!(format!("Unsupported SQL type {sql_type:?}")),
    }
}

/// Returns a validated `DataType` for the specified precision and
/// scale
pub(crate) fn make_decimal_type(precision: Option<u64>, scale: Option<u64>) -> Result<DataType> {
    // postgres like behavior
    let (precision, scale) = match (precision, scale) {
        (Some(p), Some(s)) => (p as u8, s as i8),
        (Some(p), None) => (p as u8, 0),
        (None, Some(_)) => {
            bail!("Cannot specify only scale for decimal data type".to_string())
        }
        (None, None) => (DECIMAL128_MAX_PRECISION, DECIMAL_DEFAULT_SCALE),
    };

    // Arrow decimal is i128 meaning 38 maximum decimal digits
    if precision == 0 || precision > DECIMAL128_MAX_PRECISION || scale.unsigned_abs() > precision {
        bail!(
            "Decimal(precision = {precision}, scale = {scale}) should satisfy `0 < precision <= 38`, and `scale <= precision`."
        )
    } else {
        Ok(DataType::Decimal128(precision, scale))
    }
}

impl TryFrom<StructField> for SourceField {
    type Error = String;

    fn try_from(f: StructField) -> Result<Self, Self::Error> {
        let field_name = f.name();
        let nullable = f.nullable();
        let (field_type, sql_name) = match f.data_type {
            TypeDef::StructDef(StructDef { name, fields, .. }, _) => {
                let fields: Result<_, String> = fields.into_iter().map(|f| f.try_into()).collect();

                let st = StructType {
                    name: name.clone(),
                    fields: fields?,
                };

                (FieldType::Struct(st), name)
            }
            TypeDef::DataType(dt, _) => {
                let pt = match dt {
                    DataType::Boolean => Ok(PrimitiveType::Bool),
                    DataType::Int32 => Ok(PrimitiveType::Int32),
                    DataType::Int64 => Ok(PrimitiveType::Int64),
                    DataType::UInt32 => Ok(PrimitiveType::UInt32),
                    DataType::UInt64 => Ok(PrimitiveType::UInt64),
                    DataType::Float32 => Ok(PrimitiveType::F32),
                    DataType::Float64 => Ok(PrimitiveType::F64),
                    DataType::Binary | DataType::LargeBinary => Ok(PrimitiveType::Bytes),
                    DataType::Timestamp(TimeUnit::Millisecond, _) => Ok(PrimitiveType::UnixMillis),
                    DataType::Timestamp(TimeUnit::Microsecond, _) => Ok(PrimitiveType::UnixMicros),
                    DataType::Timestamp(TimeUnit::Nanosecond, _) => Ok(PrimitiveType::UnixNanos),
                    DataType::Utf8 => Ok(PrimitiveType::String),
                    dt => Err(format!("Unsupported data type {:?}", dt)),
                }?;

                (
                    FieldType::Primitive(pt.clone()),
                    Some(primitive_to_sql(pt).to_string()),
                )
            }
        };

        Ok(SourceField {
            field_name,
            field_type: SourceFieldType {
                r#type: field_type,
                sql_name,
            },
            nullable,
        })
    }
}
