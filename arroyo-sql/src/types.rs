use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use anyhow::anyhow;
use anyhow::Result;
use arrow::datatypes::DataType;
use arrow::{
    array::Decimal128Array,
    datatypes::{Field, IntervalDayTimeType},
};

use datafusion_common::ScalarValue;
use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};
use regex::Regex;
use syn::{parse_str, Type};

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct StructDef {
    pub name: Option<String>,
    pub fields: Vec<StructField>,
}

impl StructDef {
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

    pub fn def(&self, is_key: bool) -> String {
        let fields = self.fields.iter().map(|field| field.def());
        let schema_name: Type = parse_str(&self.struct_name()).unwrap();
        let extra_derives = if is_key {
            quote!(#[derive(Eq,  Hash,  Ord)])
        } else {
            quote!()
        };
        quote! (
            #extra_derives
            #[derive(Clone, Debug, bincode::Encode, bincode::Decode, PartialEq,  PartialOrd, serde::Serialize)]
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

    pub fn get_field(&self, alias: Option<&String>, name: &String) -> Result<StructField> {
        let field = self
            .fields
            .iter()
            .find(|field| field.name().eq(name) && field.alias.as_ref().eq(&alias));
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
        let fields = self
            .fields
            .iter()
            .take(terms)
            .map(|field| field.clone())
            .collect();
        StructDef { name: None, fields }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct StructField {
    pub name: String,
    pub alias: Option<String>,
    pub data_type: TypeDef,
}

impl From<StructField> for Field {
    fn from(struct_field: StructField) -> Self {
        let (dt, nullable) = match struct_field.data_type {
            TypeDef::StructDef(s, nullable) => (
                DataType::Struct(s.fields.into_iter().map(|f| f.into()).collect()),
                nullable,
            ),
            TypeDef::DataType(dt, nullable) => (dt, nullable),
        };
        Field::new(&struct_field.name, dt, nullable)
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum TypeDef {
    StructDef(StructDef, bool),
    DataType(DataType, bool),
}

impl TypeDef {
    pub fn is_optional(&self) -> bool {
        match self {
            TypeDef::StructDef(_, optional) => *optional,
            TypeDef::DataType(_, optional) => *optional,
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
            TypeDef::DataType(data_type, _) => StructField::data_type_name(data_type).to_string(),
        }
    }

    pub fn get_literal(scalar: &ScalarValue) -> syn::Expr {
        if scalar.is_null() {
            return parse_str("None").unwrap();
        }
        match scalar {
            ScalarValue::Null => parse_str("None").unwrap(),
            ScalarValue::Boolean(Some(value)) => parse_str(&format!("{}", value)).unwrap(),
            ScalarValue::Float32(Some(value)) => parse_str(&format!("{}", value)).unwrap(),
            ScalarValue::Float64(Some(value)) => parse_str(&format!("{}", value)).unwrap(),
            ScalarValue::Decimal128(Some(value), precision, scale) => parse_str(
                &Decimal128Array::from_value(*value, 1)
                    .with_precision_and_scale(*precision, *scale)
                    .unwrap()
                    .value_as_string(0),
            )
            .unwrap(),
            ScalarValue::Int8(Some(val)) => parse_str(&val.to_string()).unwrap(),
            ScalarValue::Int16(Some(val)) => parse_str(&val.to_string()).unwrap(),
            ScalarValue::Int32(Some(val)) => parse_str(&val.to_string()).unwrap(),
            ScalarValue::Int64(Some(val)) => parse_str(&val.to_string()).unwrap(),
            ScalarValue::UInt8(Some(val)) => parse_str(&val.to_string()).unwrap(),
            ScalarValue::UInt16(Some(val)) => parse_str(&val.to_string()).unwrap(),
            ScalarValue::UInt32(Some(val)) => parse_str(&val.to_string()).unwrap(),
            ScalarValue::UInt64(Some(val)) => parse_str(&val.to_string()).unwrap(),
            ScalarValue::Utf8(Some(val)) | ScalarValue::LargeUtf8(Some(val)) => {
                parse_str(&quote!(#val.to_string()).to_string()).unwrap()
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
            ScalarValue::TimestampSecond(_, _) => todo!(),
            ScalarValue::TimestampMillisecond(_, _) => todo!(),
            ScalarValue::TimestampMicrosecond(_, _) => todo!(),
            ScalarValue::TimestampNanosecond(_, _) => todo!(),
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
            ScalarValue::IntervalMonthDayNano(_) => todo!(),
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
        let re = Regex::new("[^a-zA-Z0-9_]").unwrap();
        re.replace_all(&self.qualified_name(), "_").to_string()
    }

    pub fn qualified_name(&self) -> String {
        match &self.alias {
            Some(alias) => format!("{}_{}", alias, self.name),
            None => self.name.clone(),
        }
    }

    pub fn field_ident(&self) -> Ident {
        match parse_str(&self.field_name()) {
            Ok(ident) => ident,
            Err(_) => {
                format_ident!("r#{}", self.field_name())
            }
        }
    }

    fn def(&self) -> TokenStream {
        let name: Ident = self.field_ident();
        let type_string = self.get_type();
        quote!(pub #name: #type_string)
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
            TypeDef::DataType(data_type, false) => Self::data_type_name(data_type).to_string(),
        };
        parse_str(&type_string).unwrap()
    }

    pub(crate) fn data_type_name(data_type: &DataType) -> &str {
        match data_type {
            DataType::Null => todo!(),
            DataType::Boolean => "bool",
            DataType::Int8 => "i8",
            DataType::Int16 => "i16",
            DataType::Int32 => "i32",
            DataType::Int64 => "i64",
            DataType::UInt8 => "u8",
            DataType::UInt16 => "u16",
            DataType::UInt32 => "u32",
            DataType::UInt64 => "u64",
            DataType::Float16 => "f16",
            DataType::Float32 => "f32",
            DataType::Float64 => "f64",
            DataType::Timestamp(_, _) => "std::time::SystemTime",
            DataType::Date32 => "std::time::SystemTime",
            DataType::Date64 => "std::time::SystemTime",
            DataType::Time32(_) => todo!(),
            DataType::Time64(_) => todo!(),
            DataType::Duration(_) => "std::time::Duration",
            DataType::Interval(_) => "std::time::Duration",
            DataType::Binary => todo!(),
            DataType::FixedSizeBinary(_) => todo!(),
            DataType::LargeBinary => todo!(),
            DataType::Utf8 => "String",
            DataType::LargeUtf8 => todo!(),
            DataType::List(_) => todo!(),
            DataType::FixedSizeList(_, _) => todo!(),
            DataType::LargeList(_) => todo!(),
            DataType::Struct(_) => unreachable!(),
            DataType::Union(_, _, _) => todo!(),
            DataType::Dictionary(_, _) => todo!(),
            DataType::Decimal128(_, _) => todo!(),
            DataType::Decimal256(_, _) => todo!(),
            DataType::Map(_, _) => todo!(),
            DataType::RunEndEncoded(_, _) => todo!(),
        }
    }

    pub fn get_return_expression(&self, parent_ident: TokenStream) -> TokenStream {
        let ident: Ident = parse_str(&self.field_name()).unwrap();
        quote!(#parent_ident.#ident.clone())
    }

    pub fn as_nullable(&self) -> Self {
        StructField {
            name: self.name.clone(),
            alias: self.alias.clone(),
            data_type: self.data_type.as_nullable(),
        }
    }

    pub(crate) fn nullable(&self) -> bool {
        self.data_type.is_optional()
    }
}
