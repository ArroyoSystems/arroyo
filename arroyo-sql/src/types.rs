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
use datafusion::sql::sqlparser::ast::{DataType as SQLDataType, ExactNumberInfo, TimezoneInfo};

use datafusion_common::ScalarValue;
use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};
use regex::Regex;
use syn::PathArguments::AngleBracketed;
use syn::{parse_quote, parse_str, FnArg, GenericArgument, Type};

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct StructDef {
    pub name: Option<String>,
    pub fields: Vec<StructField>,
}
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct StructPair {
    pub left: StructDef,
    pub right: StructDef,
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
        StructDef { name: None, fields }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct StructField {
    pub name: String,
    pub alias: Option<String>,
    pub data_type: TypeDef,
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

// quote a duration as a syn::Expr
pub fn duration_to_syn_expr(duration: Duration) -> syn::Expr {
    let secs = duration.as_secs();
    let nanos = duration.subsec_nanos();

    parse_quote!(std::time::Duration::new(#secs, #nanos))
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

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum TypeDef {
    StructDef(StructDef, bool),
    DataType(DataType, bool),
}

fn rust_to_arrow(typ: &Type) -> std::result::Result<DataType, ()> {
    match typ {
        Type::Path(pat) => match pat.path.get_ident().ok_or(())?.to_string().as_str() {
            "u64" => Ok(DataType::UInt64),
            _ => Err(()),
        },
        _ => Err(()),
    }
}

impl TryFrom<&Type> for TypeDef {
    type Error = ();

    fn try_from(typ: &Type) -> std::result::Result<Self, Self::Error> {
        match typ {
            Type::Path(pat) => {
                let last = pat.path.segments.last().unwrap();
                if last.ident.to_string() == "Option" {
                    let AngleBracketed(args) = &last.arguments else {
                        return Err(())
                    };

                    let GenericArgument::Type(inner) = args.args.first().ok_or(())? else {
                        return Err(())
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

    pub fn as_datatype(&self) -> Option<&DataType> {
        match self {
            TypeDef::StructDef(_, _) => None,
            TypeDef::DataType(dt, _) => Some(dt),
        }
    }

    pub fn get_literal(scalar: &ScalarValue) -> syn::Expr {
        if scalar.is_null() {
            return parse_quote!("None");
        }
        match scalar {
            ScalarValue::Null => parse_quote!("None"),
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
            DataType::Union(_, _) => todo!(),
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
        SQLDataType::Timestamp(None, TimezoneInfo::None) => {
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
                bail!(format!("Unsupported SQL type {sql_type:?}"))
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

pub fn rust_to_datafusion(typ: &Type) -> Option<DataType> {
    match typ {
        Type::Path(pat) => match pat.path.get_ident()?.to_string().as_str() {
            "u64" => Some(DataType::UInt64),
            _ => None,
        },
        _ => None,
    }
}