use crate::{
    operators::TwoPhaseAggregation,
    pipeline::SortDirection,
    types::{StructDef, StructField, TypeDef},
    ArroyoSchemaProvider,
};
use anyhow::{anyhow, bail, Ok, Result};
use arrow::datatypes::DataType;
use arrow_schema::{Field, TimeUnit};
use arroyo_types::{DatePart, DateTruncPrecision};
use datafusion_common::ScalarValue;
use datafusion_expr::{
    aggregate_function,
    expr::{AggregateFunction, ScalarFunction, ScalarUDF, Sort},
    type_coercion::aggregates::{avg_return_type, sum_return_type},
    BinaryExpr, BuiltinScalarFunction, Expr, TryCast,
};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use regex::Regex;
use std::{fmt::Debug, sync::Arc};
use syn::{parse_quote, parse_str, Ident, Path};

#[derive(Debug, Clone)]
pub struct BinaryOperator(datafusion_expr::Operator);

#[derive(Debug, Clone)]
pub enum Expression {
    Column(ColumnExpression),
    UnaryBoolean(UnaryBooleanExpression),
    Literal(LiteralExpression),
    BinaryComparison(BinaryComparisonExpression),
    BinaryMath(BinaryMathExpression),
    StructField(StructFieldExpression),
    Aggregation(AggregationExpression),
    Cast(CastExpression),
    Numeric(NumericExpression),
    Date(DateTimeFunction),
    String(StringFunction),
    Hash(HashExpression),
    DataStructure(DataStructureFunction),
    Json(JsonExpression),
    RustUdf(RustUdfExpression),
    WrapType(WrapTypeExpression),
    Case(CaseExpression),
}

impl Expression {
    pub fn to_syn_expression(&self) -> syn::Expr {
        match self {
            Expression::Column(column_expression) => column_expression.to_syn_expression(),
            Expression::UnaryBoolean(unary_boolean_expression) => {
                unary_boolean_expression.to_syn_expression()
            }
            Expression::Literal(literal_expression) => literal_expression.to_syn_expression(),
            Expression::BinaryComparison(comparison_expression) => {
                comparison_expression.to_syn_expression()
            }
            Expression::BinaryMath(math_expression) => math_expression.to_syn_expression(),
            Expression::StructField(struct_field_expression) => {
                struct_field_expression.to_syn_expression()
            }
            Expression::Aggregation(aggregation_expression) => {
                aggregation_expression.to_syn_expression()
            }
            Expression::Cast(cast_expression) => cast_expression.to_syn_expression(),
            Expression::Numeric(numeric_expression) => numeric_expression.to_syn_expression(),
            Expression::String(string_function) => string_function.to_syn_expression(),
            Expression::Hash(hash_expression) => hash_expression.to_syn_expression(),
            Expression::DataStructure(data_structure_expression) => {
                data_structure_expression.to_syn_expression()
            }
            Expression::Json(json_function) => json_function.to_syn_expression(),
            Expression::RustUdf(t) => t.to_syn_expression(),
            Expression::WrapType(t) => t.to_syn_expression(),
            Expression::Case(case_expression) => case_expression.to_syn_expression(),
            Expression::Date(datetime_expr) => datetime_expr.to_syn_expression(),
        }
    }

    fn syn_expression_with_nullity(&self, nullity: bool) -> syn::Expr {
        let expr = self.to_syn_expression();
        match (self.nullable(), nullity) {
            (true, true) | (false, false) => expr,
            (false, true) => parse_quote!(Some(#expr)),
            (true, false) => unreachable!(
                "Should not be possible to have a nullable expression with nullity=false"
            ),
        }
    }

    pub fn return_type(&self) -> TypeDef {
        match self {
            Expression::Column(column_expression) => column_expression.return_type(),
            Expression::UnaryBoolean(unary_boolean_expression) => {
                unary_boolean_expression.return_type()
            }
            Expression::Literal(literal_expression) => literal_expression.return_type(),
            Expression::BinaryComparison(comparison_expression) => {
                comparison_expression.return_type()
            }
            Expression::BinaryMath(math_expression) => math_expression.return_type(),
            Expression::StructField(struct_field_expression) => {
                struct_field_expression.return_type()
            }
            Expression::Aggregation(aggregation_expression) => aggregation_expression.return_type(),
            Expression::Cast(cast_expression) => cast_expression.return_type(),
            Expression::Numeric(numeric_expression) => numeric_expression.return_type(),
            Expression::Date(date_function) => date_function.return_type(),
            Expression::String(string_function) => string_function.return_type(),
            Expression::Hash(hash_expression) => hash_expression.return_type(),
            Expression::DataStructure(data_structure_expression) => {
                data_structure_expression.return_type()
            }
            Expression::Json(json_function) => json_function.return_type(),
            Expression::RustUdf(t) => t.return_type(),
            Expression::WrapType(t) => t.return_type(),
            Expression::Case(case_statement) => case_statement.return_type(),
        }
    }

    pub fn nullable(&self) -> bool {
        self.return_type().is_optional()
    }

    pub(crate) fn has_max_value(&self, field: &StructField) -> Option<u64> {
        match self {
            Expression::BinaryComparison(BinaryComparisonExpression { left, op, right }) => {
                if let BinaryComparison::And = op {
                    match (left.has_max_value(field), right.has_max_value(field)) {
                        (None, None) => {}
                        (None, Some(max)) | (Some(max), None) => return Some(max),
                        (Some(left), Some(right)) => return Some(left.min(right)),
                    }
                }
                if let BinaryComparison::Or = op {
                    if let (Some(left), Some(right)) =
                        (left.has_max_value(field), right.has_max_value(field))
                    {
                        return Some(left.max(right));
                    }
                }
                match (left.as_ref(), right.as_ref()) {
                    (
                        Expression::Column(ColumnExpression { column_field }),
                        Expression::Literal(LiteralExpression { literal }),
                    ) => {
                        if field == column_field {
                            match (op, literal) {
                                (BinaryComparison::Lt, ScalarValue::UInt64(Some(max))) => {
                                    Some(*max - 1)
                                }
                                (BinaryComparison::LtEq, ScalarValue::UInt64(Some(max))) => {
                                    Some(*max)
                                }
                                (BinaryComparison::Eq, ScalarValue::UInt64(Some(max))) => {
                                    Some(*max)
                                }
                                _ => None,
                            }
                        } else {
                            None
                        }
                    }
                    (
                        Expression::Literal(LiteralExpression { literal }),
                        Expression::Column(ColumnExpression { column_field }),
                    ) => {
                        if field == column_field {
                            match (op, literal) {
                                (BinaryComparison::Gt, ScalarValue::UInt64(Some(max))) => {
                                    Some(*max + 1)
                                }
                                (BinaryComparison::GtEq, ScalarValue::UInt64(Some(max))) => {
                                    Some(*max)
                                }
                                (BinaryComparison::Eq, ScalarValue::UInt64(Some(max))) => {
                                    Some(*max)
                                }
                                _ => None,
                            }
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }
}

pub struct ExpressionContext<'a> {
    pub schema_provider: &'a ArroyoSchemaProvider,
    pub input_struct: &'a StructDef,
}

impl<'a> ExpressionContext<'a> {
    pub fn compile_expr(&self, expression: &Expr) -> Result<Expression> {
        match expression {
            Expr::Alias(expr, _alias) => self.compile_expr(expr),
            Expr::Column(column) => Ok(Expression::Column(ColumnExpression::from_column(
                column,
                self.input_struct,
            )?)),
            Expr::Literal(literal) => Ok(LiteralExpression::new(literal.clone())),
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
                datafusion_expr::Operator::Eq
                | datafusion_expr::Operator::NotEq
                | datafusion_expr::Operator::Lt
                | datafusion_expr::Operator::LtEq
                | datafusion_expr::Operator::Gt
                | datafusion_expr::Operator::GtEq
                | datafusion_expr::Operator::IsDistinctFrom
                | datafusion_expr::Operator::IsNotDistinctFrom
                | datafusion_expr::Operator::And
                | datafusion_expr::Operator::Or => Ok(BinaryComparisonExpression::new(
                    Box::new(self.compile_expr(left)?),
                    *op,
                    Box::new(self.compile_expr(right)?),
                )?),
                datafusion_expr::Operator::Plus
                | datafusion_expr::Operator::Minus
                | datafusion_expr::Operator::Multiply
                | datafusion_expr::Operator::Divide
                | datafusion_expr::Operator::Modulo => BinaryMathExpression::new(
                    Box::new(self.compile_expr(left)?),
                    *op,
                    Box::new(self.compile_expr(right)?),
                ),
                datafusion_expr::Operator::StringConcat => {
                    Ok(Expression::String(StringFunction::Concat(vec![
                        self.compile_expr(left)?,
                        self.compile_expr(right)?,
                    ])))
                }
                datafusion_expr::Operator::RegexMatch
                | datafusion_expr::Operator::RegexIMatch
                | datafusion_expr::Operator::RegexNotMatch
                | datafusion_expr::Operator::RegexNotIMatch
                | datafusion_expr::Operator::BitwiseAnd
                | datafusion_expr::Operator::BitwiseOr
                | datafusion_expr::Operator::BitwiseXor
                | datafusion_expr::Operator::BitwiseShiftRight
                | datafusion_expr::Operator::BitwiseShiftLeft => bail!("{:?} is unimplemented", op),
            },
            Expr::Not(_) => bail!("NOT is unimplemented"),
            Expr::IsNotNull(expr) => Ok(UnaryBooleanExpression::new(
                UnaryOperator::IsNotNull,
                Box::new(self.compile_expr(expr)?),
            )),
            Expr::IsNull(expr) => Ok(UnaryBooleanExpression::new(
                UnaryOperator::IsNull,
                Box::new(self.compile_expr(expr)?),
            )),
            Expr::IsTrue(expr) => Ok(UnaryBooleanExpression::new(
                UnaryOperator::IsTrue,
                Box::new(self.compile_expr(expr)?),
            )),
            Expr::IsFalse(expr) => Ok(UnaryBooleanExpression::new(
                UnaryOperator::IsFalse,
                Box::new(self.compile_expr(expr)?),
            )),
            Expr::IsUnknown(expr) => Ok(UnaryBooleanExpression::new(
                UnaryOperator::IsUnknown,
                Box::new(self.compile_expr(expr)?),
            )),
            Expr::IsNotTrue(expr) => Ok(UnaryBooleanExpression::new(
                UnaryOperator::IsNotTrue,
                Box::new(self.compile_expr(expr)?),
            )),
            Expr::IsNotFalse(expr) => Ok(UnaryBooleanExpression::new(
                UnaryOperator::IsNotFalse,
                Box::new(self.compile_expr(expr)?),
            )),
            Expr::IsNotUnknown(expr) => Ok(UnaryBooleanExpression::new(
                UnaryOperator::IsNotUnknown,
                Box::new(self.compile_expr(expr)?),
            )),
            Expr::Negative(expr) => Ok(UnaryBooleanExpression::new(
                UnaryOperator::Negative,
                Box::new(self.compile_expr(expr)?),
            )),
            Expr::GetIndexedField(datafusion_expr::GetIndexedField { expr, key }) => {
                StructFieldExpression::new(Box::new(self.compile_expr(expr)?), key)
            }
            Expr::AggregateFunction(datafusion_expr::expr::AggregateFunction {
                fun,
                args,
                distinct,
                filter,
                order_by,
            }) => {
                if args.len() != 1 {
                    bail!("multiple aggregation parameters is not yet supported");
                }

                if filter.is_some() {
                    bail!("filters in aggregations is not yet supported");
                }
                if order_by.is_some() {
                    bail!("order by in aggregations is not yet supported");
                }

                Ok(AggregationExpression::new(
                    Box::new(self.compile_expr(&args[0])?),
                    fun.clone(),
                    *distinct,
                )?)
            }
            Expr::AggregateUDF { .. } => bail!("aggregate UDFs not supported"),
            Expr::Case(datafusion_expr::Case {
                expr,
                when_then_expr,
                else_expr,
            }) => {
                let expr = expr
                    .as_ref()
                    .map(|e| Ok(Box::new(self.compile_expr(e)?)))
                    .transpose()?;
                let when_then_expr = when_then_expr
                    .iter()
                    .map(|(when, then)| {
                        Ok((
                            Box::new(self.compile_expr(when)?),
                            Box::new(self.compile_expr(then)?),
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let else_expr = else_expr
                    .as_ref()
                    .map(|e| Ok(Box::new(self.compile_expr(e)?)))
                    .transpose()?;

                Ok(Expression::Case(CaseExpression::new(
                    expr,
                    when_then_expr,
                    else_expr,
                )))
            }
            Expr::Cast(datafusion_expr::Cast { expr, data_type }) => Ok(CastExpression::new(
                Box::new(self.compile_expr(expr)?),
                data_type,
            )?),
            Expr::TryCast(TryCast { expr, data_type }) => {
                bail!(
                    "try cast not implemented yet expr:{:?}, data_type:{:?}",
                    expr,
                    data_type
                )
            }
            Expr::ScalarFunction(ScalarFunction { fun, args }) => {
                let mut arg_expressions: Vec<_> = args
                    .iter()
                    .map(|arg| self.compile_expr(arg))
                    .collect::<Result<Vec<_>>>()?;
                //let arg_expression = Box::new(self.compile_expr(&args[0])?);
                match fun {
                    BuiltinScalarFunction::Abs
                    | BuiltinScalarFunction::Acos
                    | BuiltinScalarFunction::Asin
                    | BuiltinScalarFunction::Atan
                    | BuiltinScalarFunction::Acosh
                    | BuiltinScalarFunction::Asinh
                    | BuiltinScalarFunction::Atanh
                    | BuiltinScalarFunction::Cos
                    | BuiltinScalarFunction::Cosh
                    | BuiltinScalarFunction::Ln
                    | BuiltinScalarFunction::Log
                    | BuiltinScalarFunction::Log10
                    | BuiltinScalarFunction::Sin
                    | BuiltinScalarFunction::Sinh
                    | BuiltinScalarFunction::Sqrt
                    | BuiltinScalarFunction::Tan
                    | BuiltinScalarFunction::Tanh
                    | BuiltinScalarFunction::Ceil
                    | BuiltinScalarFunction::Floor
                    | BuiltinScalarFunction::Round
                    | BuiltinScalarFunction::Signum
                    | BuiltinScalarFunction::Trunc
                    | BuiltinScalarFunction::Log2
                    | BuiltinScalarFunction::Exp => Ok(NumericExpression::new(
                        fun.clone(),
                        Box::new(arg_expressions.remove(0)),
                    )?),
                    BuiltinScalarFunction::Power | BuiltinScalarFunction::Atan2 => bail!(
                        "multiple argument numeric function {:?} not implemented",
                        fun
                    ),
                    BuiltinScalarFunction::Ascii
                    | BuiltinScalarFunction::BitLength
                    | BuiltinScalarFunction::Btrim
                    | BuiltinScalarFunction::CharacterLength
                    | BuiltinScalarFunction::Chr
                    | BuiltinScalarFunction::Concat
                    | BuiltinScalarFunction::ConcatWithSeparator
                    | BuiltinScalarFunction::InitCap
                    | BuiltinScalarFunction::SplitPart
                    | BuiltinScalarFunction::StartsWith
                    | BuiltinScalarFunction::Strpos
                    | BuiltinScalarFunction::Substr
                    | BuiltinScalarFunction::Left
                    | BuiltinScalarFunction::Lpad
                    | BuiltinScalarFunction::Lower
                    | BuiltinScalarFunction::Ltrim
                    | BuiltinScalarFunction::Trim
                    | BuiltinScalarFunction::Translate
                    | BuiltinScalarFunction::OctetLength
                    | BuiltinScalarFunction::Upper
                    | BuiltinScalarFunction::Repeat
                    | BuiltinScalarFunction::Replace
                    | BuiltinScalarFunction::Reverse
                    | BuiltinScalarFunction::Right
                    | BuiltinScalarFunction::Rpad
                    | BuiltinScalarFunction::Rtrim
                    | BuiltinScalarFunction::RegexpMatch
                    | BuiltinScalarFunction::RegexpReplace => {
                        let string_function: StringFunction =
                            (fun.clone(), arg_expressions).try_into()?;
                        Ok(Expression::String(string_function))
                    }
                    BuiltinScalarFunction::Coalesce => Ok(Expression::DataStructure(
                        DataStructureFunction::Coalesce(arg_expressions),
                    )),
                    BuiltinScalarFunction::NullIf => {
                        Ok(Expression::DataStructure(DataStructureFunction::NullIf {
                            left: Box::new(arg_expressions.remove(0)),
                            right: Box::new(arg_expressions.remove(0)),
                        }))
                    }
                    BuiltinScalarFunction::MakeArray => {
                        if matches!(arg_expressions[0].return_type(), TypeDef::StructDef(_, _)) {
                            bail!("make_array only supports primitive types");
                        };
                        Ok(Expression::DataStructure(DataStructureFunction::MakeArray(
                            arg_expressions,
                        )))
                    }
                    BuiltinScalarFunction::Struct | BuiltinScalarFunction::ArrowTypeof => {
                        bail!("data structure function {:?} not implemented", fun)
                    }

                    BuiltinScalarFunction::ToTimestamp => CastExpression::new(
                        Box::new(arg_expressions.remove(0)),
                        &DataType::Timestamp(TimeUnit::Nanosecond, None),
                    ),
                    BuiltinScalarFunction::ToTimestampMillis => CastExpression::new(
                        Box::new(arg_expressions.remove(0)),
                        &DataType::Timestamp(TimeUnit::Millisecond, None),
                    ),
                    BuiltinScalarFunction::ToTimestampMicros => CastExpression::new(
                        Box::new(arg_expressions.remove(0)),
                        &DataType::Timestamp(TimeUnit::Microsecond, None),
                    ),
                    BuiltinScalarFunction::ToTimestampSeconds => CastExpression::new(
                        Box::new(arg_expressions.remove(0)),
                        &DataType::Timestamp(TimeUnit::Second, None),
                    ),
                    BuiltinScalarFunction::DateBin
                    | BuiltinScalarFunction::CurrentDate
                    | BuiltinScalarFunction::FromUnixtime
                    | BuiltinScalarFunction::Now
                    | BuiltinScalarFunction::CurrentTime => {
                        bail!("date function {:?} not implemented", fun)
                    }
                    BuiltinScalarFunction::Digest
                    | BuiltinScalarFunction::MD5
                    | BuiltinScalarFunction::Random
                    | BuiltinScalarFunction::SHA224
                    | BuiltinScalarFunction::SHA256
                    | BuiltinScalarFunction::SHA384
                    | BuiltinScalarFunction::SHA512 => Ok(HashExpression::new(
                        fun.clone(),
                        Box::new(arg_expressions.remove(0)),
                    )?),
                    BuiltinScalarFunction::ToHex => bail!("hex not implemented"),
                    BuiltinScalarFunction::Uuid => bail!("UUID unimplemented"),
                    BuiltinScalarFunction::Cbrt => bail!("cube root unimplemented"),
                    BuiltinScalarFunction::Degrees => bail!("degrees not implemented yet"),
                    BuiltinScalarFunction::Pi => bail!("pi not implemented yet"),
                    BuiltinScalarFunction::Radians => bail!("radians not implemented yet"),
                    BuiltinScalarFunction::Factorial => bail!("factorial not implemented yet"),
                    BuiltinScalarFunction::Gcd => bail!("gcd not implemented yet"),
                    BuiltinScalarFunction::Lcm => bail!("lcm not implemented yet"),
                    BuiltinScalarFunction::DatePart | BuiltinScalarFunction::DateTrunc => {
                        let date_function: DateTimeFunction =
                            (fun.clone(), arg_expressions).try_into()?;
                        Ok(Expression::Date(date_function))
                    }
                }
            }
            Expr::ScalarUDF(ScalarUDF { fun, args }) => match fun.name.as_str() {
                "get_first_json_object" => {
                    let json_string = Box::new(self.compile_expr(&args[0])?);
                    let path = Box::new(self.compile_expr(&args[1])?);
                    Ok(Expression::Json(JsonExpression {
                        function: JsonFunction::GetFirstJsonObject,
                        json_string,
                        path,
                    }))
                }
                "get_json_objects" => {
                    let json_string = Box::new(self.compile_expr(&args[0])?);
                    let path = Box::new(self.compile_expr(&args[1])?);
                    Ok(Expression::Json(JsonExpression {
                        function: JsonFunction::GetJsonObjects,
                        json_string,
                        path,
                    }))
                }
                "extract_json_string" => {
                    let json_string = Box::new(self.compile_expr(&args[0])?);
                    let path = Box::new(self.compile_expr(&args[1])?);
                    Ok(Expression::Json(JsonExpression {
                        function: JsonFunction::ExtractJsonString,
                        json_string,
                        path,
                    }))
                }
                udf => {
                    // get udf from context
                    let def = self
                        .schema_provider
                        .udf_defs
                        .get(udf)
                        .ok_or_else(|| anyhow!("no UDF with name '{}'", udf))?;

                    let inputs: Result<Vec<Expression>> =
                        args.iter().map(|e| (self.compile_expr(e))).collect();
                    let inputs = inputs?;

                    if inputs.len() != def.args.len() {
                        bail!(
                            "wrong number of arguments for udf {} (found {}, expected {})",
                            udf,
                            args.len(),
                            def.args.len()
                        );
                    }

                    Ok(Expression::RustUdf(RustUdfExpression {
                        name: udf.to_string(),
                        args: def.args.clone().into_iter().zip(inputs).collect(),
                        ret_type: def.ret.clone(),
                    }))
                }
            },
            expression => {
                bail!("expression {:?} not yet implemented", expression)
            }
        }
    }

    pub fn as_two_phase_aggregation(&mut self, expr: &Expr) -> Result<TwoPhaseAggregation> {
        if !self.input_struct.fields.is_empty() {
            bail!("expected single field input");
        }
        match expr {
            Expr::AggregateFunction(AggregateFunction {
                fun,
                args,
                distinct: false,
                filter: None,
                order_by: None,
            }) => {
                if args.len() != 1 {
                    bail!("unexpected arg length");
                }
                let incoming_expression = self.compile_expr(&args[0])?;
                let aggregator = Aggregator::from_datafusion(fun.clone(), false)?;
                Ok(TwoPhaseAggregation {
                    incoming_expression,
                    aggregator,
                })
            }
            _ => bail!("expected aggregate expression"),
        }
    }
}

/// A named reference to a qualified field in a schema.
#[derive(Debug, Clone)]
pub struct Column {
    /// relation/table name.
    pub relation: Option<String>,
    /// field/column name.
    pub name: String,
}

impl Column {
    pub fn convert(column: &datafusion_common::Column) -> Self {
        Column {
            relation: column.relation.to_owned().map(|s| s.to_string()),
            name: column.name.clone(),
        }
    }
    pub fn convert_expr(expr: &datafusion_expr::Expr) -> Result<Self> {
        if let datafusion_expr::Expr::Column(column) = expr {
            Ok(Self::convert(column))
        } else {
            bail!(
                "only support converting column expressions to columns, not {}",
                expr
            )
        }
    }
}

#[derive(Debug, Clone)]
pub struct ColumnExpression {
    column_field: StructField,
}

impl ColumnExpression {
    pub fn new(column_field: StructField) -> Self {
        Self { column_field }
    }
    pub fn from_column(
        column: &datafusion_common::Column,
        input_struct: &StructDef,
    ) -> Result<Self> {
        let column_field = input_struct.get_field(
            column.relation.as_ref().map(|table| table.to_string()),
            &column.name,
        )?;
        Ok(ColumnExpression { column_field })
    }

    fn to_syn_expression(&self) -> syn::Expr {
        let field_ident = self.column_field.field_ident();
        parse_quote!(arg.#field_ident.clone())
    }

    fn return_type(&self) -> TypeDef {
        self.column_field.data_type.clone()
    }
}

#[derive(Debug, Clone)]
pub enum UnaryOperator {
    IsNotNull,
    IsNull,
    IsTrue,
    IsFalse,
    IsUnknown,
    IsNotTrue,
    IsNotFalse,
    IsNotUnknown,
    Negative,
}

#[derive(Debug, Clone)]
pub struct UnaryBooleanExpression {
    operator: UnaryOperator,
    input: Box<Expression>,
}

impl UnaryBooleanExpression {
    fn to_syn_expression(&self) -> syn::Expr {
        let argument_expr = self.input.to_syn_expression();
        match (self.input.return_type().is_optional(), &self.operator) {
            (true, UnaryOperator::IsNotNull) => parse_quote!(#argument_expr.is_some()),
            (true, UnaryOperator::IsNull) => parse_quote!(#argument_expr.is_none()),
            (true, UnaryOperator::IsTrue) => parse_quote!(#argument_expr.unwrap_or(false)),
            (true, UnaryOperator::IsFalse) => parse_quote!(!#argument_expr.unwrap_or(true)),
            (true, UnaryOperator::IsUnknown) => parse_quote!(#argument_expr.is_none()),
            (true, UnaryOperator::IsNotTrue) => parse_quote!((!#argument_expr.unwrap_or(false))),
            (true, UnaryOperator::IsNotFalse) => parse_quote!(#argument_expr.unwrap_or(true)),
            (true, UnaryOperator::IsNotUnknown) => parse_quote!(#argument_expr.is_some()),
            (true, UnaryOperator::Negative) => parse_quote!(#argument_expr.map(|x| -1 * x)),
            (false, UnaryOperator::IsNotNull) => parse_quote!(true),
            (false, UnaryOperator::IsNull) => parse_quote!(false),
            (false, UnaryOperator::IsTrue) => parse_quote!(#argument_expr),
            (false, UnaryOperator::IsFalse) => parse_quote!((!#argument_expr)),
            (false, UnaryOperator::IsUnknown) => parse_quote!(false),
            (false, UnaryOperator::IsNotTrue) => parse_quote!((!#argument_expr)),
            (false, UnaryOperator::IsNotFalse) => parse_quote!(#argument_expr),
            (false, UnaryOperator::IsNotUnknown) => parse_quote!(true),
            (false, UnaryOperator::Negative) => parse_quote!((-1 * #argument_expr)),
        }
    }

    fn return_type(&self) -> TypeDef {
        match &self.operator {
            UnaryOperator::IsNotNull
            | UnaryOperator::IsTrue
            | UnaryOperator::IsFalse
            | UnaryOperator::IsUnknown
            | UnaryOperator::IsNotTrue
            | UnaryOperator::IsNotFalse
            | UnaryOperator::IsNotUnknown
            | UnaryOperator::IsNull => TypeDef::DataType(DataType::Boolean, false),
            UnaryOperator::Negative => self.input.return_type(),
        }
    }
    fn new(operator: UnaryOperator, input: Box<Expression>) -> Expression {
        Expression::UnaryBoolean(UnaryBooleanExpression { operator, input })
    }
}

#[derive(Debug, Clone)]
pub struct LiteralExpression {
    literal: ScalarValue,
}

impl LiteralExpression {
    fn to_syn_expression(&self) -> syn::Expr {
        TypeDef::get_literal(&self.literal)
    }

    fn return_type(&self) -> TypeDef {
        TypeDef::DataType(self.literal.get_datatype(), self.literal.is_null())
    }

    fn new(literal: ScalarValue) -> Expression {
        Expression::Literal(Self { literal })
    }
}

#[derive(Debug, Clone)]
pub enum BinaryComparison {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    IsDistinctFrom,
    IsNotDistinctFrom,
    And,
    Or,
}

impl TryFrom<datafusion_expr::Operator> for BinaryComparison {
    type Error = anyhow::Error;

    fn try_from(op: datafusion_expr::Operator) -> Result<Self> {
        let op = match op {
            datafusion_expr::Operator::Eq => Self::Eq,
            datafusion_expr::Operator::NotEq => Self::NotEq,
            datafusion_expr::Operator::Lt => Self::Lt,
            datafusion_expr::Operator::LtEq => Self::LtEq,
            datafusion_expr::Operator::Gt => Self::Gt,
            datafusion_expr::Operator::GtEq => Self::GtEq,
            datafusion_expr::Operator::IsDistinctFrom => Self::IsDistinctFrom,
            datafusion_expr::Operator::IsNotDistinctFrom => Self::IsNotDistinctFrom,
            datafusion_expr::Operator::And => Self::And,
            datafusion_expr::Operator::Or => Self::Or,
            _ => bail!("{:?} is not an order comparison", op),
        };
        Ok(op)
    }
}

#[derive(Debug, Clone)]
pub struct BinaryComparisonExpression {
    pub left: Box<Expression>,
    pub op: BinaryComparison,
    pub right: Box<Expression>,
}

impl BinaryComparisonExpression {
    fn new(
        left: Box<Expression>,
        op: datafusion_expr::Operator,
        right: Box<Expression>,
    ) -> Result<Expression> {
        let op = op.try_into()?;
        Ok(Expression::BinaryComparison(Self { left, op, right }))
    }
}

impl BinaryComparisonExpression {
    fn to_syn_expression(&self) -> syn::Expr {
        let left_expr = self.left.to_syn_expression();
        let right_expr = self.right.to_syn_expression();

        let op = match self.op {
            BinaryComparison::Eq => quote!(==),
            BinaryComparison::NotEq => quote!(!=),
            BinaryComparison::Lt => quote!(<),
            BinaryComparison::LtEq => quote!(<=),
            BinaryComparison::Gt => quote!(>),
            BinaryComparison::GtEq => quote!(>=),
            BinaryComparison::IsNotDistinctFrom => return parse_quote!((#left_expr == #right_expr)),
            BinaryComparison::IsDistinctFrom => return parse_quote!((#left_expr != #right_expr)),
            BinaryComparison::And => quote!(&&),
            BinaryComparison::Or => quote!(||),
        };
        match (
            self.left.return_type().is_optional(),
            self.right.return_type().is_optional(),
        ) {
            (true, true) => parse_quote!({
                let left = #left_expr;
                let right = #right_expr;
                match (left, right) {
                    (Some(left), Some(right)) => Some(left #op right),
                    _ => None
                }
            }),
            (true, false) => {
                parse_quote!(#left_expr.map(|left| left #op #right_expr))
            }
            (false, true) => {
                parse_quote!(#right_expr.map(|right| #left_expr #op right))
            }
            (false, false) => parse_quote!((#left_expr #op #right_expr)),
        }
    }

    fn return_type(&self) -> TypeDef {
        TypeDef::DataType(
            DataType::Boolean,
            self.left.return_type().is_optional() || self.right.return_type().is_optional(),
        )
    }
}

#[derive(Debug, Clone)]
pub enum BinaryMathOperator {
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
}

impl BinaryMathOperator {
    fn as_tokens(&self) -> TokenStream {
        match self {
            BinaryMathOperator::Plus => quote!(+),
            BinaryMathOperator::Minus => quote!(-),
            BinaryMathOperator::Multiply => quote!(*),
            BinaryMathOperator::Divide => quote!(/),
            BinaryMathOperator::Modulo => quote!(%),
        }
    }
}

impl TryFrom<datafusion_expr::Operator> for BinaryMathOperator {
    type Error = anyhow::Error;

    fn try_from(op: datafusion_expr::Operator) -> Result<Self> {
        let op = match op {
            datafusion_expr::Operator::Plus => Self::Plus,
            datafusion_expr::Operator::Minus => Self::Minus,
            datafusion_expr::Operator::Multiply => Self::Multiply,
            datafusion_expr::Operator::Divide => Self::Divide,
            datafusion_expr::Operator::Modulo => Self::Modulo,
            _ => bail!("{:?} is not a math operator", op),
        };
        Ok(op)
    }
}

#[derive(Debug, Clone)]
pub struct BinaryMathExpression {
    left: Box<Expression>,
    op: BinaryMathOperator,
    right: Box<Expression>,
}

impl BinaryMathExpression {
    fn new(
        left: Box<Expression>,
        op: datafusion_expr::Operator,
        right: Box<Expression>,
    ) -> Result<Expression> {
        let op = op.try_into()?;
        Ok(Expression::BinaryMath(Self { left, op, right }))
    }
}

impl BinaryMathExpression {
    fn to_syn_expression(&self) -> syn::Expr {
        let left_expr = self.left.to_syn_expression();
        let right_expr = self.right.to_syn_expression();
        let op = self.op.as_tokens();
        match (self.left.nullable(), self.right.nullable()) {
            (true, true) => parse_quote!({
                let left = #left_expr;
                let right = #right_expr;
                match (left, right) {
                    (Some(left), Some(right)) => Some(left #op right),
                    _ => None
                }
            }),
            (true, false) => {
                parse_quote!(#left_expr.map(|left| left #op #right_expr))
            }
            (false, true) => {
                parse_quote!(#right_expr.map(|right| #left_expr #op right))
            }
            (false, false) => parse_quote!((#left_expr #op #right_expr)),
        }
    }

    fn return_type(&self) -> TypeDef {
        let nullable = self.left.nullable() || self.right.nullable();
        self.left.return_type().with_nullity(nullable)
    }
}

#[derive(Debug, Clone)]
pub struct StructFieldExpression {
    struct_expression: Box<Expression>,
    struct_field: StructField,
}

impl StructFieldExpression {
    fn new(struct_expression: Box<Expression>, key: &ScalarValue) -> Result<Expression> {
        if let TypeDef::StructDef(struct_type, _) = struct_expression.return_type() {
            match key {
                ScalarValue::Utf8(Some(column)) => {
                    let struct_field = struct_type.get_field(None, column)?;
                    Ok(Expression::StructField(Self {
                        struct_expression,
                        struct_field,
                    }))
                }
                _ => bail!("don't support key {:?} for struct field lookup", key),
            }
        } else {
            bail!("{:?} doesn't return a struct", struct_expression);
        }
    }
}

impl StructFieldExpression {
    fn to_syn_expression(&self) -> syn::Expr {
        let struct_expression = self.struct_expression.to_syn_expression();
        let field_ident = self.struct_field.field_ident();
        match (
            self.struct_expression.nullable(),
            self.struct_field.nullable(),
        ) {
            (true, true) => {
                parse_quote!(#struct_expression.map(|arg| arg.#field_ident.clone()).flatten())
            }
            (true, false) => parse_quote!(#struct_expression.map(|arg| arg.#field_ident.clone())),
            (false, true) => parse_quote!(#struct_expression.#field_ident.clone()),
            (false, false) => parse_quote!(#struct_expression.#field_ident.clone()),
        }
    }

    fn return_type(&self) -> TypeDef {
        match self.struct_expression.nullable() {
            true => self.struct_field.data_type.as_nullable(),
            false => self.struct_field.data_type.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Aggregator {
    Count,
    Sum,
    Min,
    Max,
    Avg,
    CountDistinct,
}

impl Aggregator {
    pub fn from_datafusion(
        aggregator: aggregate_function::AggregateFunction,
        distinct: bool,
    ) -> Result<Self> {
        match (aggregator, distinct) {
            (datafusion_expr::AggregateFunction::Count, false) => Ok(Self::Count),
            (datafusion_expr::AggregateFunction::Sum, false) => Ok(Self::Sum),
            (datafusion_expr::AggregateFunction::Min, false) => Ok(Self::Min),
            (datafusion_expr::AggregateFunction::Max, false) => Ok(Self::Max),
            (datafusion_expr::AggregateFunction::Avg, false) => Ok(Self::Avg),
            (datafusion_expr::AggregateFunction::Count, true) => Ok(Self::CountDistinct),
            (aggregator, true) => bail!("distinct not supported for {:?}", aggregator),
            (aggregator, false) => bail!("aggregator {:?} not supported yet", aggregator),
        }
    }

    pub fn return_data_type(&self, input_type: TypeDef) -> DataType {
        let (input_type, _) = match input_type {
            TypeDef::StructDef(_, _) => unreachable!("aggregates over structs not supported"),
            TypeDef::DataType(arg_type, nullable) => (arg_type, nullable),
        };
        match self {
            Aggregator::Count => DataType::Int64,
            Aggregator::Sum => {
                sum_return_type(&input_type).expect("data fusion should've validated types")
            }
            Aggregator::Min => input_type,
            Aggregator::Max => input_type,
            Aggregator::Avg => {
                avg_return_type(&input_type).expect("data fusion should've validated types")
            }
            Aggregator::CountDistinct => DataType::Int64,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AggregationExpression {
    pub producing_expression: Box<Expression>,
    pub aggregator: Aggregator,
}

impl TryFrom<AggregationExpression> for TwoPhaseAggregation {
    type Error = anyhow::Error;

    fn try_from(aggregation_expression: AggregationExpression) -> Result<Self> {
        if aggregation_expression.allows_two_phase() {
            Ok(TwoPhaseAggregation {
                incoming_expression: *aggregation_expression.producing_expression,
                aggregator: aggregation_expression.aggregator,
            })
        } else {
            bail!(
                "{:?} does not support two phase aggregation",
                aggregation_expression.aggregator
            );
        }
    }
}

impl AggregationExpression {
    fn new(
        producing_expression: Box<Expression>,
        aggregator: aggregate_function::AggregateFunction,
        distinct: bool,
    ) -> Result<Expression> {
        let aggregator = Aggregator::from_datafusion(aggregator, distinct)?;
        Ok(Expression::Aggregation(Self {
            producing_expression,
            aggregator,
        }))
    }

    pub(crate) fn allows_two_phase(&self) -> bool {
        match self.aggregator {
            Aggregator::Count
            | Aggregator::Sum
            | Aggregator::Min
            | Aggregator::Avg
            | Aggregator::Max => true,
            Aggregator::CountDistinct => false,
        }
    }

    pub fn try_from_expression(ctx: &mut ExpressionContext, expr: &Expr) -> Result<Self> {
        match expr {
            Expr::AggregateFunction(datafusion_expr::expr::AggregateFunction {
                fun,
                args,
                distinct,
                filter: None,
                order_by: None,
            }) => {
                if args.len() != 1 {
                    bail!("unexpected arg length");
                }
                let producing_expression = Box::new(ctx.compile_expr(&args[0])?);
                let aggregator = Aggregator::from_datafusion(fun.clone(), *distinct)?;
                Ok(AggregationExpression {
                    producing_expression,
                    aggregator,
                })
            }
            _ => bail!("expected aggregate function, not {}", expr),
        }
    }

    pub fn to_syn_expression(&self) -> syn::Expr {
        let sub_expr = self.producing_expression.to_syn_expression();
        let (map_type, unwrap) = if self.producing_expression.nullable() {
            (format_ident!("filter_map"), None)
        } else {
            (format_ident!("map"), Some(quote!(.unwrap())))
        };
        match self.aggregator {
            Aggregator::Count => {
                if self.producing_expression.nullable() {
                    parse_quote!({
                        arg.iter()
                            .filter_map(|arg| #sub_expr)
                            .count() as i64
                    })
                } else {
                    parse_quote!((arg.len() as i64))
                }
            }
            Aggregator::Sum => parse_quote!({
                arg.iter()
                    .#map_type(|arg| #sub_expr)
                    .reduce(|left, right| left + right)
                    #unwrap
            }),
            Aggregator::Min => parse_quote!({
                arg.iter()
                    .#map_type(|arg| #sub_expr)
                    .reduce( |left, right| left.min(right))
                    #unwrap
            }),
            Aggregator::Max => parse_quote!({
                arg.iter()
                    .map(|arg| #sub_expr)
                    .reduce(|left, right| left.max(right))
                    .unwrap()
            }),
            Aggregator::Avg => parse_quote!({
                arg.iter()
                    .#map_type(|arg| #sub_expr)
                    .map(|val| (1, val))
                    .reduce(|left, right| (left.0 + right.0, left.1+right.1))
                    .map(|result| (result.1 as f64)/(result.0 as f64))
                    #unwrap
            }),
            Aggregator::CountDistinct => parse_quote! ({
                arg.iter()
                    .#map_type(|arg| #sub_expr)
                    .collect::<std::collections::HashSet<_>>()
                    .len() as i64
            }),
        }
    }

    pub fn return_type(&self) -> TypeDef {
        match &self.aggregator {
            Aggregator::Count | Aggregator::CountDistinct => {
                TypeDef::DataType(DataType::Int64, false)
            }
            aggregator => TypeDef::DataType(
                aggregator.return_data_type(self.producing_expression.return_type()),
                self.producing_expression.nullable(),
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CastExpression {
    input: Box<Expression>,
    data_type: DataType,
}

impl CastExpression {
    fn new(input: Box<Expression>, data_type: &DataType) -> Result<Expression> {
        if let TypeDef::DataType(input_type, _) = input.return_type() {
            if Self::allowed_types(&input_type, data_type) {
                Ok(Expression::Cast(Self {
                    input,
                    data_type: data_type.clone(),
                }))
            } else {
                bail!(
                    "casting from {:?} to {:?} is currently unsupported",
                    input_type,
                    data_type
                );
            }
        } else {
            bail!("casting structs is currently unsupported")
        }
    }

    #[allow(clippy::if_same_then_else, clippy::needless_bool)]
    fn allowed_types(input_data_type: &DataType, output_data_type: &DataType) -> bool {
        // handle casts between strings and numerics.
        if (Self::is_numeric(input_data_type) || Self::is_string(input_data_type))
            && (Self::is_numeric(output_data_type) || Self::is_string(output_data_type))
        {
            true
        // handle date to string casts.
        } else if Self::is_date(input_data_type) && Self::is_string(output_data_type) {
            true
        // handle string to date casts.
        } else if Self::is_string(input_data_type) && Self::is_date(output_data_type) {
            true
        // handle timestamp casts
        } else if Self::is_date(input_data_type) && Self::is_date(output_data_type) {
            true
        } else if *input_data_type == DataType::Int64 && Self::is_date(output_data_type) {
            true
        } else {
            false
        }
    }

    fn is_numeric(data_type: &DataType) -> bool {
        matches!(
            data_type,
            DataType::Int8
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
        )
    }

    fn is_date(data_type: &DataType) -> bool {
        matches!(data_type, DataType::Timestamp(_, None))
    }

    fn is_string(data_type: &DataType) -> bool {
        matches!(data_type, DataType::Utf8 | DataType::LargeUtf8)
    }
    fn cast_expr(input_type: &DataType, output_type: &DataType, sub_expr: syn::Expr) -> syn::Expr {
        if Self::is_numeric(input_type) && Self::is_numeric(output_type) {
            let cast_type: syn::Type =
                parse_str(&StructField::data_type_name(output_type)).unwrap();
            parse_quote!(#sub_expr as #cast_type)
        } else if Self::is_numeric(input_type) && Self::is_string(output_type) {
            parse_quote!(#sub_expr.to_string())
        } else if Self::is_string(input_type) && Self::is_numeric(output_type) {
            let cast_type: syn::Type =
                parse_str(&StructField::data_type_name(output_type)).unwrap();
            parse_quote!(#sub_expr.parse::<#cast_type>().unwrap())
        } else if Self::is_date(input_type) && Self::is_string(output_type) {
            parse_quote!({
                let datetime: chrono::DateTime<chrono::Utc> = #sub_expr.into();
                datetime.to_rfc3339()
            })
        } else if Self::is_date(input_type) && Self::is_date(output_type) {
            parse_quote!(#sub_expr)
        } else if Self::is_string(input_type) && Self::is_date(output_type) {
            parse_quote!({
                let datetime = chrono::DateTime::parse_from_rfc3339(&#sub_expr).unwrap();
                std::time::SystemTime::UNIX_EPOCH
                + std::time::Duration::from_micros(datetime.with_timezone(&chrono::Utc).timestamp_micros() as u64)
            })
        } else if *input_type == DataType::Int64 && Self::is_date(output_type) {
            match output_type {
                DataType::Timestamp(time_unit, None) => {
                    let from_func: Ident = match time_unit {
                        TimeUnit::Second => parse_quote!(from_secs),
                        TimeUnit::Millisecond => parse_quote!(from_millis),
                        TimeUnit::Microsecond => parse_quote!(from_micros),
                        TimeUnit::Nanosecond => parse_quote!(from_nanos),
                    };
                    parse_quote!({
                        std::time::SystemTime::UNIX_EPOCH
                        + std::time::Duration::#from_func(#sub_expr as u64)
                    })
                }
                _ => unreachable!(),
            }
        } else {
            unreachable!("invalid cast from {:?} to {:?}", input_type, output_type)
        }
    }

    fn to_syn_expression(&self) -> syn::Expr {
        let sub_expr = self.input.to_syn_expression();
        let TypeDef::DataType(input_type, nullable) = self.input.return_type() else {
            unreachable!()
        };
        if nullable {
            let cast_expr = Self::cast_expr(&input_type, &self.data_type, parse_quote!(x));
            parse_quote!(#sub_expr.map(|x| #cast_expr))
        } else {
            let cast_expr = Self::cast_expr(&input_type, &self.data_type, sub_expr);
            parse_quote!(#cast_expr)
        }
    }

    fn return_type(&self) -> TypeDef {
        TypeDef::DataType(self.data_type.clone(), self.input.nullable())
    }
}

#[derive(Debug, Clone)]
enum NumericFunction {
    Abs,
    Acos,
    Acosh,
    Asin,
    Asinh,
    Atan,
    Atanh,
    Cos,
    Cosh,
    Ln,
    Log,
    Log10,
    Sin,
    Sinh,
    Sqrt,
    Tan,
    Tanh,
    Ceil,
    Floor,
    Round,
    Signum,
    Trunc,
    Log2,
    Exp,
}

impl NumericFunction {
    fn function_name(&self) -> Ident {
        let name = match self {
            NumericFunction::Abs => "abs",
            NumericFunction::Acos => "acos",
            NumericFunction::Acosh => "acosh",
            NumericFunction::Asin => "asin",
            NumericFunction::Asinh => "asinh",
            NumericFunction::Atan => "atan",
            NumericFunction::Atanh => "atanh",
            NumericFunction::Cos => "cos",
            NumericFunction::Cosh => "cosh",
            NumericFunction::Ln => "ln",
            NumericFunction::Log => "log",
            NumericFunction::Log10 => "log10",
            NumericFunction::Sin => "sin",
            NumericFunction::Sinh => "sinh",
            NumericFunction::Sqrt => "sqrt",
            NumericFunction::Tan => "tan",
            NumericFunction::Tanh => "tanh",
            NumericFunction::Log2 => "log2",
            NumericFunction::Exp => "exp",
            NumericFunction::Ceil => "ceil",
            NumericFunction::Floor => "floor",
            NumericFunction::Round => "round",
            NumericFunction::Trunc => "trunc",
            NumericFunction::Signum => "signum",
        };
        format_ident!("{}", name)
    }
}

impl TryFrom<BuiltinScalarFunction> for NumericFunction {
    type Error = anyhow::Error;

    fn try_from(fun: BuiltinScalarFunction) -> Result<Self> {
        match fun {
            BuiltinScalarFunction::Abs => Ok(Self::Abs),
            BuiltinScalarFunction::Acos => Ok(Self::Acos),
            BuiltinScalarFunction::Acosh => Ok(Self::Acosh),
            BuiltinScalarFunction::Asin => Ok(Self::Asin),
            BuiltinScalarFunction::Asinh => Ok(Self::Asinh),
            BuiltinScalarFunction::Atan => Ok(Self::Atan),
            BuiltinScalarFunction::Atanh => Ok(Self::Atanh),
            BuiltinScalarFunction::Cos => Ok(Self::Cos),
            BuiltinScalarFunction::Cosh => Ok(Self::Cosh),
            BuiltinScalarFunction::Ln => Ok(Self::Ln),
            BuiltinScalarFunction::Log => Ok(Self::Log),
            BuiltinScalarFunction::Log10 => Ok(Self::Log10),
            BuiltinScalarFunction::Sin => Ok(Self::Sin),
            BuiltinScalarFunction::Sinh => Ok(Self::Sinh),
            BuiltinScalarFunction::Sqrt => Ok(Self::Sqrt),
            BuiltinScalarFunction::Tan => Ok(Self::Tan),
            BuiltinScalarFunction::Tanh => Ok(Self::Tanh),
            BuiltinScalarFunction::Ceil => Ok(Self::Ceil),
            BuiltinScalarFunction::Floor => Ok(Self::Floor),
            BuiltinScalarFunction::Round => Ok(Self::Round),
            BuiltinScalarFunction::Signum => Ok(Self::Signum),
            BuiltinScalarFunction::Trunc => Ok(Self::Trunc),
            BuiltinScalarFunction::Log2 => Ok(Self::Log2),
            BuiltinScalarFunction::Exp => Ok(Self::Exp),
            _ => bail!("{:?} is not a single argument numeric function", fun),
        }
    }
}

#[derive(Debug, Clone)]
pub struct NumericExpression {
    function: NumericFunction,
    input: Box<Expression>,
}

impl NumericExpression {
    fn new(function: BuiltinScalarFunction, input: Box<Expression>) -> Result<Expression> {
        let function = function.try_into()?;
        Ok(Expression::Numeric(NumericExpression { function, input }))
    }
    fn to_syn_expression(&self) -> syn::Expr {
        let function_name = self.function.function_name();
        let argument_expression = self.input.to_syn_expression();
        if self.input.return_type().is_optional() {
            parse_quote!(#argument_expression.map(|val| (val as f64).#function_name()))
        } else {
            parse_quote!((#argument_expression as f64).#function_name())
        }
    }

    fn return_type(&self) -> TypeDef {
        TypeDef::DataType(DataType::Float64, self.input.return_type().is_optional())
    }
}

#[derive(Debug, Clone)]
pub struct SortExpression {
    value: Expression,
    direction: SortDirection,
    nulls_first: bool,
}

impl SortExpression {
    pub fn from_expression(ctx: &mut ExpressionContext, sort: &Sort) -> Result<Self> {
        let value = ctx.compile_expr(&sort.expr)?;

        let direction = if sort.asc {
            SortDirection::Asc
        } else {
            SortDirection::Desc
        };
        let nulls_first = sort.nulls_first;
        Ok(Self {
            value,
            direction,
            nulls_first,
        })
    }

    fn tuple_type(&self) -> syn::Type {
        let value_type = if self.value.return_type().is_float() {
            let t = self.value.return_type().return_type();
            parse_quote! { arroyo_worker::OrderedFloat<#t> }
        } else {
            self.value.return_type().return_type()
        };

        match (self.value.nullable(), &self.direction, self.nulls_first) {
            (false, SortDirection::Asc, _) | (true, SortDirection::Asc, true) => {
                parse_quote!(#value_type)
            }
            (false, SortDirection::Desc, _) | (true, SortDirection::Desc, true) => {
                parse_quote!(std::cmp::Reverse<#value_type>)
            }
            (true, SortDirection::Asc, false) => parse_quote!((bool, #value_type)),
            (true, SortDirection::Desc, false) => {
                parse_quote!(std::cmp::Reverse<(bool, #value_type)>)
            }
        }
    }

    pub fn sort_tuple_type(sort_expressions: &Vec<SortExpression>) -> syn::Type {
        match sort_expressions.len() {
            0 => parse_quote!(()),
            1 => {
                let singleton_type = sort_expressions[0].tuple_type();
                parse_quote!((#singleton_type,))
            }
            _ => {
                let tuple_types: Vec<syn::Type> = sort_expressions
                    .iter()
                    .map(|sort_expression| sort_expression.tuple_type())
                    .collect();
                parse_quote!((#(#tuple_types),*))
            }
        }
    }

    pub fn sort_tuple_expression(sort_expressions: &Vec<SortExpression>) -> syn::Expr {
        match sort_expressions.len() {
            0 => parse_quote!(()),
            1 => {
                let singleton_expr = sort_expressions[0].to_syn_expr();
                parse_quote!((#singleton_expr,))
            }
            _ => {
                let tuple_exprs: Vec<syn::Expr> = sort_expressions
                    .iter()
                    .map(|sort_expression| sort_expression.to_syn_expr())
                    .collect();
                parse_quote!((#(#tuple_exprs),*))
            }
        }
    }

    fn to_syn_expr(&self) -> syn::Expr {
        let value = if self.value.return_type().is_float() {
            Expression::WrapType(WrapTypeExpression::new(
                "arroyo_worker::OrderedFloat",
                self.value.clone(),
            ))
        } else {
            self.value.clone()
        };

        let value_expr = value.to_syn_expression();
        match (self.value.nullable(), &self.direction, self.nulls_first) {
            (false, SortDirection::Asc, _) | (true, SortDirection::Asc, true) => {
                parse_quote!(#value_expr)
            }
            (false, SortDirection::Desc, _) | (true, SortDirection::Desc, true) => {
                parse_quote!(std::cmp::Reverse(#value_expr))
            }
            (true, SortDirection::Asc, false) => parse_quote!({
                let option = #value_expr;
                (option.is_none(), option)
            }),
            (true, SortDirection::Desc, false) => parse_quote!({
                let option = #value_expr;
                std::cmp::Reverse((option.is_none(), option))
            }),
        }
    }
}

#[derive(Debug, Clone)]
pub enum StringFunction {
    Ascii(Box<Expression>),
    BitLength(Box<Expression>),
    Btrim(Box<Expression>, Option<Box<Expression>>),
    CharacterLength(Box<Expression>),
    Chr(Box<Expression>),
    Concat(Vec<Expression>),
    ConcatWithSeparator(Box<Expression>, Vec<Expression>),
    InitCap(Box<Expression>),
    SplitPart(Box<Expression>, Box<Expression>, Box<Expression>),
    StartsWith(Box<Expression>, Box<Expression>),
    Strpos(Box<Expression>, Box<Expression>),
    Substr(Box<Expression>, Box<Expression>, Option<Box<Expression>>),
    Left(Box<Expression>, Box<Expression>),
    Lpad(Box<Expression>, Box<Expression>, Option<Box<Expression>>),
    Lower(Box<Expression>),
    Ltrim(Box<Expression>, Option<Box<Expression>>),
    Trim(Box<Expression>, Option<Box<Expression>>),
    Translate(Box<Expression>, Box<Expression>, Box<Expression>),
    OctetLength(Box<Expression>),
    Upper(Box<Expression>),
    RegexpMatch(
        Box<Expression>,
        // Checked regex
        String,
    ),
    RegexpReplace(
        // String to mutate
        Box<Expression>,
        // Checked Regex
        String,
        // String to replace
        Box<Expression>,
        // Optional flags of 'i'  (insensitive) and 'g' (global)
        Option<Box<Expression>>,
    ),
    Repeat(Box<Expression>, Box<Expression>),
    Replace(Box<Expression>, Box<Expression>, Box<Expression>),
    Reverse(Box<Expression>),
    Right(Box<Expression>, Box<Expression>),
    Rpad(Box<Expression>, Box<Expression>, Option<Box<Expression>>),
    Rtrim(Box<Expression>, Option<Box<Expression>>),
}

#[derive(Debug, Clone)]
pub enum HashFunction {
    MD5,
    SHA224,
    SHA256,
    SHA384,
    SHA512,
}

impl ToString for HashFunction {
    fn to_string(&self) -> String {
        match self {
            Self::MD5 => "md5".to_string(),
            Self::SHA224 => "sha224".to_string(),
            Self::SHA256 => "sha256".to_string(),
            Self::SHA384 => "sha384".to_string(),
            Self::SHA512 => "sha512".to_string(),
        }
    }
}

impl TryFrom<BuiltinScalarFunction> for HashFunction {
    type Error = anyhow::Error;

    fn try_from(value: BuiltinScalarFunction) -> Result<Self> {
        match value {
            BuiltinScalarFunction::MD5 => Ok(Self::MD5),
            BuiltinScalarFunction::SHA224 => Ok(Self::SHA224),
            BuiltinScalarFunction::SHA256 => Ok(Self::SHA256),
            BuiltinScalarFunction::SHA384 => Ok(Self::SHA384),
            BuiltinScalarFunction::SHA512 => Ok(Self::SHA512),
            _ => bail!("function {} is not a hash function", value),
        }
    }
}

#[derive(Debug, Clone)]
pub struct HashExpression {
    function: HashFunction,
    input: Box<Expression>,
}

impl HashExpression {
    pub fn new(function: BuiltinScalarFunction, input: Box<Expression>) -> Result<Expression> {
        Ok(Expression::Hash(HashExpression {
            function: function.try_into()?,
            input,
        }))
    }

    fn to_syn_expression(&self) -> syn::Expr {
        let input = self.input.to_syn_expression();
        let hash_fn = format_ident!("{}", self.function.to_string());

        let coerce = match self.input.return_type() {
            TypeDef::StructDef(_, _) => unreachable!(),
            TypeDef::DataType(DataType::Utf8, _) => quote!(.as_bytes().to_vec()),
            TypeDef::DataType(DataType::Binary, _) => quote!(),
            TypeDef::DataType(_, _) => unreachable!(),
        };

        match self.input.nullable() {
            true => parse_quote!({
                match #input {
                    Some(unwrapped) => Some(arroyo_worker::operators::functions::hash::#hash_fn(unwrapped #coerce)),
                    None => None,
                }
            }),
            false => {
                parse_quote!(arroyo_worker::operators::functions::hash::#hash_fn(#input #coerce))
            }
        }
    }

    fn return_type(&self) -> TypeDef {
        // this *can* be null because in SQL - MD5(NULL) = NULL
        TypeDef::DataType(DataType::Utf8, self.input.nullable())
    }
}

impl TryFrom<(BuiltinScalarFunction, Vec<Expression>)> for StringFunction {
    type Error = anyhow::Error;

    fn try_from(value: (BuiltinScalarFunction, Vec<Expression>)) -> Result<Self> {
        let func = value.0;
        let mut args = value.1;
        // handle two vector cases
        if func == BuiltinScalarFunction::Concat {
            return Ok(StringFunction::Concat(args));
        }
        if func == BuiltinScalarFunction::ConcatWithSeparator {
            let separator = Box::new(args.remove(0));
            return Ok(StringFunction::ConcatWithSeparator(separator, args));
        }
        match (args.len(), func) {
            (1, BuiltinScalarFunction::Ascii) => {
                Ok(StringFunction::Ascii(Box::new(args.remove(0))))
            }
            (1, BuiltinScalarFunction::BitLength) => {
                Ok(StringFunction::BitLength(Box::new(args.remove(0))))
            }
            (1, BuiltinScalarFunction::CharacterLength) => {
                Ok(StringFunction::CharacterLength(Box::new(args.remove(0))))
            }
            (1, BuiltinScalarFunction::Chr) => Ok(StringFunction::Chr(Box::new(args.remove(0)))),
            (1, BuiltinScalarFunction::InitCap) => {
                Ok(StringFunction::InitCap(Box::new(args.remove(0))))
            }
            (1, BuiltinScalarFunction::Lower) => {
                Ok(StringFunction::Lower(Box::new(args.remove(0))))
            }
            (1, BuiltinScalarFunction::OctetLength) => {
                Ok(StringFunction::OctetLength(Box::new(args.remove(0))))
            }
            (1, BuiltinScalarFunction::Reverse) => {
                Ok(StringFunction::Reverse(Box::new(args.remove(0))))
            }
            (1, BuiltinScalarFunction::Btrim) => {
                Ok(StringFunction::Btrim(Box::new(args.remove(0)), None))
            }
            (1, BuiltinScalarFunction::Ltrim) => {
                Ok(StringFunction::Ltrim(Box::new(args.remove(0)), None))
            }
            (1, BuiltinScalarFunction::Rtrim) => {
                Ok(StringFunction::Rtrim(Box::new(args.remove(0)), None))
            }
            (1, BuiltinScalarFunction::Trim) => {
                Ok(StringFunction::Trim(Box::new(args.remove(0)), None))
            }
            (1, BuiltinScalarFunction::Upper) => {
                Ok(StringFunction::Upper(Box::new(args.remove(0))))
            }
            (2, BuiltinScalarFunction::Btrim) => Ok(StringFunction::Btrim(
                Box::new(args.remove(0)),
                Some(Box::new(args.remove(0))),
            )),
            (2, BuiltinScalarFunction::Left) => Ok(StringFunction::Left(
                Box::new(args.remove(0)),
                Box::new(args.remove(0)),
            )),
            (2, BuiltinScalarFunction::Lpad) => Ok(StringFunction::Lpad(
                Box::new(args.remove(0)),
                Box::new(args.remove(0)),
                None,
            )),
            (2, BuiltinScalarFunction::Ltrim) => Ok(StringFunction::Ltrim(
                Box::new(args.remove(0)),
                Some(Box::new(args.remove(0))),
            )),
            (2, BuiltinScalarFunction::RegexpMatch) => {
                let first_argument = Box::new(args.remove(0));
                let regex_arg = args.remove(0);
                let Expression::Literal(LiteralExpression{literal: ScalarValue::Utf8(Some(regex))}) = regex_arg else {
                    bail!("regex argument must be a string literal")
                };
                let _ = Regex::new(&regex)?;
                Ok(StringFunction::RegexpMatch(first_argument, regex))
            }
            (2, BuiltinScalarFunction::Repeat) => Ok(StringFunction::Repeat(
                Box::new(args.remove(0)),
                Box::new(args.remove(0)),
            )),
            (2, BuiltinScalarFunction::Right) => Ok(StringFunction::Right(
                Box::new(args.remove(0)),
                Box::new(args.remove(0)),
            )),
            (2, BuiltinScalarFunction::Rpad) => Ok(StringFunction::Rpad(
                Box::new(args.remove(0)),
                Box::new(args.remove(0)),
                None,
            )),
            (2, BuiltinScalarFunction::Rtrim) => Ok(StringFunction::Rtrim(
                Box::new(args.remove(0)),
                Some(Box::new(args.remove(0))),
            )),
            (2, BuiltinScalarFunction::StartsWith) => Ok(StringFunction::StartsWith(
                Box::new(args.remove(0)),
                Box::new(args.remove(0)),
            )),
            (2, BuiltinScalarFunction::Strpos) => Ok(StringFunction::Strpos(
                Box::new(args.remove(0)),
                Box::new(args.remove(0)),
            )),
            (2, BuiltinScalarFunction::Substr) => Ok(StringFunction::Substr(
                Box::new(args.remove(0)),
                Box::new(args.remove(0)),
                None,
            )),
            (2, BuiltinScalarFunction::Trim) => Ok(StringFunction::Trim(
                Box::new(args.remove(0)),
                Some(Box::new(args.remove(0))),
            )),
            (3, BuiltinScalarFunction::Substr) => Ok(StringFunction::Substr(
                Box::new(args.remove(0)),
                Box::new(args.remove(0)),
                Some(Box::new(args.remove(0))),
            )),
            (3, BuiltinScalarFunction::Translate) => Ok(StringFunction::Translate(
                Box::new(args.remove(0)),
                Box::new(args.remove(0)),
                Box::new(args.remove(0)),
            )),
            (3, BuiltinScalarFunction::Lpad) => Ok(StringFunction::Lpad(
                Box::new(args.remove(0)),
                Box::new(args.remove(0)),
                Some(Box::new(args.remove(0))),
            )),
            (3, BuiltinScalarFunction::Rpad) => Ok(StringFunction::Rpad(
                Box::new(args.remove(0)),
                Box::new(args.remove(0)),
                Some(Box::new(args.remove(0))),
            )),
            (3, BuiltinScalarFunction::Replace) => Ok(StringFunction::Replace(
                Box::new(args.remove(0)),
                Box::new(args.remove(0)),
                Box::new(args.remove(0)),
            )),
            (3, BuiltinScalarFunction::SplitPart) => Ok(StringFunction::SplitPart(
                Box::new(args.remove(0)),
                Box::new(args.remove(0)),
                Box::new(args.remove(0)),
            )),
            (3, BuiltinScalarFunction::RegexpReplace) => {
                let first_argument = Box::new(args.remove(0));
                let regex_arg = args.remove(0);
                let Expression::Literal(LiteralExpression{literal: ScalarValue::Utf8(Some(regex))}) = regex_arg else {
                    bail!("regex argument must be a string literal")
                };
                let _ = Regex::new(&regex)?;
                let substitute = args.remove(0);
                Ok(StringFunction::RegexpReplace(
                    first_argument,
                    regex,
                    Box::new(substitute),
                    None,
                ))
            }
            (_, BuiltinScalarFunction::Concat) => Ok(StringFunction::Concat(args)),
            (1..=usize::MAX, func) if func == BuiltinScalarFunction::Concat => {
                let separator = Box::new(args.remove(0));
                Ok(StringFunction::ConcatWithSeparator(separator, args))
            }
            (_, func) => bail!("function {} with args {:?} not supported", func, args),
        }
    }
}

impl StringFunction {
    fn return_type(&self) -> TypeDef {
        match self {
            StringFunction::Ascii(expr)
            | StringFunction::BitLength(expr)
            | StringFunction::CharacterLength(expr)
            | StringFunction::OctetLength(expr)
            | StringFunction::Reverse(expr) => TypeDef::DataType(DataType::Int32, expr.nullable()),
            StringFunction::StartsWith(expr1, expr2) => {
                TypeDef::DataType(DataType::Boolean, expr1.nullable() || expr2.nullable())
            }
            StringFunction::Left(expr1, expr2)
            | StringFunction::Repeat(expr1, expr2)
            | StringFunction::Right(expr1, expr2)
            | StringFunction::Btrim(expr1, Some(expr2))
            | StringFunction::Trim(expr1, Some(expr2))
            | StringFunction::Ltrim(expr1, Some(expr2))
            | StringFunction::Rtrim(expr1, Some(expr2))
            | StringFunction::Substr(expr1, expr2, None)
            | StringFunction::Lpad(expr1, expr2, None)
            | StringFunction::Rpad(expr1, expr2, None) => {
                TypeDef::DataType(DataType::Utf8, expr1.nullable() || expr2.nullable())
            }
            StringFunction::Btrim(expr, None)
            | StringFunction::Lower(expr)
            | StringFunction::Upper(expr)
            | StringFunction::Chr(expr)
            | StringFunction::InitCap(expr)
            | StringFunction::Ltrim(expr, None)
            | StringFunction::Rtrim(expr, None)
            | StringFunction::Trim(expr, None) => {
                TypeDef::DataType(DataType::Utf8, expr.nullable())
            }
            StringFunction::Substr(expr1, expr2, Some(expr3))
            | StringFunction::Translate(expr1, expr2, expr3)
            | StringFunction::Lpad(expr1, expr2, Some(expr3))
            | StringFunction::Rpad(expr1, expr2, Some(expr3))
            | StringFunction::Replace(expr1, expr2, expr3)
            | StringFunction::SplitPart(expr1, expr2, expr3) => TypeDef::DataType(
                DataType::Utf8,
                expr1.nullable() || expr2.nullable() || expr3.nullable(),
            ),
            StringFunction::Concat(_exprs) => TypeDef::DataType(DataType::Utf8, false),
            StringFunction::ConcatWithSeparator(expr, _exprs) => {
                TypeDef::DataType(DataType::Utf8, expr.nullable())
            }
            StringFunction::RegexpReplace(expr1, _, expr3, _) => {
                TypeDef::DataType(DataType::Utf8, expr1.nullable() || expr3.nullable())
            }
            StringFunction::RegexpMatch(expr1, _) => {
                TypeDef::DataType(DataType::Utf8, expr1.nullable())
            }
            StringFunction::Strpos(expr1, expr2) => {
                TypeDef::DataType(DataType::Int32, expr1.nullable() || expr2.nullable())
            }
        }
    }
    fn non_null_function_invocation(&self) -> syn::Expr {
        match self {
            StringFunction::Ascii(_) => {
                parse_quote!(arroyo_worker::operators::functions::strings::ascii(arg))
            }
            StringFunction::BitLength(_) => {
                parse_quote!(arroyo_worker::operators::functions::strings::bit_length(
                    arg
                ))
            }
            StringFunction::CharacterLength(_) => parse_quote!((arg.chars().count() as i32)),
            StringFunction::Chr(_) => {
                parse_quote!((arroyo_worker::operators::functions::strings::chr(arg)))
            }
            StringFunction::InitCap(_) => {
                parse_quote!(arroyo_worker::operators::functions::strings::initcap(arg))
            }
            StringFunction::OctetLength(_) => {
                parse_quote!(arroyo_worker::operators::functions::strings::octet_length(
                    arg
                ))
            }
            StringFunction::Lower(_) => parse_quote!(arg.to_lowercase()),
            StringFunction::Upper(_) => parse_quote!(arg.to_uppercase()),
            StringFunction::Reverse(_) => parse_quote!(arg.chars().rev().collect()),
            StringFunction::Btrim(_, None) | StringFunction::Trim(_, None) => {
                parse_quote!(arroyo_worker::operators::functions::strings::trim(
                    arg,
                    " ".to_string()
                ))
            }
            StringFunction::Ltrim(_, None) => {
                parse_quote!(arroyo_worker::operators::functions::strings::ltrim(
                    arg,
                    " ".to_string()
                ))
            }
            StringFunction::Rtrim(_, None) => {
                parse_quote!(arroyo_worker::operators::functions::strings::rtrim(
                    arg,
                    " ".to_string()
                ))
            }
            StringFunction::Btrim(_, Some(_)) | StringFunction::Trim(_, Some(_)) => {
                parse_quote!(arroyo_worker::operators::functions::strings::trim(
                    arg1, arg2
                ))
            }
            StringFunction::Substr(_, _, None) => {
                parse_quote!(arroyo_worker::operators::functions::strings::substr(
                    arg1, arg2, None
                ))
            }
            StringFunction::StartsWith(_, _) => {
                parse_quote!(arroyo_worker::operators::functions::strings::starts_with(
                    arg1, arg2
                ))
            }
            StringFunction::Strpos(_, _) => {
                parse_quote!(arroyo_worker::operators::functions::strings::strpos(
                    arg1, arg2
                ))
            }
            StringFunction::Left(_, _) => {
                parse_quote!(arroyo_worker::operators::functions::strings::left(
                    arg1, arg2
                ))
            }
            StringFunction::Lpad(_, _, None) => parse_quote!(
                arroyo_worker::operators::functions::strings::lpad(arg1, arg2, " ".to_string())
            ),
            StringFunction::Ltrim(_, Some(_)) => {
                parse_quote!(arroyo_worker::operators::functions::strings::ltrim(
                    arg1, arg2
                ))
            }
            StringFunction::RegexpMatch(_, regex) => {
                parse_quote!(arroyo_worker::operators::functions::regexp::regexp_match(
                    arg, #regex.to_string()
                ))
            }
            StringFunction::RegexpReplace(_, regex, _, _) => {
                parse_quote!(arroyo_worker::operators::functions::regexp::regexp_replace(
                    arg1, #regex.to_string(), arg2
                ))
            }
            StringFunction::Repeat(_, _) => parse_quote!(arg1.repeat(arg2 as usize)),
            StringFunction::Right(_, _) => {
                parse_quote!(arroyo_worker::operators::functions::strings::right(
                    arg1, arg2
                ))
            }
            StringFunction::Rpad(_, _, None) => parse_quote!(
                arroyo_worker::operators::functions::strings::rpad(arg1, arg2, " ".to_string())
            ),
            StringFunction::Rtrim(_, Some(_)) => {
                parse_quote!(arroyo_worker::operators::functions::strings::rtrim(
                    arg1, arg2
                ))
            }
            StringFunction::Replace(_, _, _) => parse_quote!(arg1.replace(&arg2, &arg3)),
            StringFunction::Substr(_, _, Some(_)) => parse_quote!(
                arroyo_worker::operators::functions::strings::substr(arg1, arg2, Some(arg3))
            ),
            StringFunction::Translate(_, _, _) => parse_quote!(
                arroyo_worker::operators::functions::strings::translate(arg1, arg2, arg3)
            ),
            StringFunction::Lpad(_, _, Some(_)) => {
                parse_quote!(arroyo_worker::operators::functions::strings::lpad(
                    arg1, arg2, arg3
                ))
            }
            StringFunction::Rpad(_, _, Some(_)) => {
                parse_quote!(arroyo_worker::operators::functions::strings::rpad(
                    arg1, arg2, arg3
                ))
            }
            StringFunction::SplitPart(_, _, _) => parse_quote!(
                arroyo_worker::operators::functions::strings::split_part(arg1, arg2, arg3)
            ),
            StringFunction::Concat(_) => parse_quote!(args.join("")),
            StringFunction::ConcatWithSeparator(_, _) => parse_quote!(args.join(arg)),
        }
    }

    pub fn to_syn_expression(&self) -> syn::Expr {
        let function = self.non_null_function_invocation();
        let function = if self.return_type().is_optional() {
            parse_quote!(Some(#function))
        } else {
            function
        };
        match self {
            // Single argument: arg
            StringFunction::Ascii(arg)
            | StringFunction::BitLength(arg)
            | StringFunction::CharacterLength(arg)
            | StringFunction::Chr(arg)
            | StringFunction::InitCap(arg)
            | StringFunction::Lower(arg)
            | StringFunction::Upper(arg)
            | StringFunction::Reverse(arg)
            | StringFunction::OctetLength(arg)
            | StringFunction::Btrim(arg, None)
            | StringFunction::Trim(arg, None)
            | StringFunction::Ltrim(arg, None)
            | StringFunction::Rtrim(arg, None)
            | StringFunction::RegexpMatch(arg, _) => {
                let expr = arg.to_syn_expression();
                match arg.nullable() {
                    true => parse_quote!({
                        if let Some(arg) = #expr {
                            #function
                        } else {
                            None
                        }
                    }),
                    false => parse_quote!({
                        let arg = #expr;
                        #function
                    }),
                }
            }
            // Two arguments: arg1 and arg2
            StringFunction::StartsWith(arg1, arg2)
            | StringFunction::Strpos(arg1, arg2)
            | StringFunction::Left(arg1, arg2)
            | StringFunction::Repeat(arg1, arg2)
            | StringFunction::Right(arg1, arg2)
            | StringFunction::Btrim(arg1, Some(arg2))
            | StringFunction::Trim(arg1, Some(arg2))
            | StringFunction::Ltrim(arg1, Some(arg2))
            | StringFunction::Rtrim(arg1, Some(arg2))
            | StringFunction::Substr(arg1, arg2, None)
            | StringFunction::Lpad(arg1, arg2, None)
            | StringFunction::Rpad(arg1, arg2, None)
            | StringFunction::RegexpReplace(arg1, _, arg2, None) => {
                let expr1 = arg1.to_syn_expression();
                let expr2 = arg2.to_syn_expression();
                match (arg1.nullable(), arg2.nullable()) {
                    (true, true) => parse_quote!({
                        if let (Some(arg1), Some(arg2)) = (#expr1, #expr2) {
                            #function
                        } else {
                            None
                        }
                    }),
                    (true, false) => parse_quote!({
                        let arg2 = #expr2;
                        if let Some(arg1) = #expr1 {
                            #function
                        } else {
                            None
                        }
                    }),
                    (false, true) => parse_quote!({
                        let arg1 = #expr1;
                        if let Some(arg2) = #expr2 {
                            #function
                        } else {
                            None
                        }
                    }),
                    (false, false) => parse_quote!({
                        let arg1 = #expr1;
                        let arg2 = #expr2;
                        #function
                    }),
                }
            }
            StringFunction::Substr(arg1, arg2, Some(arg3))
            | StringFunction::Translate(arg1, arg2, arg3)
            | StringFunction::Lpad(arg1, arg2, Some(arg3))
            | StringFunction::Rpad(arg1, arg2, Some(arg3))
            | StringFunction::Replace(arg1, arg2, arg3)
            | StringFunction::SplitPart(arg1, arg2, arg3) => {
                let expr1 = arg1.to_syn_expression();
                let expr2 = arg2.to_syn_expression();
                let expr3 = arg3.to_syn_expression();

                match (arg1.nullable(), arg2.nullable(), arg3.nullable()) {
                    (true, true, true) => parse_quote!({
                        if let (Some(arg1), Some(arg2), Some(arg3)) = (#expr1, #expr2, #expr3) {
                            #function
                        } else {
                            None
                        }
                    }),
                    (true, true, false) => parse_quote!({
                        let arg3 = #expr3;
                        if let (Some(arg1), Some(arg2)) = (#expr1, #expr2) {
                            #function
                        } else {
                            None
                        }
                    }),
                    (true, false, true) => parse_quote!({
                        let arg2 = #expr2;
                        if let (Some(arg1), Some(arg3)) = (#expr1, #expr3) {
                            #function
                        } else {
                            None
                        }
                    }),
                    (true, false, false) => parse_quote!({
                        let arg2 = #expr2;
                        let arg3 = #expr3;
                        if let Some(arg1) = #expr1 {
                            #function
                        } else {
                            None
                        }
                    }),
                    (false, true, true) => parse_quote!({
                        let arg1 = #expr1;
                        if let (Some(arg2), Some(arg3)) = (#expr2, #expr3) {
                            #function
                        } else {
                            None
                        }
                    }),
                    (false, true, false) => parse_quote!({
                        let arg1 = #expr1;
                        let arg3 = #expr3;
                        if let Some(arg2) = #expr2 {
                            #function
                        } else {
                            None
                        }
                    }),
                    (false, false, true) => parse_quote!({
                        let arg1 = #expr1;
                        let arg2 = #expr2;
                        if let Some(arg3) = #expr3 {
                            #function
                        } else {
                            None
                        }
                    }),
                    (false, false, false) => parse_quote!({
                        let arg1 = #expr1;
                        let arg2 = #expr2;
                        let arg3 = #expr3;
                        #function
                    }),
                }
            }
            StringFunction::Concat(args) => {
                let pushes: Vec<syn::Expr> = args
                    .iter()
                    .map(|arg| {
                        let expr = arg.to_syn_expression();
                        if arg.nullable() {
                            parse_quote!(if let Some(to_append) = #expr {
                                result.push_str(&to_append);
                            })
                        } else {
                            parse_quote!(result.push_str(&#expr))
                        }
                    })
                    .collect();
                parse_quote!({
                    let mut result = String::new();
                    #(#pushes;)*
                    result
                })
            }
            StringFunction::ConcatWithSeparator(arg, args) => {
                let separator_expr = arg.to_syn_expression();
                let pushes: Vec<syn::Expr> = args
                    .iter()
                    .map(|arg| {
                        let expr = arg.to_syn_expression();
                        if arg.nullable() {
                            parse_quote!(if let Some(to_append) = #expr {
                                if !result.is_empty() {
                                    result.push_str(&separator);
                                }
                                result.push_str(&to_append);
                            })
                        } else {
                            parse_quote!({if !result.is_empty() {
                                result.push_str(&separator);
                            };result.push_str(&#expr)})
                        }
                    })
                    .collect();
                let non_null_computation: syn::Expr = parse_quote!({
                    let mut result = String::new();
                    #(#pushes;)*
                    result
                });
                if arg.nullable() {
                    parse_quote!({
                        if let Some(separator) = #separator_expr {
                            Some(#non_null_computation)
                        } else {
                            None
                        }
                    })
                } else {
                    parse_quote!({
                        let separator = #separator_expr;
                        #non_null_computation
                    })
                }
            }
            StringFunction::RegexpReplace(_, _, _, Some(_)) => unreachable!(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum DataStructureFunction {
    Coalesce(Vec<Expression>),
    NullIf {
        left: Box<Expression>,
        right: Box<Expression>,
    },
    MakeArray(Vec<Expression>),
}

impl DataStructureFunction {
    fn to_syn_expression(&self) -> syn::Expr {
        match self {
            DataStructureFunction::Coalesce(terms) => {
                let exprs: Vec<_> = terms
                    .iter()
                    .map(|term| (term.to_syn_expression(), term.nullable()))
                    .collect();
                // return the first Some() value, or None if all are None
                let mut iterator = exprs.into_iter();
                let (first_expression, nullable) = iterator.next().unwrap();
                if !nullable {
                    return first_expression;
                }
                let mut result = first_expression;
                for (syn_expr, nullable) in iterator {
                    if !nullable {
                        return parse_quote!(#result.unwrap_or_else(|| #syn_expr));
                    }
                    result = parse_quote!(#result.or_else(||#syn_expr));
                }
                result
            }
            DataStructureFunction::NullIf { left, right } => {
                let left_expr = left.to_syn_expression();
                let right_expr = right.to_syn_expression();
                let left_nullable = left.nullable();
                let right_nullable = right.nullable();
                match (left_nullable, right_nullable) {
                    (true, true) => parse_quote!({
                        let left = #left_expr;
                        let right = #right_expr;
                        if left == right {
                            None
                        } else {
                            left
                        }
                    }),
                    (true, false) => parse_quote!({
                        let left = #left_expr;
                        let right = #right_expr;
                        match left {
                            Some(left) if left == right => None,
                            _ => left,
                        }
                    }),
                    (false, true) => parse_quote!({
                        let left = #left_expr;
                        let right = #right_expr;
                        match right {
                            Some(right) if left == right => None,
                            _ => Some(left),
                        }
                    }),
                    (false, false) => parse_quote!({
                        let left = #left_expr;
                        let right = #right_expr;
                        if left == right {
                            None
                        } else {
                            Some(left)
                        }
                    }),
                }
            }
            DataStructureFunction::MakeArray(terms) => {
                if terms.iter().any(|term| term.nullable()) {
                    let entries: Vec<syn::Expr> = terms
                        .iter()
                        .map(|term| {
                            let expr = term.to_syn_expression();
                            if term.nullable() {
                                parse_quote!(#expr)
                            } else {
                                parse_quote!(Some(#expr))
                            }
                        })
                        .collect::<Vec<_>>();
                    parse_quote!(vec![#(#entries),*])
                } else {
                    let nullable = terms.iter().any(|term| term.nullable());
                    if nullable {
                        let entries = terms
                            .iter()
                            .map(|term| {
                                let expr = term.to_syn_expression();
                                if term.nullable() {
                                    parse_quote!(#expr)
                                } else {
                                    parse_quote!(Some(#expr))
                                }
                            })
                            .collect::<Vec<syn::Expr>>();
                        parse_quote!(vec![#(#entries),*])
                    } else {
                        let entries = terms.iter().map(|term| term.to_syn_expression());
                        parse_quote!(vec![#(#entries),*])
                    }
                }
            }
        }
    }
    fn return_type(&self) -> TypeDef {
        match self {
            DataStructureFunction::Coalesce(terms) => {
                let nullable = terms.iter().all(|term| term.nullable());
                terms[0].return_type().with_nullity(nullable)
            }
            DataStructureFunction::NullIf { left, right: _ } => left.return_type().as_nullable(),
            DataStructureFunction::MakeArray(terms) => {
                let TypeDef::DataType(primitive_type, _ ) = terms[0].return_type() else {
                    unreachable!("make_array should only be called on a primitive type")
                };
                let nullable = terms.iter().any(|term| term.nullable());
                TypeDef::DataType(
                    DataType::List(Arc::new(Field::new("items", primitive_type, nullable))),
                    false,
                )
            }
        }
    }
}

#[derive(Debug, Clone)]
enum JsonFunction {
    GetFirstJsonObject,
    GetJsonObjects,
    ExtractJsonString,
}

#[derive(Debug, Clone)]
pub struct JsonExpression {
    function: JsonFunction,
    json_string: Box<Expression>,
    path: Box<Expression>,
}

impl JsonExpression {
    fn to_syn_expression(&self) -> syn::Expr {
        let path_nullable = self.path.nullable();
        let json_nullable = self.json_string.nullable();
        let path_expr = self.path.to_syn_expression();
        let json_string_expr = self.json_string.to_syn_expression();
        let function_tokens = match self.function {
            JsonFunction::GetFirstJsonObject => quote!(get_first_json_object),
            JsonFunction::GetJsonObjects => quote!(get_json_objects),
            JsonFunction::ExtractJsonString => quote!(extract_json_string),
        };
        // Handle different nullabilities.
        match (path_nullable, json_nullable) {
            (true, true) => parse_quote!({
                if let (Some(path), Some(json_string)) = (#path_expr, #json_string_expr) {
                    arroyo_worker::operators::functions::json::#function_tokens(json_string, path)
                } else {
                    None
                }
            }),
            (true, false) => parse_quote!({
                let json_string = #json_string_expr;
                if let Some(path) = #path_expr {
                    arroyo_worker::operators::functions::json::#function_tokens(json_string, path)
                } else {
                    None
                }
            }),
            (false, true) => parse_quote!({
                let path = #path_expr;
                if let Some(json_string) = #json_string_expr {
                    arroyo_worker::operators::functions::json::#function_tokens(json_string, path)
                } else {
                    None
                }
            }),
            (false, false) => parse_quote!({
                let path = #path_expr;
                let json_string = #json_string_expr;
                arroyo_worker::operators::functions::json::#function_tokens(json_string, path)
            }),
        }
    }

    fn return_type(&self) -> TypeDef {
        match self.function {
            JsonFunction::GetFirstJsonObject => TypeDef::DataType(DataType::Utf8, true),
            JsonFunction::GetJsonObjects => TypeDef::DataType(
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
                true,
            ),
            JsonFunction::ExtractJsonString => TypeDef::DataType(DataType::Utf8, true),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RustUdfExpression {
    name: String,
    args: Vec<(TypeDef, Expression)>,
    ret_type: TypeDef,
}

impl RustUdfExpression {
    fn to_syn_expression(&self) -> syn::Expr {
        let name = format_ident!("{}", &self.name);

        let (defs, args): (Vec<_>, Vec<_>) = self
            .args
            .iter()
            .enumerate()
            .map(|(i, (def, expr))| {
                let t = expr.to_syn_expression();
                let id = format_ident!("__{}", i);
                let def = match (def.is_optional(), expr.nullable()) {
                    (true, true) | (false, false) => quote!(let #id = #t),
                    (true, false) => quote!(let #id = Some(#t)),
                    (false, true) => quote!(let #id = (#t)?),
                };
                (def, quote!(#id))
            })
            .unzip();

        let mut ret = quote!(udfs::#name(#(#args, )*));

        if self.return_type().is_optional() && !self.ret_type.is_optional() {
            // we have to wrap the result in Some
            ret = quote! { Some(#ret) }
        };

        parse_quote!({
            (|| {
                #(#defs; )*
                #ret
            })()
        })
    }

    fn return_type(&self) -> TypeDef {
        self.ret_type.with_nullity(
            self.ret_type.is_optional()
                || self
                    .args
                    .iter()
                    .any(|(t, e)| t.is_optional() && !e.nullable()),
        )
    }
}

#[derive(Debug, Clone)]
pub struct WrapTypeExpression {
    name: String,
    arg: Box<Expression>,
    ret_type: TypeDef,
}

impl WrapTypeExpression {
    fn new(name: &str, arg: Expression) -> Self {
        Self {
            name: name.to_string(),
            ret_type: arg.return_type(),
            arg: Box::new(arg),
        }
    }

    fn to_syn_expression(&self) -> syn::Expr {
        let path: Path = parse_str(&self.name).unwrap();
        let arg = self.arg.to_syn_expression();

        if self.arg.nullable() {
            parse_quote!(#arg.map_over_inner(|f| #path(f)))
        } else {
            parse_quote!(#path(#arg))
        }
    }

    fn return_type(&self) -> TypeDef {
        self.ret_type.clone()
    }
}

#[derive(Debug, Clone)]
pub enum CaseExpression {
    // match a single value to multiple potential matches
    Match {
        value: Box<Expression>,
        matches: Vec<(Box<Expression>, Box<Expression>)>,
        default: Option<Box<Expression>>,
    },
    // search for a true expression
    When {
        condition_pairs: Vec<(Box<Expression>, Box<Expression>)>,
        default: Option<Box<Expression>>,
    },
}

impl CaseExpression {
    fn new(
        primary_expr: Option<Box<Expression>>,
        when_then_expr: Vec<(Box<Expression>, Box<Expression>)>,
        else_expr: Option<Box<Expression>>,
    ) -> Self {
        match primary_expr {
            Some(primary_expr) => Self::Match {
                value: primary_expr,
                matches: when_then_expr,
                default: else_expr,
            },
            None => {
                // if there is no primary expression, then it's a when expression
                Self::When {
                    condition_pairs: when_then_expr,
                    default: else_expr,
                }
            }
        }
    }

    fn to_syn_expression(&self) -> syn::Expr {
        let nullable = self.nullable();
        match self {
            CaseExpression::Match {
                value,
                matches,
                default,
            } => {
                // It's easier to have value always be option and then return default if it is None.
                // It is possible to have more efficient code when all of the expressions are
                // not nullable and the default is not nullable, but it's not worth the complexity.
                let value = value.syn_expression_with_nullity(true);
                let if_clauses: Vec<syn::ExprIf> = matches
                    .iter()
                    .map(|(when_expr, then_expr)| {
                        let when_expr = when_expr.syn_expression_with_nullity(true);
                        let then_expr = then_expr.syn_expression_with_nullity(nullable);
                        parse_quote!(if #when_expr == value { #then_expr })
                    })
                    .collect();
                let default_expr = default
                    .as_ref()
                    .map(|d| d.syn_expression_with_nullity(nullable))
                    // this is safe because if default is null the result is nullable.
                    .unwrap_or_else(|| parse_quote!(None));
                parse_quote!({
                    let value = #value;
                    if value.is_none() {
                        #default_expr
                    } else #(#if_clauses else)* {
                            #default_expr
                        }
                })
            }
            CaseExpression::When {
                condition_pairs,
                default,
            } => {
                let if_clauses: Vec<syn::ExprIf> = condition_pairs
                    .iter()
                    .map(|(when_expr, then_expr)| {
                        let when_expr = when_expr.syn_expression_with_nullity(true);
                        let then_expr = then_expr.syn_expression_with_nullity(nullable);
                        parse_quote!(if #when_expr.unwrap_or(false) { #then_expr })
                    })
                    .collect();
                let default_expr = default
                    .as_ref()
                    .map(|d| d.syn_expression_with_nullity(nullable))
                    // this is safe because if default is null the result is nullable.
                    .unwrap_or_else(|| parse_quote!(None));
                parse_quote!({
                    #(#if_clauses else)* {
                        #default_expr
                    }
                })
            }
        }
    }

    fn nullable(&self) -> bool {
        match self {
            CaseExpression::Match {
                value: _,
                matches: pairs,
                default,
            }
            | CaseExpression::When {
                condition_pairs: pairs,
                default,
            } => {
                // if there is a nullable default or it is missing, then it is nullable. Otherwise, it is not nullable
                match default {
                    Some(default) => {
                        default.nullable()
                            || pairs
                                .iter()
                                .any(|(_when_expr, then_expr)| then_expr.nullable())
                    }
                    None => true,
                }
            }
        }
    }

    fn return_type(&self) -> TypeDef {
        match self {
            CaseExpression::Match {
                value: _,
                matches: pairs,
                default: _,
            }
            | CaseExpression::When {
                condition_pairs: pairs,
                default: _,
            } => {
                // guaranteed to have at least one pair.
                pairs[0].1.return_type().with_nullity(self.nullable())
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum DateTimeFunction {
    DateTrunc(Box<Expression>, Box<DateTruncPrecision>),
    DatePart(Box<Expression>, Box<DatePart>),
}

fn extract_literal_string(expr: Expression) -> Result<String, anyhow::Error> {
    let Expression::Literal(LiteralExpression{literal: ScalarValue::Utf8(Some(literal_string))}) = expr else {
        bail!("Can only convert a literal into a string")
    };
    Ok(literal_string.as_str().to_string())
}

impl TryFrom<(BuiltinScalarFunction, Vec<Expression>)> for DateTimeFunction {
    type Error = anyhow::Error;

    fn try_from(
        value: (BuiltinScalarFunction, Vec<Expression>),
    ) -> std::result::Result<Self, Self::Error> {
        let func = value.0;
        let mut args = value.1;
        match (args.len(), func) {
            (2, BuiltinScalarFunction::DatePart) => {
                let arg1 = args.remove(0);
                let arg2 = Box::new(args.remove(0));
                let date_part = extract_literal_string(arg1)?
                    .as_str()
                    .try_into()
                    .map_err(anyhow::Error::msg)?;
                Ok(DateTimeFunction::DatePart(arg2, Box::new(date_part)))
            }
            (2, BuiltinScalarFunction::DateTrunc) => {
                let arg1 = args.remove(0);
                let arg2 = Box::new(args.remove(0));
                let date_trunc_precision = extract_literal_string(arg1)?
                    .as_str()
                    .try_into()
                    .map_err(anyhow::Error::msg)?;
                Ok(DateTimeFunction::DateTrunc(
                    arg2,
                    Box::new(date_trunc_precision),
                ))
            }
            (_, func) => bail!("function {} with args {:?} not supported", func, args),
        }
    }
}

impl DateTimeFunction {
    fn non_null_function_invocation(&self) -> syn::Expr {
        match self {
            DateTimeFunction::DateTrunc(_, _) => {
                parse_quote!(arroyo_worker::operators::functions::datetime::date_trunc(
                    arg1, arg2
                ))
            }
            DateTimeFunction::DatePart(_, _) => {
                parse_quote!(arroyo_worker::operators::functions::datetime::date_part(
                    arg1, arg2
                ))
            }
        }
    }

    fn to_syn_expression(&self) -> syn::Expr {
        let function = self.non_null_function_invocation();
        let (null_arg, expr1, expr2): (bool, syn::Expr, syn::Expr) = match self {
            DateTimeFunction::DatePart(arg1, arg2) => {
                let arg2 = format!("arroyo_types::DatePart::{:?}", arg2);
                (
                    arg1.nullable(),
                    arg1.to_syn_expression(),
                    parse_str(&arg2).unwrap(),
                )
            }
            DateTimeFunction::DateTrunc(arg1, arg2) => {
                let arg2 = format!("arroyo_types::DateTruncPrecision::{:?}", arg2);
                (
                    arg1.nullable(),
                    arg1.to_syn_expression(),
                    parse_str(&arg2).unwrap(),
                )
            }
        };
        match null_arg {
            true => parse_quote!({
                use arroyo_types::{DatePart, DateTruncPrecision};
                if let Some(arg1) = #expr1 {
                    let arg2 = #expr2;
                    Some(#function)
                } else {
                    None
                }
            }),
            false => parse_quote!({
                use arroyo_types::{DatePart, DateTruncPrecision};
                let arg1 = #expr1;
                let arg2 = #expr2;
                #function
            }),
        }
    }

    fn return_type(&self) -> TypeDef {
        match self {
            DateTimeFunction::DateTrunc(arg, _) => TypeDef::DataType(
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                arg.nullable(),
            ),
            DateTimeFunction::DatePart(arg, _) => {
                TypeDef::DataType(DataType::UInt32, arg.nullable())
            }
        }
    }
}
