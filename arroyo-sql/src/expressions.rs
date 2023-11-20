use crate::{
    code_gen::{
        CodeGenerator, JoinListsContext, JoinPairContext, ValuePointerContext, VecOfPointersContext,
    },
    operators::TwoPhaseAggregation,
    pipeline::{JoinType, SortDirection, SqlOperator},
    schemas::window_type_def,
    types::{interval_month_day_nanos_to_duration, StructDef, StructField, TypeDef},
    ArroyoSchemaProvider,
};
use anyhow::{anyhow, bail, Ok, Result};
use arrow::datatypes::DataType;
use arrow_schema::{Field, TimeUnit};
use arroyo_datastream::WindowType;
use arroyo_types::{DatePart, DateTruncPrecision};
use datafusion_common::ScalarValue;
use datafusion_expr::{
    aggregate_function,
    expr::{AggregateUDF, Alias, ScalarFunction, ScalarUDF, Sort},
    type_coercion::aggregates::{avg_return_type, sum_return_type},
    BinaryExpr, BuiltinScalarFunction, Expr, GetFieldAccess, TryCast,
};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use regex::Regex;
use std::hash::Hash;
use std::{fmt::Debug, sync::Arc, time::Duration};
use syn::{parse_quote, parse_str, Ident, Path};

#[derive(Debug, Clone)]
pub struct BinaryOperator(datafusion_expr::Operator);

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
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
    WindowUDF(WindowType),
    Unnest(Box<Expression>, bool),
}

pub struct JoinedPairedStruct {
    pub left: StructDef,
    pub right: StructDef,
    pub join_type: JoinType,
}

impl JoinedPairedStruct {}

pub struct PairMerger;

impl CodeGenerator<JoinPairContext, StructDef, syn::Expr> for JoinType {
    fn generate(&self, input_context: &JoinPairContext) -> syn::Expr {
        let left_struct = &input_context.left_struct;
        let right_struct = &input_context.right_struct;
        let left_ident = input_context.left_ident();
        let right_ident = input_context.right_ident();

        let mut assignments: Vec<_> = vec![];

        left_struct.fields.iter().for_each(|field| {
                let field_name = format_ident!("{}",field.field_name());
                if self.left_nullable() {
                    if field.data_type.is_optional() {
                        assignments.push(quote!(#field_name : #left_ident.as_ref().map(|inner| inner.#field_name.clone()).flatten()));
                    } else {
                        assignments.push(quote!(#field_name : #left_ident.as_ref().map(|inner| inner.#field_name.clone())));
                    }
                } else {
                    assignments.push(quote!(#field_name : #left_ident.#field_name.clone()));
                }
            });
        right_struct.fields.iter().for_each(|field| {
                let field_name = format_ident!("{}",field.field_name());
                if self.right_nullable() {
                    if field.data_type.is_optional() {
                        assignments.push(quote!(#field_name : #right_ident.as_ref().map(|inner| inner.#field_name.clone()).flatten()));
                    } else {
                        assignments.push(quote!(#field_name : #right_ident.as_ref().map(|inner| inner.#field_name.clone())));
                    }
                } else {
                    assignments.push(quote!(#field_name :#right_ident.#field_name.clone()));
                }
            });

        let return_struct = self.expression_type(input_context);
        let return_type = return_struct.get_type();
        parse_quote!(
                #return_type {
                    #(#assignments)
                    ,*
                }
        )
    }

    fn expression_type(&self, input_context: &JoinPairContext) -> StructDef {
        let left_struct = &input_context.left_struct;
        let right_struct = &input_context.right_struct;
        // input to join should always be two structs. Nullability determined by join type.
        let mut fields = if self.left_nullable() {
            left_struct
                .fields
                .iter()
                .map(|field| field.as_nullable())
                .collect()
        } else {
            left_struct.fields.clone()
        };
        if self.right_nullable() {
            right_struct
                .fields
                .iter()
                .map(|field| field.as_nullable())
                .for_each(|field| fields.push(field))
        } else {
            fields.append(&mut right_struct.fields.clone());
        }
        StructDef::for_fields(fields)
    }
}

impl CodeGenerator<JoinListsContext, StructDef, syn::Expr> for JoinType {
    fn generate(&self, input_context: &JoinListsContext) -> syn::Expr {
        let pair_context = JoinPairContext {
            left_struct: input_context.left_struct.clone(),
            right_struct: input_context.right_struct.clone(),
        };
        let pair_expression = self.generate(&pair_context);
        let left_list_ident = input_context.left_list_ident();
        let right_list_ident = input_context.right_list_ident();
        let left_ident = pair_context.left_ident();
        let right_ident = pair_context.right_ident();
        let right_type = input_context.right_struct.get_type();
        let left_type = input_context.left_struct.get_type();
        match self {
            JoinType::Inner => {
                parse_quote!( {
                    let mut result = vec![];
                    for #left_ident in #left_list_ident {
                        for #right_ident in #right_list_ident {
                            result.push(#pair_expression)
                        }
                    }
                    result
                })
            }
            JoinType::Left => {
                parse_quote!( {
                    let mut result = vec![];
                    for #left_ident in #left_list_ident {
                        let mut found = false;
                        for #right_ident in #right_list_ident {
                            let #right_ident =:Option<#right_type> = Some(#right_ident);
                            result.push(#pair_expression);
                            found = true;
                        }
                        if !found {
                            let #right_ident : Option<#right_type> = None;
                            result.push(#pair_expression);
                        }
                    }
                    result
                })
            }
            JoinType::Right => {
                parse_quote!( {
                    let mut result = vec![];
                    for #right_ident in #right_list_ident {
                        let mut found = false;
                        for #left_ident in #left_list_ident {
                            result.push(#pair_expression);
                            found = true;
                        }
                        if !found {
                            let #left_ident : Option<#left_type> = None;
                            result.push(#pair_expression);
                        }
                    }
                    result
                })
            }
            JoinType::Full => {
                parse_quote!(
                    {
                        let mut result = vec![];
                        let left_empty = #left_list_ident.is_empty();
                        let right_empty = #right_list_ident.is_empty();
                        for #left_ident in #left_list_ident {
                            let #left_ident :Option<&#left_type> = Some(#left_ident);
                            for #right_ident in #right_list_ident {
                                let #right_ident :Option<&#right_type> = Some(#right_ident);
                                result.push(#pair_expression);
                            }
                            if right_empty {
                                let #right_ident : Option<#right_type> = None;
                                result.push(#pair_expression);
                            }
                        }
                        if left_empty {
                            let #left_ident : Option<#left_type> = None;
                            for #right_ident in #right_list_ident {
                                let #right_ident :Option<&#right_type> = Some(#right_ident);
                                result.push(#pair_expression);
                            }
                        }
                        result
                    }
                )
            }
        }
    }

    fn expression_type(&self, input_context: &JoinListsContext) -> StructDef {
        self.join_struct_type(&input_context.left_struct, &input_context.right_struct)
    }
}

impl CodeGenerator<ValuePointerContext, TypeDef, syn::Expr> for Expression {
    fn generate(&self, input_context: &ValuePointerContext) -> syn::Expr {
        match self {
            Expression::Column(column) => column.generate(input_context),
            Expression::UnaryBoolean(unary_boolean) => unary_boolean.generate(input_context),
            Expression::Literal(literal) => literal.generate(input_context),
            Expression::BinaryComparison(comparison) => comparison.generate(input_context),
            Expression::BinaryMath(math) => math.generate(input_context),
            Expression::StructField(struct_field) => struct_field.generate(input_context),
            Expression::Aggregation(_aggregation) => panic!("don't expect to reach here"),
            Expression::Cast(cast) => cast.generate(input_context),
            Expression::Numeric(numeric) => numeric.generate(input_context),
            Expression::Date(date) => date.generate(input_context),
            Expression::String(string) => string.generate(input_context),
            Expression::Hash(hash) => hash.generate(input_context),
            Expression::DataStructure(data_structure) => data_structure.generate(input_context),
            Expression::Json(json) => json.generate(input_context),
            Expression::RustUdf(udf) => udf.generate(input_context),
            Expression::WrapType(wrap_type) => wrap_type.generate(input_context),
            Expression::Case(case) => case.generate(input_context),
            Expression::WindowUDF(_window_type) => {
                unreachable!("window functions shouldn't be computed off of a value pointer")
            }
            Expression::Unnest(_, taken) => {
                if !taken {
                    panic!("unnest appeared in a non-projection context");
                } else {
                    let ident = input_context.variable_ident();
                    parse_quote!(#ident.clone())
                }
            }
        }
    }

    fn expression_type(&self, input_context: &ValuePointerContext) -> TypeDef {
        match self {
            Expression::Column(column_expression) => {
                column_expression.expression_type(input_context)
            }
            Expression::UnaryBoolean(unary_boolean_expression) => {
                unary_boolean_expression.expression_type(input_context)
            }
            Expression::Literal(literal_expression) => {
                literal_expression.expression_type(input_context)
            }
            Expression::BinaryComparison(comparison_expression) => {
                comparison_expression.expression_type(input_context)
            }
            Expression::BinaryMath(math_expression) => {
                math_expression.expression_type(input_context)
            }
            Expression::StructField(struct_field_expression) => {
                struct_field_expression.expression_type(input_context)
            }
            Expression::Aggregation(_aggregation_expression) => {
                unreachable!("aggregates shouldn't actually be expressions!")
            }
            Expression::Cast(cast_expression) => cast_expression.expression_type(input_context),
            Expression::Numeric(numeric_expression) => {
                numeric_expression.expression_type(input_context)
            }
            Expression::Date(date_function) => date_function.expression_type(input_context),
            Expression::String(string_function) => string_function.expression_type(input_context),
            Expression::Hash(hash_expression) => hash_expression.expression_type(input_context),
            Expression::DataStructure(data_structure_expression) => {
                data_structure_expression.expression_type(input_context)
            }
            Expression::Json(json_function) => json_function.expression_type(input_context),
            Expression::RustUdf(t) => t.expression_type(input_context),
            Expression::WrapType(t) => t.expression_type(input_context),
            Expression::Case(case_statement) => case_statement.expression_type(input_context),
            Expression::WindowUDF(_window_type) => {
                unreachable!("window functions shouldn't be computed off of a value pointer")
            }
            Expression::Unnest(t, _) => match t.expression_type(input_context) {
                TypeDef::DataType(DataType::List(inner), _) => {
                    TypeDef::DataType(inner.data_type().clone(), false)
                }
                _ => {
                    unreachable!("unnest argument must be an array");
                }
            },
        }
    }
}

impl Expression {
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

    pub(crate) fn get_window_type(&self, input: &SqlOperator) -> Result<Option<WindowType>> {
        match self {
            Expression::Column(column) => {
                if let Some(window_type) = input.get_window() {
                    let column_field = input
                        .return_type()
                        .get_field(column.column_field.alias.clone(), &column.column_field.name)?;
                    if column_field.data_type == window_type_def() {
                        return Ok(Some(window_type.clone()));
                    }
                }
                Ok(None)
            }
            Expression::WindowUDF(window_type) => {
                if let Some(input_window_type) = input.get_window() {
                    if input_window_type != *window_type {
                        bail!(
                            "window type mismatch: {:?} != {:?}",
                            input_window_type,
                            window_type
                        );
                    }
                }
                Ok(Some(window_type.clone()))
            }
            Expression::UnaryBoolean(_)
            | Expression::Literal(_)
            | Expression::BinaryComparison(_)
            | Expression::BinaryMath(_)
            | Expression::StructField(_)
            | Expression::Aggregation(_)
            | Expression::Cast(_)
            | Expression::Numeric(_)
            | Expression::Date(_)
            | Expression::String(_)
            | Expression::Hash(_)
            | Expression::DataStructure(_)
            | Expression::Json(_)
            | Expression::RustUdf(_)
            | Expression::WrapType(_)
            | Expression::Unnest(_, _)
            | Expression::Case(_) => Ok(None),
        }
    }
    fn get_duration(expression: &Expr) -> Result<Duration> {
        match expression {
            Expr::Literal(ScalarValue::IntervalDayTime(Some(val))) => {
                Ok(Duration::from_millis(*val as u64))
            }
            Expr::Literal(ScalarValue::IntervalMonthDayNano(Some(val))) => {
                Ok(interval_month_day_nanos_to_duration(*val))
            }
            _ => bail!(
                "unsupported Duration expression, expect duration literal, not {}",
                expression
            ),
        }
    }

    pub fn traverse_mut<T, F: Fn(&mut T, &mut Expression) -> ()>(
        &mut self,
        context: &mut T,
        f: &F,
    ) {
        match self {
            Expression::Column(_) => {}
            Expression::UnaryBoolean(e) => {
                (&mut *e.input).traverse_mut(context, f);
            }
            Expression::Literal(_) => {}
            Expression::BinaryComparison(e) => {
                (&mut *e.left).traverse_mut(context, f);
                (&mut *e.right).traverse_mut(context, f);
            }
            Expression::BinaryMath(e) => {
                (&mut *e.left).traverse_mut(context, f);
                (&mut *e.right).traverse_mut(context, f);
            }
            Expression::StructField(e) => {
                (&mut *e.struct_expression).traverse_mut(context, f);
            }
            Expression::Aggregation(e) => {
                (&mut *e.producing_expression).traverse_mut(context, f);
            }
            Expression::Cast(e) => {
                (&mut *e.input).traverse_mut(context, f);
            }
            Expression::Numeric(e) => {
                (&mut *e.input).traverse_mut(context, f);
            }
            Expression::Date(e) => match e {
                DateTimeFunction::DatePart(_, e)
                | DateTimeFunction::DateTrunc(_, e)
                | DateTimeFunction::FromUnixTime(e) => {
                    (&mut *e).traverse_mut(context, f);
                }
            },
            Expression::String(e) => {
                for e in e.expressions() {
                    e.traverse_mut(context, f);
                }
            }
            Expression::Hash(e) => {
                (&mut *e.input).traverse_mut(context, f);
            }
            Expression::DataStructure(e) => match e {
                DataStructureFunction::Coalesce(exprs)
                | DataStructureFunction::MakeArray(exprs) => {
                    for e in exprs {
                        e.traverse_mut(context, f);
                    }
                }
                DataStructureFunction::NullIf { left, right } => {
                    (&mut *left).traverse_mut(context, f);
                    (&mut *right).traverse_mut(context, f);
                }
                DataStructureFunction::ArrayIndex {
                    array_expression,
                    index: _,
                } => {
                    (&mut *array_expression).traverse_mut(context, f);
                }
            },
            Expression::Json(e) => {
                (&mut *e.json_string).traverse_mut(context, f);
                (&mut *e.path).traverse_mut(context, f);
            }
            Expression::RustUdf(udf) => {
                for (_, arg) in &mut udf.args {
                    arg.traverse_mut(context, f);
                }
            }
            Expression::WrapType(e) => {
                (&mut *e.arg).traverse_mut(context, f);
            }
            Expression::Case(e) => match e {
                CaseExpression::Match {
                    value,
                    matches,
                    default,
                } => {
                    f(context, &mut *value);
                    for (l, r) in matches {
                        (&mut *l).traverse_mut(context, f);
                        (&mut *r).traverse_mut(context, f);
                    }
                    if let Some(default) = default {
                        default.traverse_mut(context, f);
                    }
                }
                CaseExpression::When {
                    condition_pairs,
                    default,
                } => {
                    for (l, r) in condition_pairs {
                        (&mut *l).traverse_mut(context, f);
                        (&mut *r).traverse_mut(context, f);
                    }
                    if let Some(default) = default {
                        (default).traverse_mut(context, f);
                    }
                }
            },
            Expression::WindowUDF(_) => {}
            Expression::Unnest(n, _) => {
                (&mut *n).traverse_mut(context, f);
            }
        }

        f(context, self);
    }
}

pub struct ExpressionContext<'a> {
    pub schema_provider: &'a ArroyoSchemaProvider,
    pub input_struct: &'a StructDef,
}

impl<'a> ExpressionContext<'a> {
    pub fn compile_expr(&self, expression: &Expr) -> Result<Expression> {
        match expression {
            Expr::Alias(datafusion_expr::expr::Alias { expr, name: _ }) => self.compile_expr(expr),
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
                | datafusion_expr::Operator::BitwiseShiftLeft
                | datafusion_expr::Operator::ArrowAt
                | datafusion_expr::Operator::AtArrow => bail!("{:?} is unimplemented", op),
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
            Expr::GetIndexedField(datafusion_expr::GetIndexedField { expr, field }) => {
                StructFieldExpression::new(Box::new(self.compile_expr(expr)?), field)
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
                &ValuePointerContext::new(),
                false,
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
                        // TODO: Figure out how to detect this
                        //if matches!(arg_expressions[0].expression_type(input_context), TypeDef::StructDef(_, _)) {
                        //    bail!("make_array only supports primitive types");
                        //};
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
                        &ValuePointerContext::new(),
                        false,
                    ),
                    BuiltinScalarFunction::ToTimestampMillis => CastExpression::new(
                        Box::new(arg_expressions.remove(0)),
                        &DataType::Timestamp(TimeUnit::Millisecond, None),
                        &ValuePointerContext::new(),
                        false,
                    ),
                    BuiltinScalarFunction::ToTimestampMicros => CastExpression::new(
                        Box::new(arg_expressions.remove(0)),
                        &DataType::Timestamp(TimeUnit::Microsecond, None),
                        &ValuePointerContext::new(),
                        false,
                    ),
                    BuiltinScalarFunction::ToTimestampSeconds => CastExpression::new(
                        Box::new(arg_expressions.remove(0)),
                        &DataType::Timestamp(TimeUnit::Second, None),
                        &ValuePointerContext::new(),
                        false,
                    ),
                    BuiltinScalarFunction::FromUnixtime => Ok(Expression::Date(
                        DateTimeFunction::FromUnixTime(Box::new(arg_expressions.remove(0))),
                    )),
                    BuiltinScalarFunction::DateBin
                    | BuiltinScalarFunction::CurrentDate
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
                    BuiltinScalarFunction::DatePart => DateTimeFunction::date_part(
                        arg_expressions.remove(0),
                        arg_expressions.remove(0),
                    ),
                    BuiltinScalarFunction::DateTrunc => DateTimeFunction::date_trunc(
                        arg_expressions.remove(0),
                        arg_expressions.remove(0),
                    ),
                    BuiltinScalarFunction::Decode => bail!("decode not implemented yet"),
                    BuiltinScalarFunction::Encode => bail!("encode not implemented yet"),
                    BuiltinScalarFunction::Cot => bail!("cot not implemented yet"),
                    BuiltinScalarFunction::ArrayAppend
                    | BuiltinScalarFunction::ArrayConcat
                    | BuiltinScalarFunction::ArrayDims
                    | BuiltinScalarFunction::ArrayLength
                    | BuiltinScalarFunction::ArrayNdims
                    | BuiltinScalarFunction::ArrayPosition
                    | BuiltinScalarFunction::ArrayPositions
                    | BuiltinScalarFunction::ArrayPrepend
                    | BuiltinScalarFunction::ArrayRemove
                    | BuiltinScalarFunction::ArrayReplace
                    | BuiltinScalarFunction::ArrayToString
                    | BuiltinScalarFunction::Cardinality
                    | BuiltinScalarFunction::ArrayHas
                    | BuiltinScalarFunction::ArrayHasAll
                    | BuiltinScalarFunction::ArrayHasAny
                    | BuiltinScalarFunction::ArrayPopBack
                    | BuiltinScalarFunction::ArrayElement
                    | BuiltinScalarFunction::ArrayEmpty
                    | BuiltinScalarFunction::ArrayRemoveN
                    | BuiltinScalarFunction::ArrayRemoveAll
                    | BuiltinScalarFunction::ArrayRepeat
                    | BuiltinScalarFunction::ArrayReplaceN
                    | BuiltinScalarFunction::ArrayReplaceAll
                    | BuiltinScalarFunction::ArraySlice
                    | BuiltinScalarFunction::Flatten => {
                        bail!("array functions not implemented yet")
                    }
                    BuiltinScalarFunction::Isnan => todo!(),
                    BuiltinScalarFunction::Iszero => todo!(),
                    BuiltinScalarFunction::Nanvl => todo!(),
                }
            }
            Expr::ScalarUDF(ScalarUDF { fun, args }) => match fun.name.as_str() {
                "get_first_json_object" => JsonExpression::new(
                    JsonFunction::GetFirstJsonObject,
                    self.compile_expr(&args[0])?,
                    self.compile_expr(&args[1])?,
                ),
                "get_json_objects" | "extract_json" => JsonExpression::new(
                    JsonFunction::GetJsonObjects,
                    self.compile_expr(&args[0])?,
                    self.compile_expr(&args[1])?,
                ),
                "extract_json_string" | "json_extract_string" => JsonExpression::new(
                    JsonFunction::ExtractJsonString,
                    self.compile_expr(&args[0])?,
                    self.compile_expr(&args[1])?,
                ),
                "hop" => {
                    if args.len() != 2 {
                        bail!("wrong number of arguments for hop(), expected two");
                    }
                    let slide = Expression::get_duration(&args[0])?;
                    let width = Expression::get_duration(&args[1])?;
                    Ok(Expression::WindowUDF(WindowType::Sliding { width, slide }))
                }
                "tumble" => {
                    if args.len() != 1 {
                        bail!("wrong number of arguments for tumble(), expect one");
                    }
                    let width = Expression::get_duration(&args[0])?;
                    Ok(Expression::WindowUDF(WindowType::Tumbling { width }))
                }
                "session" => {
                    if args.len() != 1 {
                        bail!("wrong number of arguments for session(), expected one");
                    }
                    let gap = Expression::get_duration(&args[0])?;
                    Ok(Expression::WindowUDF(WindowType::Session { gap }))
                }
                "unnest" => {
                    if args.len() != 1 {
                        bail!("wrong number of arguments for unnest(), expected one");
                    }
                    Ok(Expression::Unnest(
                        Box::new(self.compile_expr(&args[0])?),
                        false,
                    ))
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

#[derive(Clone, Debug)]
pub enum AggregateResultExtraction {
    WindowTake,
    KeyColumn,
}

impl AggregateResultExtraction {
    // TODO: this should probably be in a code generator.
    pub fn get_column_assignment(
        &self,
        struct_field: &StructField,
        window_ident: syn::Ident,
        key_ident: syn::Ident,
    ) -> syn::FieldValue {
        match self {
            AggregateResultExtraction::WindowTake => {
                let field_ident = struct_field.field_ident();
                parse_quote!(#field_ident: #window_ident)
            }
            AggregateResultExtraction::KeyColumn => {
                let key_field_ident = struct_field.field_ident();
                parse_quote!(#key_field_ident: #key_ident.#key_field_ident.clone())
            }
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
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
}

impl CodeGenerator<ValuePointerContext, TypeDef, syn::Expr> for ColumnExpression {
    fn generate(&self, input_context: &ValuePointerContext) -> syn::Expr {
        let field_ident = self.column_field.field_ident();
        let argument_ident = input_context.variable_ident();
        parse_quote!(#argument_ident.#field_ident.clone())
    }

    fn expression_type(&self, _input_context: &ValuePointerContext) -> TypeDef {
        self.column_field.data_type.clone()
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
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

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
pub struct UnaryBooleanExpression {
    operator: UnaryOperator,
    input: Box<Expression>,
}

impl UnaryBooleanExpression {
    fn new(operator: UnaryOperator, input: Box<Expression>) -> Expression {
        Expression::UnaryBoolean(UnaryBooleanExpression { operator, input })
    }
}

impl CodeGenerator<ValuePointerContext, TypeDef, syn::Expr> for UnaryBooleanExpression {
    fn generate(&self, input_context: &ValuePointerContext) -> syn::Expr {
        let argument_expr = self.input.generate(input_context);
        match (
            self.input.expression_type(input_context).is_optional(),
            &self.operator,
        ) {
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

    fn expression_type(&self, input_context: &ValuePointerContext) -> TypeDef {
        match &self.operator {
            UnaryOperator::IsNotNull
            | UnaryOperator::IsTrue
            | UnaryOperator::IsFalse
            | UnaryOperator::IsUnknown
            | UnaryOperator::IsNotTrue
            | UnaryOperator::IsNotFalse
            | UnaryOperator::IsNotUnknown
            | UnaryOperator::IsNull => TypeDef::DataType(DataType::Boolean, false),
            UnaryOperator::Negative => self.input.expression_type(input_context),
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
pub struct LiteralExpression {
    literal: ScalarValue,
}

impl LiteralExpression {
    fn expression_type(&self, _input_context: &ValuePointerContext) -> TypeDef {
        TypeDef::DataType(self.literal.get_datatype(), self.literal.is_null())
    }

    fn new(literal: ScalarValue) -> Expression {
        Expression::Literal(Self { literal })
    }
}

impl CodeGenerator<ValuePointerContext, TypeDef, syn::Expr> for LiteralExpression {
    fn generate(&self, _input_context: &ValuePointerContext) -> syn::Expr {
        TypeDef::get_literal(&self.literal)
    }

    fn expression_type(&self, _input_context: &ValuePointerContext) -> TypeDef {
        TypeDef::DataType(self.literal.get_datatype(), self.literal.is_null())
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
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
impl BinaryComparison {
    fn never_null(&self) -> bool {
        match self {
            BinaryComparison::IsDistinctFrom | BinaryComparison::IsNotDistinctFrom => true,
            _ => false,
        }
    }
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

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
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
    fn generate(&self, input_context: &ValuePointerContext) -> syn::Expr {
        let left_expr = self.left.generate(input_context);
        let right_expr = self.right.generate(input_context);

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
            self.left.expression_type(input_context).is_optional(),
            self.right.expression_type(input_context).is_optional(),
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

    fn expression_type(&self, input_context: &ValuePointerContext) -> TypeDef {
        TypeDef::DataType(
            DataType::Boolean,
            self.left.expression_type(input_context).is_optional()
                || self.right.expression_type(input_context).is_optional(),
        )
    }
}

impl CodeGenerator<ValuePointerContext, TypeDef, syn::Expr> for BinaryComparisonExpression {
    fn generate(&self, input_context: &ValuePointerContext) -> syn::Expr {
        let left_expr = self.left.generate(input_context);
        let right_expr = self.right.generate(input_context);

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
            self.left.expression_type(input_context).is_optional(),
            self.right.expression_type(input_context).is_optional(),
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

    fn expression_type(&self, input_context: &ValuePointerContext) -> TypeDef {
        // non-nullable comparisons are never optional
        if self.op.never_null() {
            return TypeDef::DataType(DataType::Boolean, false);
        }
        let inputs_optional = self.left.expression_type(input_context).is_optional()
            || self.right.expression_type(input_context).is_optional();
        TypeDef::DataType(DataType::Boolean, inputs_optional)
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
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

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]

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

impl CodeGenerator<ValuePointerContext, TypeDef, syn::Expr> for BinaryMathExpression {
    fn generate(&self, input_context: &ValuePointerContext) -> syn::Expr {
        let left_expr = self.left.generate(input_context);
        let right_expr = self.right.generate(input_context);
        let op: TokenStream = self.op.as_tokens();
        match (
            self.left.expression_type(input_context).is_optional(),
            self.right.expression_type(input_context).is_optional(),
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

    fn expression_type(&self, input_context: &ValuePointerContext) -> TypeDef {
        let nullable = self.left.expression_type(input_context).is_optional()
            || self.right.expression_type(input_context).is_optional();
        self.left
            .expression_type(input_context)
            .with_nullity(nullable)
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
pub struct StructFieldExpression {
    // TODO: better type this as a struct producing expression
    struct_expression: Box<Expression>,
    field_name: String,
}

impl StructFieldExpression {
    fn new(struct_expression: Box<Expression>, key: &GetFieldAccess) -> Result<Expression> {
        match key {
            GetFieldAccess::NamedStructField {
                name: ScalarValue::Utf8(Some(column)),
            } => Ok(Expression::StructField(Self {
                struct_expression,
                field_name: column.to_string(),
            })),
            GetFieldAccess::ListIndex { key } => {
                let Expr::Literal(ScalarValue::Int64(Some(index))) = **key else {
                    bail!("list indices must be scalar integers");
                };
                if index <= 0 {
                    bail!("the index into a list must be greater than 0")
                }
                Ok(Expression::DataStructure(
                    DataStructureFunction::ArrayIndex {
                        array_expression: struct_expression,
                        index: index as usize,
                    },
                ))
            }
            _ => bail!("don't support key {:?} for field access", key),
        }
    }
}
impl CodeGenerator<ValuePointerContext, TypeDef, syn::Expr> for StructFieldExpression {
    fn generate(&self, input_context: &ValuePointerContext) -> syn::Expr {
        let struct_type = self.struct_expression.expression_type(input_context);
        let TypeDef::StructDef(struct_def, struct_nullable) = struct_type else {
            unreachable!("struct field expression should always be over a struct");
        };
        let struct_expression = self.struct_expression.generate(input_context);
        let struct_field = struct_def
            .get_field(None, &self.field_name)
            .expect("should contain struct field");
        let field_ident = struct_field.field_ident();
        match (struct_nullable, struct_field.nullable()) {
            (true, true) => {
                parse_quote!(#struct_expression.map(|arg| arg.#field_ident.clone()).flatten())
            }
            (true, false) => parse_quote!(#struct_expression.map(|arg| arg.#field_ident.clone())),
            (false, true) => parse_quote!(#struct_expression.#field_ident.clone()),
            (false, false) => parse_quote!(#struct_expression.#field_ident.clone()),
        }
    }

    fn expression_type(&self, input_context: &ValuePointerContext) -> TypeDef {
        let struct_type = self.struct_expression.expression_type(input_context);
        let TypeDef::StructDef(struct_def, struct_nullable) = struct_type else {
            unreachable!("struct field expression should always be over a struct");
        };
        let struct_field = struct_def
            .get_field(None, &self.field_name)
            .expect("should contain struct field");
        if struct_nullable {
            struct_field.data_type.as_nullable()
        } else {
            struct_field.data_type.clone()
        }
    }
}

#[derive(Debug, Clone)]
pub enum AggregateComputation {
    Builtin {
        column: Column,
        computation: AggregationExpression,
    },
    UDAF {
        column: Column,
        computation: RustUdafExpression,
    },
}
impl AggregateComputation {
    pub fn allows_two_phase(&self) -> bool {
        match self {
            AggregateComputation::Builtin { computation, .. } => computation.allows_two_phase(),
            AggregateComputation::UDAF { .. } => false,
        }
    }
    pub fn try_from_expression(
        ctx: &mut ExpressionContext,
        column: &datafusion_common::Column,
        expr: &Expr,
    ) -> Result<Self> {
        match expr {
            Expr::Alias(Alias { expr, .. }) => Self::try_from_expression(ctx, column, expr),
            Expr::AggregateFunction(aggregate_function) => {
                let computation =
                    AggregationExpression::try_from_aggregate_function(ctx, aggregate_function)?;
                Ok(Self::Builtin {
                    column: Column::convert(column),
                    computation,
                })
            }
            Expr::AggregateUDF(aggregate_udf) => {
                let computation = RustUdafExpression::try_from_aggregate_udf(ctx, aggregate_udf)?;
                Ok(Self::UDAF {
                    column: Column::convert(column),
                    computation,
                })
            }
            _ => bail!("can't convert expression {:?} to aggregate", expr),
        }
    }

    pub(crate) fn column(&self) -> Column {
        match self {
            AggregateComputation::Builtin { column, .. }
            | AggregateComputation::UDAF { column, .. } => column.clone(),
        }
    }
}

impl CodeGenerator<VecOfPointersContext, TypeDef, syn::Expr> for AggregateComputation {
    fn generate(&self, input_context: &VecOfPointersContext) -> syn::Expr {
        match self {
            AggregateComputation::Builtin { computation, .. } => {
                computation.generate(input_context)
            }
            AggregateComputation::UDAF { computation, .. } => computation.generate(input_context),
        }
    }

    fn expression_type(&self, input_context: &VecOfPointersContext) -> TypeDef {
        match self {
            AggregateComputation::Builtin { computation, .. } => {
                computation.expression_type(input_context)
            }
            AggregateComputation::UDAF { computation, .. } => {
                computation.expression_type(input_context)
            }
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
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

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
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

    pub fn try_from_aggregate_function(
        ctx: &mut ExpressionContext,
        aggregate_function: &datafusion_expr::expr::AggregateFunction,
    ) -> Result<Self> {
        if aggregate_function.filter.is_some() {
            bail!("Not supporting aggregate filters right now");
        }
        if aggregate_function.order_by.is_some() {
            bail!("Not supporting aggregate sorts right now");
        }
        let fun = &aggregate_function.fun;
        let distinct = aggregate_function.distinct;
        let args = &aggregate_function.args;

        if args.len() != 1 {
            bail!("unexpected arg length");
        }
        let producing_expression = Box::new(ctx.compile_expr(&args[0])?);
        let aggregator = Aggregator::from_datafusion(fun.clone(), distinct)?;
        Ok(AggregationExpression {
            producing_expression,
            aggregator,
        })
    }
}

impl CodeGenerator<VecOfPointersContext, TypeDef, syn::Expr> for AggregationExpression {
    fn generate(&self, input_context: &VecOfPointersContext) -> syn::Expr {
        let single_value_context = ValuePointerContext::new();
        let sub_expr = self.producing_expression.generate(&single_value_context);
        let single_value_ident = single_value_context.variable_ident();
        let vec_ident = input_context.variable_ident();
        let producing_expression_is_optional = self
            .producing_expression
            .expression_type(&single_value_context)
            .is_optional();
        let map_type = if producing_expression_is_optional {
            quote!(filter_map)
        } else {
            quote!(map)
        };
        let unwrap = if producing_expression_is_optional {
            None
        } else {
            Some(quote!(.unwrap()))
        };

        match &self.aggregator {
            Aggregator::Count => {
                if producing_expression_is_optional {
                    parse_quote!({
                        #vec_ident.iter()
                            .filter_map(|#single_value_ident| #sub_expr)
                            .count() as i64
                    })
                } else {
                    parse_quote!((#vec_ident.len() as i64))
                }
            }
            Aggregator::Sum => parse_quote!({
                #vec_ident.iter()
                    .#map_type(|#single_value_ident| #sub_expr)
                    .reduce(|left, right| left + right)
                    #unwrap
            }),
            Aggregator::Min => parse_quote!({
                #vec_ident.iter()
                    .#map_type(|#single_value_ident| #sub_expr)
                    .reduce( |left, right| left.min(right))
                    #unwrap
            }),
            Aggregator::Max => parse_quote!({
                #vec_ident.iter()
                    .#map_type(|#single_value_ident| #sub_expr)
                    .reduce(|left, right| left.max(right))
                    #unwrap
            }),
            Aggregator::Avg => parse_quote!({
                #vec_ident.iter()
                    .#map_type(|#single_value_ident| #sub_expr)
                    .map(|val| (1, val))
                    .reduce(|left, right| (left.0 + right.0, left.1+right.1))
                    .map(|result| (result.1 as f64)/(result.0 as f64))
                    #unwrap
            }),
            Aggregator::CountDistinct => parse_quote! ({
                #vec_ident.iter()
                    .#map_type(|#single_value_ident| #sub_expr)
                    .collect::<std::collections::HashSet<_>>()
                    .len() as i64
            }),
        }
    }

    fn expression_type(&self, _input_context: &VecOfPointersContext) -> TypeDef {
        match &self.aggregator {
            Aggregator::Count | Aggregator::CountDistinct => {
                TypeDef::DataType(DataType::Int64, false)
            }
            aggregator => {
                let single_value_context = ValuePointerContext::new();
                let input_type = self
                    .producing_expression
                    .expression_type(&single_value_context);
                let is_optional = input_type.is_optional();
                TypeDef::DataType(aggregator.return_data_type(input_type), is_optional)
            }
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
pub struct CastExpression {
    input: Box<Expression>,
    data_type: DataType,
    force_nullable: bool,
}

impl CastExpression {
    pub fn new(
        input: Box<Expression>,
        data_type: &DataType,
        input_context: &ValuePointerContext,
        force_nullable: bool,
    ) -> Result<Expression> {
        if let TypeDef::DataType(input_type, _) = input.expression_type(input_context) {
            if Self::allowed_types(&input_type, data_type) {
                Ok(Expression::Cast(Self {
                    input,
                    data_type: data_type.clone(),
                    force_nullable,
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
}

impl CodeGenerator<ValuePointerContext, TypeDef, syn::Expr> for CastExpression {
    fn generate(&self, input_context: &ValuePointerContext) -> syn::Expr {
        let sub_expr = self.input.generate(input_context);
        let TypeDef::DataType(input_type, nullable) = self.input.expression_type(input_context)
        else {
            unreachable!()
        };
        if nullable {
            let cast_expr = Self::cast_expr(&input_type, &self.data_type, parse_quote!(x));
            parse_quote!(#sub_expr.map(|x| #cast_expr))
        } else {
            let cast_expr = Self::cast_expr(&input_type, &self.data_type, sub_expr);
            if self.force_nullable {
                parse_quote!(Some(#cast_expr))
            } else {
                parse_quote!(#cast_expr)
            }
        }
    }

    fn expression_type(&self, input_context: &ValuePointerContext) -> TypeDef {
        TypeDef::DataType(
            self.data_type.clone(),
            self.input.expression_type(input_context).is_optional() || self.force_nullable,
        )
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
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

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
pub struct NumericExpression {
    function: NumericFunction,
    input: Box<Expression>,
}

impl NumericExpression {
    fn new(function: BuiltinScalarFunction, input: Box<Expression>) -> Result<Expression> {
        let function = function.try_into()?;
        Ok(Expression::Numeric(NumericExpression { function, input }))
    }
}

impl CodeGenerator<ValuePointerContext, TypeDef, syn::Expr> for NumericExpression {
    fn generate(&self, input_context: &ValuePointerContext) -> syn::Expr {
        let function_name = self.function.function_name();
        let argument_expression = self.input.generate(input_context);
        if self.input.expression_type(input_context).is_optional() {
            parse_quote!(#argument_expression.map(|val| (val as f64).#function_name()))
        } else {
            parse_quote!((#argument_expression as f64).#function_name())
        }
    }

    fn expression_type(&self, input_context: &ValuePointerContext) -> TypeDef {
        TypeDef::DataType(
            DataType::Float64,
            self.input.expression_type(input_context).is_optional(),
        )
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
pub struct SortExpression {
    value: Expression,
    direction: SortDirection,
    nulls_first: bool,
}

impl SortExpression {
    pub fn expression(&mut self) -> &mut Expression {
        &mut self.value
    }

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

    fn tuple_type(&self, input_context: &ValuePointerContext) -> syn::Type {
        let value_type = if self.value.expression_type(input_context).is_float() {
            let t = self.value.expression_type(input_context).return_type();
            parse_quote! { arroyo_worker::OrderedFloat<#t> }
        } else {
            self.value.expression_type(input_context).return_type()
        };

        match (
            self.value.expression_type(input_context).is_optional(),
            &self.direction,
            self.nulls_first,
        ) {
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
                let singleton_type = sort_expressions[0].tuple_type(&ValuePointerContext::new());
                parse_quote!((#singleton_type,))
            }
            _ => {
                let tuple_types: Vec<syn::Type> = sort_expressions
                    .iter()
                    .map(|sort_expression| sort_expression.tuple_type(&ValuePointerContext::new()))
                    .collect();
                parse_quote!((#(#tuple_types),*))
            }
        }
    }

    pub fn sort_tuple_expression(sort_expressions: &Vec<SortExpression>) -> syn::Expr {
        match sort_expressions.len() {
            0 => parse_quote!(()),
            1 => {
                let singleton_expr = sort_expressions[0].to_syn_expr(&ValuePointerContext::new());
                parse_quote!((#singleton_expr,))
            }
            _ => {
                let tuple_exprs: Vec<syn::Expr> = sort_expressions
                    .iter()
                    .map(|sort_expression| sort_expression.to_syn_expr(&ValuePointerContext::new()))
                    .collect();
                parse_quote!((#(#tuple_exprs),*))
            }
        }
    }

    fn to_syn_expr(&self, input_context: &ValuePointerContext) -> syn::Expr {
        let value = if self.value.expression_type(input_context).is_float() {
            Expression::WrapType(WrapTypeExpression::new(
                "arroyo_worker::OrderedFloat",
                self.value.clone(),
            ))
        } else {
            self.value.clone()
        };

        let value_expr = value.generate(input_context);
        match (
            self.value.expression_type(input_context).is_optional(),
            &self.direction,
            self.nulls_first,
        ) {
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

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
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

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
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

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
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
}

impl CodeGenerator<ValuePointerContext, TypeDef, syn::Expr> for HashExpression {
    fn generate(&self, input_context: &ValuePointerContext) -> syn::Expr {
        let input = self.input.generate(input_context);
        let hash_fn = format_ident!("{}", self.function.to_string());

        let coerce = match self.input.expression_type(input_context) {
            TypeDef::StructDef(_, _) => unreachable!(),
            TypeDef::DataType(DataType::Utf8, _) => quote!(.as_bytes().to_vec()),
            TypeDef::DataType(DataType::Binary, _) => quote!(),
            TypeDef::DataType(_, _) => unreachable!(),
        };

        match self.input.expression_type(input_context).is_optional() {
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

    fn expression_type(&self, input_context: &ValuePointerContext) -> TypeDef {
        // this *can* be null because in SQL - MD5(NULL) = NULL
        TypeDef::DataType(
            DataType::Utf8,
            self.input.expression_type(input_context).is_optional(),
        )
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
                let Expression::Literal(LiteralExpression {
                    literal: ScalarValue::Utf8(Some(regex)),
                }) = regex_arg
                else {
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
                let Expression::Literal(LiteralExpression {
                    literal: ScalarValue::Utf8(Some(regex)),
                }) = regex_arg
                else {
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
    fn expressions(&mut self) -> Vec<&mut Expression> {
        match self {
            StringFunction::Ascii(expr)
            | StringFunction::BitLength(expr)
            | StringFunction::CharacterLength(expr)
            | StringFunction::OctetLength(expr)
            | StringFunction::Btrim(expr, None)
            | StringFunction::Lower(expr)
            | StringFunction::Upper(expr)
            | StringFunction::Chr(expr)
            | StringFunction::InitCap(expr)
            | StringFunction::Ltrim(expr, None)
            | StringFunction::Rtrim(expr, None)
            | StringFunction::Trim(expr, None)
            | StringFunction::RegexpMatch(expr, ..)
            | StringFunction::Reverse(expr) => vec![&mut *expr],

            StringFunction::StartsWith(expr1, expr2)
            | StringFunction::Left(expr1, expr2)
            | StringFunction::Repeat(expr1, expr2)
            | StringFunction::Right(expr1, expr2)
            | StringFunction::Btrim(expr1, Some(expr2))
            | StringFunction::Trim(expr1, Some(expr2))
            | StringFunction::Ltrim(expr1, Some(expr2))
            | StringFunction::Rtrim(expr1, Some(expr2))
            | StringFunction::Substr(expr1, expr2, None)
            | StringFunction::Lpad(expr1, expr2, None)
            | StringFunction::Rpad(expr1, expr2, None)
            | StringFunction::RegexpReplace(expr1, _, expr2, None)
            | StringFunction::Strpos(expr1, expr2) => {
                vec![&mut *expr1, &mut *expr2]
            }

            StringFunction::Substr(expr1, expr2, Some(expr3))
            | StringFunction::Translate(expr1, expr2, expr3)
            | StringFunction::Lpad(expr1, expr2, Some(expr3))
            | StringFunction::Rpad(expr1, expr2, Some(expr3))
            | StringFunction::Replace(expr1, expr2, expr3)
            | StringFunction::RegexpReplace(expr1, _, expr2, Some(expr3))
            | StringFunction::SplitPart(expr1, expr2, expr3) => {
                vec![&mut *expr1, &mut *expr2, &mut *expr3]
            }

            StringFunction::Concat(exprs) => exprs.iter_mut().collect(),
            StringFunction::ConcatWithSeparator(expr, exprs) => {
                let mut v = vec![&mut **expr];
                v.extend(exprs.iter_mut());
                v
            }
        }
    }

    fn expression_type(&self, input_context: &ValuePointerContext) -> TypeDef {
        match self {
            StringFunction::Ascii(expr)
            | StringFunction::BitLength(expr)
            | StringFunction::CharacterLength(expr)
            | StringFunction::OctetLength(expr)
            | StringFunction::Reverse(expr) => TypeDef::DataType(
                DataType::Int32,
                expr.expression_type(input_context).is_optional(),
            ),
            StringFunction::StartsWith(expr1, expr2) => TypeDef::DataType(
                DataType::Boolean,
                expr1.expression_type(input_context).is_optional()
                    || expr2.expression_type(input_context).is_optional(),
            ),
            StringFunction::Left(expr1, expr2)
            | StringFunction::Repeat(expr1, expr2)
            | StringFunction::Right(expr1, expr2)
            | StringFunction::Btrim(expr1, Some(expr2))
            | StringFunction::Trim(expr1, Some(expr2))
            | StringFunction::Ltrim(expr1, Some(expr2))
            | StringFunction::Rtrim(expr1, Some(expr2))
            | StringFunction::Substr(expr1, expr2, None)
            | StringFunction::Lpad(expr1, expr2, None)
            | StringFunction::Rpad(expr1, expr2, None) => TypeDef::DataType(
                DataType::Utf8,
                expr1.expression_type(input_context).is_optional()
                    || expr2.expression_type(input_context).is_optional(),
            ),
            StringFunction::Btrim(expr, None)
            | StringFunction::Lower(expr)
            | StringFunction::Upper(expr)
            | StringFunction::Chr(expr)
            | StringFunction::InitCap(expr)
            | StringFunction::Ltrim(expr, None)
            | StringFunction::Rtrim(expr, None)
            | StringFunction::Trim(expr, None) => TypeDef::DataType(
                DataType::Utf8,
                expr.expression_type(input_context).is_optional(),
            ),
            StringFunction::Substr(expr1, expr2, Some(expr3))
            | StringFunction::Translate(expr1, expr2, expr3)
            | StringFunction::Lpad(expr1, expr2, Some(expr3))
            | StringFunction::Rpad(expr1, expr2, Some(expr3))
            | StringFunction::Replace(expr1, expr2, expr3)
            | StringFunction::SplitPart(expr1, expr2, expr3) => TypeDef::DataType(
                DataType::Utf8,
                expr1.expression_type(input_context).is_optional()
                    || expr2.expression_type(input_context).is_optional()
                    || expr3.expression_type(input_context).is_optional(),
            ),
            StringFunction::Concat(_exprs) => TypeDef::DataType(DataType::Utf8, false),
            StringFunction::ConcatWithSeparator(expr, _exprs) => TypeDef::DataType(
                DataType::Utf8,
                expr.expression_type(input_context).is_optional(),
            ),
            StringFunction::RegexpReplace(expr1, _, expr3, _) => TypeDef::DataType(
                DataType::Utf8,
                expr1.expression_type(input_context).is_optional()
                    || expr3.expression_type(input_context).is_optional(),
            ),
            StringFunction::RegexpMatch(expr1, _) => TypeDef::DataType(
                DataType::List(Arc::new(Field::new("items", DataType::Utf8, true))),
                expr1.expression_type(input_context).is_optional(),
            ),
            StringFunction::Strpos(expr1, expr2) => TypeDef::DataType(
                DataType::Int32,
                expr1.expression_type(input_context).is_optional()
                    || expr2.expression_type(input_context).is_optional(),
            ),
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

    pub fn generate(&self, input_context: &ValuePointerContext) -> syn::Expr {
        let function = self.non_null_function_invocation();
        let function = if self.expression_type(input_context).is_optional() {
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
                let expr = arg.generate(input_context);
                match arg.expression_type(input_context).is_optional() {
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
                let expr1 = arg1.generate(input_context);
                let expr2 = arg2.generate(input_context);
                match (
                    arg1.expression_type(input_context).is_optional(),
                    arg2.expression_type(input_context).is_optional(),
                ) {
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
                let expr1 = arg1.generate(input_context);
                let expr2 = arg2.generate(input_context);
                let expr3 = arg3.generate(input_context);

                match (
                    arg1.expression_type(input_context).is_optional(),
                    arg2.expression_type(input_context).is_optional(),
                    arg3.expression_type(input_context).is_optional(),
                ) {
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
                        let expr = arg.generate(input_context);
                        if arg.expression_type(input_context).is_optional() {
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
                let separator_expr = arg.generate(input_context);
                let pushes: Vec<syn::Expr> = args
                    .iter()
                    .map(|arg| {
                        let expr = arg.generate(input_context);
                        if arg.expression_type(input_context).is_optional() {
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
                if arg.expression_type(input_context).is_optional() {
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
impl CodeGenerator<ValuePointerContext, TypeDef, syn::Expr> for StringFunction {
    fn generate(&self, input_context: &ValuePointerContext) -> syn::Expr {
        let function = self.non_null_function_invocation();
        let function = if self.expression_type(input_context).is_optional() {
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
                let expr = arg.generate(input_context);
                match arg.expression_type(input_context).is_optional() {
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
                let expr1 = arg1.generate(input_context);
                let expr2 = arg2.generate(input_context);
                match (
                    arg1.expression_type(input_context).is_optional(),
                    arg2.expression_type(input_context).is_optional(),
                ) {
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
                let expr1 = arg1.generate(input_context);
                let expr2 = arg2.generate(input_context);
                let expr3 = arg3.generate(input_context);

                match (
                    arg1.expression_type(input_context).is_optional(),
                    arg2.expression_type(input_context).is_optional(),
                    arg3.expression_type(input_context).is_optional(),
                ) {
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
                        let expr = arg.generate(input_context);
                        if arg.expression_type(input_context).is_optional() {
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
                let separator_expr = arg.generate(input_context);
                let pushes: Vec<syn::Expr> = args
                    .iter()
                    .map(|arg| {
                        let expr = arg.generate(input_context);
                        if arg.expression_type(input_context).is_optional() {
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
                if arg.expression_type(input_context).is_optional() {
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

    fn expression_type(&self, input_context: &ValuePointerContext) -> TypeDef {
        match self {
            StringFunction::Ascii(expr)
            | StringFunction::BitLength(expr)
            | StringFunction::CharacterLength(expr)
            | StringFunction::OctetLength(expr)
            | StringFunction::Reverse(expr) => TypeDef::DataType(
                DataType::Int32,
                expr.expression_type(input_context).is_optional(),
            ),
            StringFunction::StartsWith(expr1, expr2) => TypeDef::DataType(
                DataType::Boolean,
                expr1.expression_type(input_context).is_optional()
                    || expr2.expression_type(input_context).is_optional(),
            ),
            StringFunction::Left(expr1, expr2)
            | StringFunction::Repeat(expr1, expr2)
            | StringFunction::Right(expr1, expr2)
            | StringFunction::Btrim(expr1, Some(expr2))
            | StringFunction::Trim(expr1, Some(expr2))
            | StringFunction::Ltrim(expr1, Some(expr2))
            | StringFunction::Rtrim(expr1, Some(expr2))
            | StringFunction::Substr(expr1, expr2, None)
            | StringFunction::Lpad(expr1, expr2, None)
            | StringFunction::Rpad(expr1, expr2, None) => TypeDef::DataType(
                DataType::Utf8,
                expr1.expression_type(input_context).is_optional()
                    || expr2.expression_type(input_context).is_optional(),
            ),
            StringFunction::Btrim(expr, None)
            | StringFunction::Lower(expr)
            | StringFunction::Upper(expr)
            | StringFunction::Chr(expr)
            | StringFunction::InitCap(expr)
            | StringFunction::Ltrim(expr, None)
            | StringFunction::Rtrim(expr, None)
            | StringFunction::Trim(expr, None) => TypeDef::DataType(
                DataType::Utf8,
                expr.expression_type(input_context).is_optional(),
            ),
            StringFunction::Substr(expr1, expr2, Some(expr3))
            | StringFunction::Translate(expr1, expr2, expr3)
            | StringFunction::Lpad(expr1, expr2, Some(expr3))
            | StringFunction::Rpad(expr1, expr2, Some(expr3))
            | StringFunction::Replace(expr1, expr2, expr3)
            | StringFunction::SplitPart(expr1, expr2, expr3) => TypeDef::DataType(
                DataType::Utf8,
                expr1.expression_type(input_context).is_optional()
                    || expr2.expression_type(input_context).is_optional()
                    || expr3.expression_type(input_context).is_optional(),
            ),
            StringFunction::Concat(_exprs) => TypeDef::DataType(DataType::Utf8, false),
            StringFunction::ConcatWithSeparator(expr, _exprs) => TypeDef::DataType(
                DataType::Utf8,
                expr.expression_type(input_context).is_optional(),
            ),
            StringFunction::RegexpReplace(expr1, _, expr3, _) => TypeDef::DataType(
                DataType::Utf8,
                expr1.expression_type(input_context).is_optional()
                    || expr3.expression_type(input_context).is_optional(),
            ),
            StringFunction::RegexpMatch(expr1, _) => TypeDef::DataType(
                DataType::Utf8,
                expr1.expression_type(input_context).is_optional(),
            ),
            StringFunction::Strpos(expr1, expr2) => TypeDef::DataType(
                DataType::Int32,
                expr1.expression_type(input_context).is_optional()
                    || expr2.expression_type(input_context).is_optional(),
            ),
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
pub enum DataStructureFunction {
    Coalesce(Vec<Expression>),
    NullIf {
        left: Box<Expression>,
        right: Box<Expression>,
    },
    MakeArray(Vec<Expression>),
    ArrayIndex {
        array_expression: Box<Expression>,
        index: usize,
    },
}

impl CodeGenerator<ValuePointerContext, TypeDef, syn::Expr> for DataStructureFunction {
    fn generate(&self, input_context: &ValuePointerContext) -> syn::Expr {
        match self {
            DataStructureFunction::Coalesce(terms) => {
                let exprs: Vec<_> = terms
                    .iter()
                    .map(|term| {
                        (
                            term.generate(input_context),
                            term.expression_type(input_context).is_optional(),
                        )
                    })
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
                let left_expr = left.generate(input_context);
                let right_expr = right.generate(input_context);
                let left_nullable = left.expression_type(input_context).is_optional();
                let right_nullable = right.expression_type(input_context).is_optional();
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
                if terms
                    .iter()
                    .any(|term| term.expression_type(input_context).is_optional())
                {
                    let entries: Vec<syn::Expr> = terms
                        .iter()
                        .map(|term| {
                            let expr = term.generate(input_context);
                            if term.expression_type(input_context).is_optional() {
                                parse_quote!(#expr)
                            } else {
                                parse_quote!(Some(#expr))
                            }
                        })
                        .collect::<Vec<_>>();
                    parse_quote!(vec![#(#entries),*])
                } else {
                    let nullable = terms
                        .iter()
                        .any(|term| term.expression_type(input_context).is_optional());
                    if nullable {
                        let entries = terms
                            .iter()
                            .map(|term| {
                                let expr = term.generate(input_context);
                                if term.expression_type(input_context).is_optional() {
                                    parse_quote!(#expr)
                                } else {
                                    parse_quote!(Some(#expr))
                                }
                            })
                            .collect::<Vec<syn::Expr>>();
                        parse_quote!(vec![#(#entries),*])
                    } else {
                        let entries = terms.iter().map(|term| term.generate(input_context));
                        parse_quote!(vec![#(#entries),*])
                    }
                }
            }
            DataStructureFunction::ArrayIndex {
                array_expression,
                index,
            } => {
                let array_expr = array_expression.generate(input_context);
                let array_nullable = array_expression
                    .expression_type(input_context)
                    .is_optional();
                match array_nullable {
                    true => parse_quote!({
                        let array = #array_expr;
                        if let Some(array) = array {
                            array.get(#index - 1).cloned()
                        } else {
                            None
                        }
                    }),
                    false => parse_quote!({
                        let array = #array_expr;
                        array.get(#index - 1).cloned()
                    }),
                }
            }
        }
    }
    fn expression_type(&self, input_context: &ValuePointerContext) -> TypeDef {
        match self {
            DataStructureFunction::Coalesce(terms) => {
                let nullable = terms
                    .iter()
                    .all(|term| term.expression_type(input_context).is_optional());
                terms[0]
                    .expression_type(input_context)
                    .with_nullity(nullable)
            }
            DataStructureFunction::NullIf { left, right: _ } => {
                left.expression_type(input_context).as_nullable()
            }
            DataStructureFunction::MakeArray(terms) => {
                let TypeDef::DataType(primitive_type, _) = terms[0].expression_type(input_context)
                else {
                    unreachable!("make_array should only be called on a primitive type")
                };
                let nullable = terms
                    .iter()
                    .any(|term| term.expression_type(input_context).is_optional());
                TypeDef::DataType(
                    DataType::List(Arc::new(Field::new("items", primitive_type, nullable))),
                    false,
                )
            }
            DataStructureFunction::ArrayIndex {
                array_expression,
                index: _,
            } => {
                let TypeDef::DataType(DataType::List(field), _) =
                    array_expression.expression_type(input_context)
                else {
                    unreachable!(
                        "array_index should only be called on a list, not on {:?}, {:?}",
                        array_expression.expression_type(input_context),
                        array_expression
                    )
                };
                TypeDef::DataType(field.data_type().clone(), true)
            }
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
enum JsonFunction {
    GetFirstJsonObject,
    GetJsonObjects,
    ExtractJsonString,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
pub struct JsonExpression {
    function: JsonFunction,
    json_string: Box<Expression>,
    path: Box<Expression>,
}

impl JsonExpression {
    fn new(
        function: JsonFunction,
        json_string: Expression,
        path: Expression,
    ) -> Result<Expression> {
        if let Expression::Literal(v) = &path {
            let ScalarValue::Utf8(Some(v)) = &v.literal else {
                bail!("Literal argument to {:?} must be a string", function);
            };

            serde_json_path::JsonPath::parse(v)
                .map_err(|e| anyhow!("invalid json path: {} at position {:?}", v, e.position()))?;
        }

        Ok(Expression::Json(JsonExpression {
            function,
            json_string: Box::new(json_string),
            path: Box::new(path),
        }))
    }

    fn generate(&self, input_context: &ValuePointerContext) -> syn::Expr {
        let path_nullable = self.path.expression_type(input_context).is_optional();
        let json_nullable = self
            .json_string
            .expression_type(input_context)
            .is_optional();
        let path_expr = self.path.generate(input_context);
        let json_string_expr = self.json_string.generate(input_context);
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

    fn expression_type(&self, _input_context: &ValuePointerContext) -> TypeDef {
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

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
pub struct RustUdfExpression {
    name: String,
    args: Vec<(TypeDef, Expression)>,
    ret_type: TypeDef,
}

impl RustUdfExpression {
    pub fn name(&self) -> String {
        self.name.clone()
    }
}

impl CodeGenerator<ValuePointerContext, TypeDef, syn::Expr> for RustUdfExpression {
    fn generate(&self, input_context: &ValuePointerContext) -> syn::Expr {
        let name = format_ident!("{}", &self.name);

        let (defs, args): (Vec<_>, Vec<_>) = self
            .args
            .iter()
            .enumerate()
            .map(|(i, (def, expr))| {
                let t = expr.generate(input_context);
                let id = format_ident!("__{}", i);
                let def = match (
                    def.is_optional(),
                    expr.expression_type(input_context).is_optional(),
                ) {
                    (true, true) | (false, false) => quote!(let #id = #t),
                    (true, false) => quote!(let #id = Some(#t)),
                    (false, true) => quote!(let #id = (#t)?),
                };
                (def, quote!(#id))
            })
            .unzip();

        let mut ret = quote!(udfs::#name(#(#args, )*));

        if self.expression_type(input_context).is_optional() && !self.ret_type.is_optional() {
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

    fn expression_type(&self, input_context: &ValuePointerContext) -> TypeDef {
        self.ret_type.with_nullity(
            self.ret_type.is_optional()
                || self
                    .args
                    .iter()
                    // nullable if there are non-null UDF params with nullable arguments
                    .any(|(t, e)| {
                        !t.is_optional() && e.expression_type(input_context).is_optional()
                    }),
        )
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
pub struct RustUdafExpression {
    name: String,
    args: Vec<(TypeDef, Expression)>,
    ret_type: TypeDef,
}

impl RustUdafExpression {
    pub(crate) fn name(&self) -> String {
        self.name.clone()
    }

    pub fn expressions(&mut self) -> impl Iterator<Item = &mut Expression> {
        self.args.iter_mut().map(|(_, e)| e)
    }

    fn try_from_aggregate_udf(
        ctx: &mut ExpressionContext<'_>,
        aggregate_udf: &AggregateUDF,
    ) -> Result<Self> {
        let udf_name = &aggregate_udf.fun.name;
        let udf = ctx
            .schema_provider
            .udf_defs
            .get(udf_name)
            .ok_or_else(|| anyhow!("no UDAF with name '{}'", udf_name))?;
        let inputs: Result<Vec<Expression>> = aggregate_udf
            .args
            .iter()
            .map(|e| (ctx.compile_expr(e)))
            .collect();
        let inputs = inputs?;

        if inputs.len() != udf.args.len() {
            bail!(
                "wrong number of arguments for udf {} (found {}, expected {})",
                udf_name,
                aggregate_udf.args.len(),
                udf.args.len()
            );
        }
        if aggregate_udf.order_by.is_some() {
            bail!("Not supporting UDAF sorts right now, as datafusion doesn't");
        }

        Ok(RustUdafExpression {
            name: udf_name.to_string(),
            args: udf.args.clone().into_iter().zip(inputs).collect(),
            ret_type: udf.ret.clone(),
        })
    }
}
impl CodeGenerator<VecOfPointersContext, TypeDef, syn::Expr> for RustUdafExpression {
    fn generate(&self, input_context: &VecOfPointersContext) -> syn::Expr {
        let name = format_ident!("{}", &self.name);

        // early exit means there are terms that won't be included in the aggregate
        // if they have null values, because the UDAF expects non-null values
        let need_early_exit = self.args.iter().any(|(function_arg, incoming_expression)| {
            !function_arg.is_optional()
                && incoming_expression
                    .expression_type(&ValuePointerContext::new())
                    .is_optional()
        });

        let (arg_initialization, term_expressions): (Vec<syn::Stmt>, Vec<syn::Expr>) = self
            .args
            .iter()
            .enumerate()
            .map(|(i, (def, expr))| {
                let sub_expr = expr.generate(&ValuePointerContext::new());
                let term_expr = match (
                    def.is_optional(),
                    expr.expression_type(&ValuePointerContext::new())
                        .is_optional(),
                ) {
                    (true, true) | (false, false) => parse_quote!(#sub_expr),
                    (true, false) => parse_quote!(Some(#sub_expr)),
                    (false, true) => parse_quote!((#sub_expr)?),
                };
                let arg_ident = format_ident!("arg_vec_{}", i);
                let arg_init = parse_quote!(let mut #arg_ident = Vec::new(););
                (arg_init, term_expr)
            })
            .unzip();
        let single_value_ident = ValuePointerContext::new().variable_ident();

        let mut vec_init: Vec<syn::Stmt> = Vec::new();
        let mut vec_push: Vec<syn::Stmt> = Vec::new();
        let mut vec_names: Vec<Ident> = Vec::new();
        for i in 0..self.args.len() {
            let arg_ident = format_ident!("arg_vec_{}", i);
            vec_init.push(parse_quote!(let mut #arg_ident = Vec::new();));
            let index: syn::Index = parse_str(&i.to_string()).unwrap();
            vec_push.push(parse_quote!(#arg_ident.push(#single_value_ident.#index);));
            vec_names.push(arg_ident);
        }

        let trailing_comma: Option<TokenStream> = if self.args.len() == 1 {
            Some(quote!(,))
        } else {
            None
        };

        let tuple_expr: syn::Expr = parse_quote!((#(#term_expressions),*#trailing_comma));

        let (map_func, tuple_expr): (syn::Ident, syn::Expr) = if need_early_exit {
            (parse_quote!(filter_map), parse_quote!(Some(#tuple_expr)))
        } else {
            (parse_quote!(map), tuple_expr)
        };

        let ret: syn::Expr = parse_quote!(udfs::#name(#(#vec_names),*));

        let vec_arg = input_context.variable_ident();
        let tokens = quote!( {
            #(#arg_initialization)*
            #vec_arg.iter().#map_func(|#single_value_ident| {
               #tuple_expr
            }).for_each ( |#single_value_ident| {
                #(#vec_push)*
            }
            );
            #ret
        }
        );
        let token_string = tokens.to_string();
        parse_str(&token_string).expect(&token_string)
    }

    fn expression_type(&self, _input_context: &VecOfPointersContext) -> TypeDef {
        self.ret_type.clone()
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
pub struct WrapTypeExpression {
    name: String,
    arg: Box<Expression>,
}

impl WrapTypeExpression {
    fn new(name: &str, arg: Expression) -> Self {
        Self {
            name: name.to_string(),
            arg: Box::new(arg),
        }
    }
}

impl CodeGenerator<ValuePointerContext, TypeDef, syn::Expr> for WrapTypeExpression {
    fn generate(&self, input_context: &ValuePointerContext) -> syn::Expr {
        let path: Path = parse_str(&self.name).unwrap();
        let arg = self.arg.generate(input_context);

        if self.arg.expression_type(input_context).is_optional() {
            parse_quote!(#arg.map_over_inner(|f| #path(f)))
        } else {
            parse_quote!(#path(#arg))
        }
    }

    fn expression_type(&self, input_context: &ValuePointerContext) -> TypeDef {
        self.arg.expression_type(input_context)
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
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

    fn syn_expression_with_nullity(
        expression: &Expression,
        input_context: &ValuePointerContext,
        nullity: bool,
    ) -> syn::Expr {
        let expr = expression.generate(input_context);
        match (
            expression.expression_type(input_context).is_optional(),
            nullity,
        ) {
            (true, true) | (false, false) => expr,
            (false, true) => parse_quote!(Some(#expr)),
            (true, false) => unreachable!(
                "Should not be possible to have a nullable expression with nullity=false"
            ),
        }
    }

    fn nullable(&self, input_context: &ValuePointerContext) -> bool {
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
                        default.expression_type(input_context).is_optional()
                            || pairs.iter().any(|(_when_expr, then_expr)| {
                                then_expr.expression_type(input_context).is_optional()
                            })
                    }
                    None => true,
                }
            }
        }
    }
}

impl CodeGenerator<ValuePointerContext, TypeDef, syn::Expr> for CaseExpression {
    fn generate(&self, input_context: &ValuePointerContext) -> syn::Expr {
        let nullable = self.expression_type(input_context).is_optional();
        match self {
            CaseExpression::Match {
                value,
                matches,
                default,
            } => {
                // It's easier to have value always be option and then return default if it is None.
                // It is possible to have more efficient code when all of the expressions are
                // not nullable and the default is not nullable, but it's not worth the complexity.
                let value = Self::syn_expression_with_nullity(value, input_context, true);
                let if_clauses: Vec<syn::ExprIf> = matches
                    .iter()
                    .map(|(when_expr, then_expr)| {
                        let when_expr =
                            Self::syn_expression_with_nullity(when_expr, input_context, true);
                        let then_expr =
                            Self::syn_expression_with_nullity(then_expr, input_context, nullable);
                        parse_quote!(if #when_expr == value { #then_expr })
                    })
                    .collect();
                let default_expr = default
                    .as_ref()
                    .map(|d| Self::syn_expression_with_nullity(d, input_context, nullable))
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
                        let when_expr =
                            Self::syn_expression_with_nullity(when_expr, input_context, true);
                        let then_expr =
                            Self::syn_expression_with_nullity(then_expr, input_context, nullable);
                        parse_quote!(if #when_expr.unwrap_or(false) { #then_expr })
                    })
                    .collect();
                let default_expr = default
                    .as_ref()
                    .map(|d| Self::syn_expression_with_nullity(d, input_context, nullable))
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

    fn expression_type(&self, input_context: &ValuePointerContext) -> TypeDef {
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
                pairs[0]
                    .1
                    .expression_type(input_context)
                    .with_nullity(self.nullable(input_context))
            }
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd)]
pub enum DateTimeFunction {
    DatePart(DatePart, Box<Expression>),
    DateTrunc(DateTruncPrecision, Box<Expression>),
    FromUnixTime(Box<Expression>),
}

fn extract_literal_string(expr: Expression) -> Result<String, anyhow::Error> {
    let Expression::Literal(LiteralExpression {
        literal: ScalarValue::Utf8(Some(literal_string)),
    }) = expr
    else {
        bail!("Can only convert a literal into a string")
    };
    Ok(literal_string)
}

impl DateTimeFunction {
    fn date_part(date_part: Expression, expr: Expression) -> anyhow::Result<Expression> {
        let date_part = extract_literal_string(date_part)?
            .as_str()
            .try_into()
            .map_err(|s| anyhow!("Invalid argument for date_part: {}", s))?;
        Ok(Expression::Date(DateTimeFunction::DatePart(
            date_part,
            Box::new(expr),
        )))
    }

    fn date_trunc(date_part: Expression, expr: Expression) -> anyhow::Result<Expression> {
        let date_part = extract_literal_string(date_part)?
            .as_str()
            .try_into()
            .map_err(|s| anyhow!("Invalid argument for date_trunc: {}", s))?;
        Ok(Expression::Date(DateTimeFunction::DateTrunc(
            date_part,
            Box::new(expr),
        )))
    }
}

impl CodeGenerator<ValuePointerContext, TypeDef, syn::Expr> for DateTimeFunction {
    fn generate(&self, input_context: &ValuePointerContext) -> syn::Expr {
        match self {
            DateTimeFunction::DatePart(part, expr) => {
                let part: syn::Expr =
                    parse_str(&format!("arroyo_types::DatePart::{:?}", part)).unwrap();
                let arg = expr.generate(input_context);
                if expr.expression_type(input_context).is_optional() {
                    parse_quote!(#arg.map(|e| arroyo_worker::operators::functions::datetime::date_part(#part, e)))
                } else {
                    parse_quote!(arroyo_worker::operators::functions::datetime::date_part(#part, #arg))
                }
            }
            DateTimeFunction::DateTrunc(trunc, expr) => {
                let trunc: syn::Expr =
                    parse_str(&format!("arroyo_types::DateTruncPrecision::{:?}", trunc)).unwrap();
                let arg = expr.generate(input_context);
                if expr.expression_type(input_context).is_optional() {
                    parse_quote!(#arg.map(|e| arroyo_worker::operators::functions::datetime::date_trunc(#trunc, e)))
                } else {
                    parse_quote!(arroyo_worker::operators::functions::datetime::date_trunc(#trunc, #arg))
                }
            }
            DateTimeFunction::FromUnixTime(arg) => {
                let arg_expr = arg.generate(input_context);

                let expr = if arg.expression_type(input_context).is_optional() {
                    quote! {
                        #arg_expr.map(|e| chrono::Utc.timestamp_nanos(e as i64).to_rfc3339())
                    }
                } else {
                    quote! {
                        chrono::Utc.timestamp_nanos(#arg_expr as i64).to_rfc3339()
                    }
                };

                parse_quote! {
                    {
                        use chrono::TimeZone;
                        #expr
                    }
                }
            }
        }
    }

    fn expression_type(&self, input_context: &ValuePointerContext) -> TypeDef {
        match self {
            DateTimeFunction::DatePart(_, expr) => TypeDef::DataType(
                DataType::UInt32,
                expr.expression_type(input_context).is_optional(),
            ),
            DateTimeFunction::DateTrunc(_, expr) => TypeDef::DataType(
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                expr.expression_type(input_context).is_optional(),
            ),
            DateTimeFunction::FromUnixTime(expr) => TypeDef::DataType(
                DataType::Utf8,
                expr.expression_type(input_context).is_optional(),
            ),
        }
    }
}
