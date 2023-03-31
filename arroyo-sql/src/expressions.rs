use std::fmt::Debug;

use crate::{
    pipeline::SortDirection,
    types::{StructDef, StructField, TypeDef},
};
use anyhow::{bail, Result};
use arrow::datatypes::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::{
    aggregate_function,
    expr::Sort,
    type_coercion::aggregates::{avg_return_type, sum_return_type},
    BinaryExpr, BuiltinScalarFunction, Expr, TryCast,
};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_str, Ident};

#[derive(Debug, Clone)]
pub struct BinaryOperator(datafusion_expr::Operator);

pub trait ExpressionGenerator: Debug {
    fn to_syn_expression(&self) -> syn::Expr;
    fn return_type(&self) -> TypeDef;
    fn nullable(&self) -> bool {
        self.return_type().is_optional()
    }
}

pub fn parse_expression(expr: impl ToString) -> syn::Expr {
    let s = expr.to_string();
    parse_str(&format!("({})", s)).expect(&s)
}

#[derive(Debug)]
pub enum Expression {
    ColumnExpression(ColumnExpression),
    UnaryBooleanExpression(UnaryBooleanExpression),
    LiteralExpression(LiteralExpression),
    BinaryComparisonExpression(BinaryComparisonExpression),
    BinaryMathExpression(BinaryMathExpression),
    StructFieldExpression(StructFieldExpression),
    AggregationExpression(AggregationExpression),
    CastExpression(CastExpression),
    NumericExpression(NumericExpression),
}

impl Expression {
    pub fn to_syn_expression(&self) -> syn::Expr {
        match self {
            Expression::ColumnExpression(column_expression) => {
                column_expression.to_syn_expression()
            }
            Expression::UnaryBooleanExpression(unary_boolean_expression) => {
                unary_boolean_expression.to_syn_expression()
            }
            Expression::LiteralExpression(literal_expression) => {
                literal_expression.to_syn_expression()
            }
            Expression::BinaryComparisonExpression(comparison_expression) => {
                comparison_expression.to_syn_expression()
            }
            Expression::BinaryMathExpression(math_expression) => {
                math_expression.to_syn_expression()
            }
            Expression::StructFieldExpression(struct_field_expression) => {
                struct_field_expression.to_syn_expression()
            }
            Expression::AggregationExpression(aggregation_expression) => {
                aggregation_expression.to_syn_expression()
            }
            Expression::CastExpression(cast_expression) => cast_expression.to_syn_expression(),
            Expression::NumericExpression(numeric_expression) => {
                numeric_expression.to_syn_expression()
            }
        }
    }
    pub fn return_type(&self) -> TypeDef {
        match self {
            Expression::ColumnExpression(column_expression) => column_expression.return_type(),
            Expression::UnaryBooleanExpression(unary_boolean_expression) => {
                unary_boolean_expression.return_type()
            }
            Expression::LiteralExpression(literal_expression) => literal_expression.return_type(),
            Expression::BinaryComparisonExpression(comparison_expression) => {
                comparison_expression.return_type()
            }
            Expression::BinaryMathExpression(math_expression) => math_expression.return_type(),
            Expression::StructFieldExpression(struct_field_expression) => {
                struct_field_expression.return_type()
            }
            Expression::AggregationExpression(aggregation_expression) => {
                aggregation_expression.return_type()
            }
            Expression::CastExpression(cast_expression) => cast_expression.return_type(),
            Expression::NumericExpression(numeric_expression) => numeric_expression.return_type(),
        }
    }
    pub fn nullable(&self) -> bool {
        self.return_type().is_optional()
    }

    pub(crate) fn has_max_value(&self, field: &StructField) -> Option<i64> {
        match self {
            Expression::BinaryComparisonExpression(BinaryComparisonExpression {
                left,
                op,
                right,
            }) => {
                if let BinaryComparison::And = op {
                    match (left.has_max_value(field), right.has_max_value(field)) {
                        (None, None) => {}
                        (None, Some(max)) | (Some(max), None) => return Some(max),
                        (Some(left), Some(right)) => return Some(left.min(right)),
                    }
                }
                if let BinaryComparison::Or = op {
                    match (left.has_max_value(field), right.has_max_value(field)) {
                        (Some(left), Some(right)) => return Some(left.max(right)),
                        _ => {}
                    }
                }
                match (left.as_ref(), right.as_ref()) {
                    (
                        Expression::ColumnExpression(ColumnExpression { column_field }),
                        Expression::LiteralExpression(LiteralExpression { literal }),
                    ) => {
                        if field == column_field {
                            match (op, literal) {
                                (BinaryComparison::Lt, ScalarValue::Int64(Some(max))) => {
                                    Some(*max - 1)
                                }
                                (BinaryComparison::LtEq, ScalarValue::Int64(Some(max))) => {
                                    Some(*max)
                                }
                                (BinaryComparison::Eq, ScalarValue::Int64(Some(max))) => Some(*max),
                                _ => None,
                            }
                        } else {
                            None
                        }
                    }
                    (
                        Expression::LiteralExpression(LiteralExpression { literal }),
                        Expression::ColumnExpression(ColumnExpression { column_field }),
                    ) => {
                        if field == column_field {
                            match (op, literal) {
                                (BinaryComparison::Gt, ScalarValue::Int64(Some(max))) => {
                                    Some(*max + 1)
                                }
                                (BinaryComparison::GtEq, ScalarValue::Int64(Some(max))) => {
                                    Some(*max)
                                }
                                (BinaryComparison::Eq, ScalarValue::Int64(Some(max))) => Some(*max),
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

pub fn to_expression_generator(expression: &Expr, input_struct: &StructDef) -> Result<Expression> {
    match expression {
        Expr::Alias(expr, _alias) => to_expression_generator(expr, input_struct),
        Expr::Column(column) => Ok(Expression::ColumnExpression(ColumnExpression::from_column(
            column,
            input_struct,
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
                Box::new(to_expression_generator(left, input_struct)?),
                *op,
                Box::new(to_expression_generator(right, input_struct)?),
            )?),
            datafusion_expr::Operator::Plus
            | datafusion_expr::Operator::Minus
            | datafusion_expr::Operator::Multiply
            | datafusion_expr::Operator::Divide
            | datafusion_expr::Operator::Modulo => BinaryMathExpression::new(
                Box::new(to_expression_generator(left, input_struct)?),
                *op,
                Box::new(to_expression_generator(right, input_struct)?),
            ),
            datafusion_expr::Operator::RegexMatch
            | datafusion_expr::Operator::RegexIMatch
            | datafusion_expr::Operator::RegexNotMatch
            | datafusion_expr::Operator::RegexNotIMatch
            | datafusion_expr::Operator::BitwiseAnd
            | datafusion_expr::Operator::BitwiseOr
            | datafusion_expr::Operator::BitwiseXor
            | datafusion_expr::Operator::BitwiseShiftRight
            | datafusion_expr::Operator::BitwiseShiftLeft
            | datafusion_expr::Operator::StringConcat => bail!("{:?} is unimplemented", op),
        },
        Expr::Not(_) => bail!("NOT is unimplemented"),
        Expr::IsNotNull(expr) => Ok(UnaryBooleanExpression::new(
            UnaryOperator::IsNotNull,
            Box::new(to_expression_generator(expr, input_struct)?),
        )),
        Expr::IsNull(expr) => Ok(UnaryBooleanExpression::new(
            UnaryOperator::IsNull,
            Box::new(to_expression_generator(expr, input_struct)?),
        )),
        Expr::IsTrue(expr) => Ok(UnaryBooleanExpression::new(
            UnaryOperator::IsTrue,
            Box::new(to_expression_generator(expr, input_struct)?),
        )),
        Expr::IsFalse(expr) => Ok(UnaryBooleanExpression::new(
            UnaryOperator::IsFalse,
            Box::new(to_expression_generator(expr, input_struct)?),
        )),
        Expr::IsUnknown(expr) => Ok(UnaryBooleanExpression::new(
            UnaryOperator::IsUnknown,
            Box::new(to_expression_generator(expr, input_struct)?),
        )),
        Expr::IsNotTrue(expr) => Ok(UnaryBooleanExpression::new(
            UnaryOperator::IsNotTrue,
            Box::new(to_expression_generator(expr, input_struct)?),
        )),
        Expr::IsNotFalse(expr) => Ok(UnaryBooleanExpression::new(
            UnaryOperator::IsNotFalse,
            Box::new(to_expression_generator(expr, input_struct)?),
        )),
        Expr::IsNotUnknown(expr) => Ok(UnaryBooleanExpression::new(
            UnaryOperator::IsNotUnknown,
            Box::new(to_expression_generator(expr, input_struct)?),
        )),
        Expr::Negative(expr) => Ok(UnaryBooleanExpression::new(
            UnaryOperator::Negative,
            Box::new(to_expression_generator(expr, input_struct)?),
        )),
        Expr::GetIndexedField(datafusion_expr::GetIndexedField { expr, key }) => {
            StructFieldExpression::new(Box::new(to_expression_generator(expr, input_struct)?), key)
        }
        Expr::AggregateFunction(datafusion_expr::expr::AggregateFunction {
            fun,
            args,
            distinct,
            filter,
        }) => {
            if args.len() != 1 {
                bail!("multiple aggregation parameters is not yet supported");
            }

            if filter.is_some() {
                bail!("filters in aggregations is not yet supported");
            }

            Ok(AggregationExpression::new(
                Box::new(to_expression_generator(&args[0], input_struct)?),
                fun.clone(),
                *distinct,
            )?)
        }
        Expr::AggregateUDF { .. } => bail!("aggregate UDFs not supported"),
        Expr::Case(datafusion_expr::Case {
            expr: _,
            when_then_expr: _,
            else_expr: _,
        }) => bail!("case statements not supported yet"),
        Expr::Cast(datafusion_expr::Cast { expr, data_type }) => Ok(CastExpression::new(
            Box::new(to_expression_generator(expr, input_struct)?),
            data_type,
        )?),
        Expr::TryCast(TryCast { expr, data_type }) => {
            bail!(
                "try cast not implemented yet expr:{:?}, data_type:{:?}",
                expr,
                data_type
            )
        }
        Expr::ScalarFunction { fun, args } => {
            if args.len() != 1 {
                bail!("multiple scalar function arguments are not yet supported");
            }
            let arg_expression = Box::new(to_expression_generator(&args[0], input_struct)?);
            match fun {
                BuiltinScalarFunction::Abs
                | BuiltinScalarFunction::Acos
                | BuiltinScalarFunction::Asin
                | BuiltinScalarFunction::Atan
                | BuiltinScalarFunction::Cos
                | BuiltinScalarFunction::Ln
                | BuiltinScalarFunction::Log
                | BuiltinScalarFunction::Log10
                | BuiltinScalarFunction::Sin
                | BuiltinScalarFunction::Sqrt
                | BuiltinScalarFunction::Tan
                | BuiltinScalarFunction::Ceil
                | BuiltinScalarFunction::Floor
                | BuiltinScalarFunction::Round
                | BuiltinScalarFunction::Signum
                | BuiltinScalarFunction::Trunc
                | BuiltinScalarFunction::Log2
                | BuiltinScalarFunction::Exp => {
                    Ok(NumericExpression::new(fun.clone(), arg_expression)?)
                }
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
                | BuiltinScalarFunction::RegexpMatch
                | BuiltinScalarFunction::RegexpReplace
                | BuiltinScalarFunction::Repeat
                | BuiltinScalarFunction::Replace
                | BuiltinScalarFunction::Reverse
                | BuiltinScalarFunction::Right
                | BuiltinScalarFunction::Rpad
                | BuiltinScalarFunction::Rtrim => {
                    bail!("string function {:?} not implemented", fun)
                }
                BuiltinScalarFunction::Coalesce
                | BuiltinScalarFunction::NullIf
                | BuiltinScalarFunction::MakeArray
                | BuiltinScalarFunction::Struct
                | BuiltinScalarFunction::ArrowTypeof => {
                    bail!("data structure function {:?} not implemented", fun)
                }
                BuiltinScalarFunction::DatePart
                | BuiltinScalarFunction::DateTrunc
                | BuiltinScalarFunction::ToTimestamp
                | BuiltinScalarFunction::ToTimestampMillis
                | BuiltinScalarFunction::ToTimestampMicros
                | BuiltinScalarFunction::ToTimestampSeconds
                | BuiltinScalarFunction::DateBin
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
                | BuiltinScalarFunction::SHA512 => {
                    bail!("hashing function {:?} not implemented", fun)
                }
                BuiltinScalarFunction::ToHex => bail!("hex not implemented"),
                BuiltinScalarFunction::Uuid => bail!("UUID unimplemented"),
            }
        }
        expression => {
            bail!("expression {:?} not yet implemented", expression)
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
            relation: column.relation.clone(),
            name: column.name.clone(),
        }
    }
    pub fn convert_expr(expr: &datafusion_expr::Expr) -> Self {
        if let datafusion_expr::Expr::Column(column) = expr {
            Self::convert(column)
        } else {
            todo!()
        }
    }
}

#[derive(Debug)]
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
        let column_field = input_struct.get_field(column.relation.as_ref(), &column.name)?;
        Ok(ColumnExpression { column_field })
    }
}

impl ExpressionGenerator for ColumnExpression {
    fn to_syn_expression(&self) -> syn::Expr {
        let field_ident = self.column_field.field_ident();
        parse_expression(quote!(arg.#field_ident.clone()))
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

#[derive(Debug)]
pub struct UnaryBooleanExpression {
    operator: UnaryOperator,
    input: Box<Expression>,
}

impl ExpressionGenerator for UnaryBooleanExpression {
    fn to_syn_expression(&self) -> syn::Expr {
        let argument_expr = self.input.to_syn_expression();
        let tokens = match (self.input.return_type().is_optional(), &self.operator) {
            (true, UnaryOperator::IsNotNull) => quote!(#argument_expr.is_some()),
            (true, UnaryOperator::IsNull) => quote!(#argument_expr.is_none()),
            (true, UnaryOperator::IsTrue) => quote!(#argument_expr.unwrap_or(false)),
            (true, UnaryOperator::IsFalse) => quote!(!#argument_expr.unwrap_or(true)),
            (true, UnaryOperator::IsUnknown) => quote!(#argument_expr.is_none()),
            (true, UnaryOperator::IsNotTrue) => quote!(!#argument_expr.unwrap_or(false)),
            (true, UnaryOperator::IsNotFalse) => quote!(#argument_expr.unwrap_or(true)),
            (true, UnaryOperator::IsNotUnknown) => quote!(#argument_expr.is_some()),
            (true, UnaryOperator::Negative) => quote!(#argument_expr.map(|x| -1 * x)),
            (false, UnaryOperator::IsNotNull) => quote!(true),
            (false, UnaryOperator::IsNull) => quote!(false),
            (false, UnaryOperator::IsTrue) => quote!(#argument_expr),
            (false, UnaryOperator::IsFalse) => quote!(!#argument_expr),
            (false, UnaryOperator::IsUnknown) => quote!(false),
            (false, UnaryOperator::IsNotTrue) => quote!(!#argument_expr),
            (false, UnaryOperator::IsNotFalse) => quote!(#argument_expr),
            (false, UnaryOperator::IsNotUnknown) => quote!(true),
            (false, UnaryOperator::Negative) => quote!((-1 * #argument_expr)),
        };
        parse_expression(quote!(#tokens))
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
}

impl UnaryBooleanExpression {
    fn new(operator: UnaryOperator, input: Box<Expression>) -> Expression {
        Expression::UnaryBooleanExpression(UnaryBooleanExpression { operator, input })
    }
}

#[derive(Debug)]
pub struct LiteralExpression {
    literal: ScalarValue,
}

impl ExpressionGenerator for LiteralExpression {
    fn to_syn_expression(&self) -> syn::Expr {
        TypeDef::get_literal(&self.literal)
    }

    fn return_type(&self) -> TypeDef {
        TypeDef::DataType(self.literal.get_datatype(), self.literal.is_null())
    }
}

impl LiteralExpression {
    fn new(literal: ScalarValue) -> Expression {
        Expression::LiteralExpression(Self { literal })
    }
}

#[derive(Debug)]
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

#[derive(Debug)]
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
        Ok(Expression::BinaryComparisonExpression(Self {
            left,
            op,
            right,
        }))
    }
}

impl ExpressionGenerator for BinaryComparisonExpression {
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
            BinaryComparison::IsNotDistinctFrom => {
                return parse_str(&quote!((#left_expr == #right_expr)).to_string()).unwrap()
            }
            BinaryComparison::IsDistinctFrom => {
                return parse_str(&quote!((#left_expr != #right_expr)).to_string()).unwrap()
            }
            BinaryComparison::And => quote!(&&),
            BinaryComparison::Or => quote!(||),
        };
        let expr = match (
            self.left.return_type().is_optional(),
            self.right.return_type().is_optional(),
        ) {
            (true, true) => quote!({
                let left = #left_expr;
                let right = #right_expr;
                match (left, right) {
                    (Some(left), Some(right)) => Some(left #op right),
                    _ => None
                }
            }),
            (true, false) => {
                quote!(#left_expr.map(|left| left #op #right_expr))
            }
            (false, true) => {
                quote!(#right_expr.map(|right| #left_expr #op right))
            }
            (false, false) => quote!(#left_expr #op #right_expr),
        };
        parse_expression(quote!(#expr))
    }

    fn return_type(&self) -> TypeDef {
        TypeDef::DataType(
            DataType::Boolean,
            self.left.return_type().is_optional() || self.right.return_type().is_optional(),
        )
    }
}

#[derive(Debug)]
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

#[derive(Debug)]
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
        Ok(Expression::BinaryMathExpression(Self { left, op, right }))
    }
}

impl ExpressionGenerator for BinaryMathExpression {
    fn to_syn_expression(&self) -> syn::Expr {
        let left_expr = self.left.to_syn_expression();
        let right_expr = self.right.to_syn_expression();
        let op = self.op.as_tokens();
        let expr = match (self.left.nullable(), self.right.nullable()) {
            (true, true) => quote!({
                let left = #left_expr;
                let right = #right_expr;
                match (left, right) {
                    (Some(left), Some(right)) => Some(left #op right),
                    _ => None
                }
            }),
            (true, false) => {
                quote!(#left_expr.map(|left| left #op #right_expr))
            }
            (false, true) => {
                quote!(#right_expr.map(|right| #left_expr #op right))
            }
            (false, false) => quote!(#left_expr #op #right_expr),
        };
        parse_expression(quote!(#expr))
    }

    fn return_type(&self) -> TypeDef {
        let nullable = self.left.nullable() || self.right.nullable();
        self.left.return_type().with_nullity(nullable)
    }
}

#[derive(Debug)]
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
                    Ok(Expression::StructFieldExpression(Self {
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

impl ExpressionGenerator for StructFieldExpression {
    fn to_syn_expression(&self) -> syn::Expr {
        let struct_expression = self.struct_expression.to_syn_expression();
        let field_ident = self.struct_field.field_ident();
        let tokens = match (
            self.struct_expression.nullable(),
            self.struct_field.nullable(),
        ) {
            (true, true) => {
                quote!(#struct_expression.map(|arg| arg.#field_ident.clone()).flatten())
            }
            (true, false) => quote!(#struct_expression.map(|arg| arg.#field_ident.clone())),
            (false, true) => quote!(#struct_expression.#field_ident.clone()),
            (false, false) => quote!(#struct_expression.#field_ident.clone()),
        };
        parse_expression(quote!(#tokens))
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

#[derive(Debug)]
pub struct AggregationExpression {
    producing_expression: Box<Expression>,
    aggregator: Aggregator,
}

impl AggregationExpression {
    fn new(
        producing_expression: Box<Expression>,
        aggregator: aggregate_function::AggregateFunction,
        distinct: bool,
    ) -> Result<Expression> {
        let aggregator = Aggregator::from_datafusion(aggregator, distinct)?;
        Ok(Expression::AggregationExpression(Self {
            producing_expression,
            aggregator,
        }))
    }
}

impl ExpressionGenerator for AggregationExpression {
    fn to_syn_expression(&self) -> syn::Expr {
        let sub_expr = self.producing_expression.to_syn_expression();
        let (map_type, unwrap) = if self.producing_expression.nullable() {
            (format_ident!("filter_map"), None)
        } else {
            (format_ident!("map"), Some(quote!(.unwrap())))
        };
        let tokens = match self.aggregator {
            Aggregator::Count => {
                if self.producing_expression.nullable() {
                    quote!(
                        arg.iter()
                            .filter_map(|arg| #sub_expr)
                            .count() as i64
                    )
                } else {
                    quote!(arg.len() as i64)
                }
            }
            Aggregator::Sum => quote!(
                arg.iter()
                    .#map_type(|arg| #sub_expr)
                    .reduce(|left, right| left + right)
                    #unwrap
            ),
            Aggregator::Min => quote!(
                arg.iter()
                    .#map_type(|arg| #sub_expr)
                    .reduce( |left, right| left.min(right))
                    #unwrap
            ),
            Aggregator::Max => quote!(
                arg.iter()
                    .map(|arg| #sub_expr)
                    .reduce(|left, right| left.max(right))
                    .unwrap()
            ),
            Aggregator::Avg => quote!(
                arg.iter()
                    .#map_type(|arg| #sub_expr)
                    .map(|val| (1, val))
                    .reduce(|left, right| (left.0 + right.0, left.1+right.1))
                    .map(|result| (result.1 as f64)/(result.0 as f64))
                    #unwrap
            ),
            Aggregator::CountDistinct => quote! {
                arg.iter()
                    .#map_type(|arg| #sub_expr)
                    .collect::<std::collections::HashSet<_>>()
                    .len() as i64
            },
        };
        parse_expression(quote!(#tokens))
    }

    fn return_type(&self) -> TypeDef {
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

#[derive(Debug)]
pub struct CastExpression {
    input: Box<Expression>,
    data_type: DataType,
}

impl CastExpression {
    fn new(input: Box<Expression>, data_type: &DataType) -> Result<Expression> {
        if let TypeDef::DataType(input_type, _) = input.return_type() {
            if Self::is_numeric(&input_type) && Self::is_numeric(data_type) {
                return Ok(Expression::CastExpression(Self {
                    input,
                    data_type: data_type.clone(),
                }));
            } else {
                bail!("casting from {:?} to {:?} is currently unsupported, only numeric types are supported", input_type, data_type);
            }
        } else {
            bail!("casting structs is currently unsupported")
        }
    }

    fn is_numeric(data_type: &DataType) -> bool {
        match data_type {
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
            | DataType::Float64 => true,
            _ => false,
        }
    }
}

impl ExpressionGenerator for CastExpression {
    fn to_syn_expression(&self) -> syn::Expr {
        let sub_expr = self.input.to_syn_expression();
        let cast_type = TypeDef::DataType(self.data_type.clone(), false).return_type();
        let tokens = if self.nullable() {
            quote!(#sub_expr.map(|x| x as #cast_type) )
        } else {
            quote!(#sub_expr as #cast_type)
        };
        parse_expression(quote!(#tokens))
    }

    fn return_type(&self) -> TypeDef {
        TypeDef::DataType(self.data_type.clone(), self.input.nullable())
    }
}

#[derive(Debug, Clone)]
enum NumericFunction {
    Abs,
    Acos,
    Asin,
    Atan,
    Cos,
    Ln,
    Log,
    Log10,
    Sin,
    Sqrt,
    Tan,
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
            NumericFunction::Asin => "asin",
            NumericFunction::Atan => "atan",
            NumericFunction::Cos => "cos",
            NumericFunction::Ln => "ln",
            NumericFunction::Log => "log",
            NumericFunction::Log10 => "log10",
            NumericFunction::Sin => "sin",
            NumericFunction::Sqrt => "sqrt",
            NumericFunction::Tan => "tan",
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
            BuiltinScalarFunction::Asin => Ok(Self::Asin),
            BuiltinScalarFunction::Atan => Ok(Self::Atan),
            BuiltinScalarFunction::Cos => Ok(Self::Cos),
            BuiltinScalarFunction::Ln => Ok(Self::Ln),
            BuiltinScalarFunction::Log => Ok(Self::Log),
            BuiltinScalarFunction::Log10 => Ok(Self::Log10),
            BuiltinScalarFunction::Sin => Ok(Self::Sin),
            BuiltinScalarFunction::Sqrt => Ok(Self::Sqrt),
            BuiltinScalarFunction::Tan => Ok(Self::Tan),
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

#[derive(Debug)]
pub struct NumericExpression {
    function: NumericFunction,
    input: Box<Expression>,
}

impl NumericExpression {
    fn new(function: BuiltinScalarFunction, input: Box<Expression>) -> Result<Expression> {
        let function = function.try_into()?;
        Ok(Expression::NumericExpression(NumericExpression {
            function,
            input,
        }))
    }
}

impl ExpressionGenerator for NumericExpression {
    fn to_syn_expression(&self) -> syn::Expr {
        let function_name = self.function.function_name();
        let argument_expression = self.input.to_syn_expression();
        let tokens = if self.input.return_type().is_optional() {
            quote!(#argument_expression.map(|val| (val as f64).#function_name()))
        } else {
            quote!((#argument_expression as f64).#function_name())
        };
        parse_str(&quote!(#tokens).to_string()).unwrap()
    }

    fn return_type(&self) -> TypeDef {
        TypeDef::DataType(DataType::Int64, self.input.return_type().is_optional())
    }
}

#[derive(Debug)]
pub struct SortExpression {
    value: Expression,
    direction: SortDirection,
    nulls_first: bool,
}

impl SortExpression {
    pub fn from_expression(sort: &Sort, input_struct: &StructDef) -> Result<Self> {
        let value = to_expression_generator(&sort.expr, input_struct)?;
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
    pub fn tuple_type(&self) -> syn::Type {
        let value_type = self.value.return_type().return_type();
        let tokens = match (self.value.nullable(), &self.direction, self.nulls_first) {
            (false, SortDirection::Asc, _) | (true, SortDirection::Asc, true) => {
                quote!(#value_type)
            }
            (false, SortDirection::Desc, _) | (true, SortDirection::Desc, true) => {
                quote!(std::cmp::Reverse<#value_type>)
            }
            (true, SortDirection::Asc, false) => quote!((bool, #value_type)),
            (true, SortDirection::Desc, false) => quote!(std::cmp::Reverse<(bool, #value_type)>),
        };
        parse_str(&tokens.to_string()).unwrap()
    }
    pub fn to_syn_expr(&self) -> syn::Expr {
        let value_expr = self.value.to_syn_expression();
        let tokens = match (self.value.nullable(), &self.direction, self.nulls_first) {
            (false, SortDirection::Asc, _) | (true, SortDirection::Asc, true) => {
                quote!(#value_expr)
            }
            (false, SortDirection::Desc, _) | (true, SortDirection::Desc, true) => {
                quote!(std::cmp::Reverse(#value_expr))
            }
            (true, SortDirection::Asc, false) => quote!({
                    let option = #value_expr;
                    (option.is_none(), option)
            }),
            (true, SortDirection::Desc, false) => quote!({
                let option = #value_expr;
                std::cmp::Reverse((option.is_none(), option))
            }),
        };
        parse_expression(tokens)
    }
}
