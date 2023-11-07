use crate::{
    code_gen::{
        BinAggregatingContext, BinType, CodeGenerator, CombiningContext, MemoryAddingContext,
        MemoryAggregatingContext, MemoryRemovingContext, ValueBinMergingContext,
        ValuePointerContext, VecAggregationContext,
    },
    expressions::{
        AggregateComputation, AggregateResultExtraction, Aggregator, Column, Expression,
    },
    types::{data_type_as_syn_type, StructDef, StructField, TypeDef},
};
use anyhow::{bail, Result};
use arrow_schema::DataType;
use arroyo_rpc::formats::Format;
use datafusion_expr::type_coercion::aggregates::{avg_return_type, sum_return_type};
use proc_macro2::TokenStream;
use quote::quote;
use syn::{parse_quote, parse_str};

#[derive(Debug, Clone)]
pub struct Projection {
    pub fields: Vec<(Column, Expression)>,
    pub format: Option<Format>,
}

impl Projection {
    pub fn new(fields: Vec<(Column, Expression)>) -> Self {
        Self {
            fields,
            format: None,
        }
    }

    pub fn output_struct(&self) -> StructDef {
        self.expression_type(&ValuePointerContext::new())
    }

    pub fn expressions(&mut self) -> impl Iterator<Item = &mut Expression> {
        self.fields.iter_mut().map(|(_, expression)| expression)
    }
}

impl CodeGenerator<ValuePointerContext, StructDef, syn::Expr> for Projection {
    fn generate(&self, input_context: &ValuePointerContext) -> syn::Expr {
        let assignments: Vec<_> = self
            .fields
            .iter()
            .map(|(col, computation)| {
                let data_type = computation.expression_type(input_context);
                let field_ident =
                    StructField::new(col.name.clone(), col.relation.clone(), data_type)
                        .field_ident();
                let expr = computation.generate(input_context);
                quote!(#field_ident : #expr)
            })
            .collect();
        let output_type = self.expression_type(input_context).get_type();
        parse_quote!(
            #output_type {
                #(#assignments)
                ,*
            }
        )
    }

    fn expression_type(&self, input_context: &ValuePointerContext) -> StructDef {
        let fields = self
            .fields
            .iter()
            .map(|(col, computation)| {
                let field_type = computation.expression_type(&input_context);
                StructField::new(col.name.clone(), col.relation.clone(), field_type)
            })
            .collect();
        StructDef::new(None, true, fields, self.format.clone())
    }
}

#[derive(Debug, Clone)]
pub enum UnnestFieldType {
    Default,
    UnnestOuter,
}

#[derive(Debug, Clone)]
pub struct UnnestProjection {
    pub fields: Vec<(Column, Expression, UnnestFieldType)>,
    pub unnest_inner: Expression,
    pub format: Option<Format>,
}

impl UnnestProjection {
    pub fn output_struct(&self) -> StructDef {
        self.expression_type(&ValuePointerContext::new())
    }
}

impl CodeGenerator<ValuePointerContext, StructDef, syn::Expr> for UnnestProjection {
    fn generate(&self, input_context: &ValuePointerContext) -> syn::Expr {
        let array_creating_expr = self.unnest_inner.generate(input_context);

        let handle_optional = if self
            .unnest_inner
            .expression_type(input_context)
            .is_optional()
        {
            quote! { .flatten() }
        } else {
            quote!()
        };

        let unnest_context = ValuePointerContext::with_arg("___unnest");

        let assignments: Vec<_> = self
            .fields
            .iter()
            .map(|(col, expr, typ)| {
                let name = &col.name;
                let alias = &col.relation;
                let ctx = match typ {
                    UnnestFieldType::Default => input_context,
                    UnnestFieldType::UnnestOuter => &unnest_context,
                };

                let data_type = expr.expression_type(ctx);

                let field_ident =
                    StructField::new(name.clone(), alias.clone(), data_type).field_ident();
                let expr = expr.generate(ctx);
                quote!(#field_ident : #expr)
            })
            .collect();
        let output_type = self.expression_type(input_context).get_type();

        parse_quote!(
            #array_creating_expr.into_iter()
                #handle_optional
                .map(|___unnest| {
                    #output_type {
                        #(#assignments),*
                    }
                })
        )
    }

    fn expression_type(&self, input_context: &ValuePointerContext) -> StructDef {
        let fields: Vec<_> = self
            .fields
            .iter()
            .map(|(col, computation, _)| {
                let field_type = computation.expression_type(&input_context);
                StructField::new(col.name.clone(), col.relation.clone(), field_type)
            })
            .collect();

        StructDef::new(None, true, fields, self.format.clone())
    }
}

#[derive(Debug, Clone)]
pub struct AggregateProjection {
    pub aggregates: Vec<AggregateComputation>,
    pub group_bys: Vec<(StructField, AggregateResultExtraction)>,
}

impl AggregateProjection {
    pub(crate) fn supports_two_phase(&self) -> bool {
        self.aggregates
            .iter()
            .all(|computation| computation.allows_two_phase())
    }
}

impl CodeGenerator<VecAggregationContext, StructDef, syn::Expr> for AggregateProjection {
    fn generate(&self, input_context: &VecAggregationContext) -> syn::Expr {
        let mut assignments: Vec<_> = self
            .aggregates
            .iter()
            .map(|computation| {
                let data_type = computation.expression_type(&input_context.values_context);
                let expr = computation.generate(&input_context.values_context);
                let column = computation.column();
                let field_ident =
                    StructField::new(column.name, column.relation, data_type).field_ident();
                parse_quote!(#field_ident: #expr)
            })
            .collect();

        assignments.extend(self.group_bys.iter().map(|(column, expr)| {
            expr.get_column_assignment(
                column,
                input_context.window_ident(),
                input_context.key_ident(),
            )
        }));

        let output_type = self.expression_type(input_context).get_type();
        parse_quote!(
            {
                #output_type {
                    #(#assignments),*
                }
            }
        )
    }

    fn expression_type(&self, input_context: &VecAggregationContext) -> StructDef {
        let mut fields: Vec<StructField> = self
            .aggregates
            .iter()
            .map(|computation| {
                let field_type = computation.expression_type(&input_context.values_context);
                let column = computation.column();
                StructField::new(column.name, column.relation, field_type)
            })
            .collect();
        fields.extend(
            self.group_bys
                .iter()
                .map(|(struct_field, _expr)| struct_field.clone()),
        );
        StructDef::new(None, true, fields, None)
    }
}

#[derive(Debug, Clone)]
pub struct TwoPhaseAggregateProjection {
    pub aggregates: Vec<(Column, TwoPhaseAggregation)>,
    pub group_bys: Vec<(StructField, AggregateResultExtraction)>,
}

impl TwoPhaseAggregateProjection {
    pub fn expressions(&mut self) -> impl Iterator<Item = &mut Expression> {
        self.aggregates
            .iter_mut()
            .map(|(_, computation)| &mut computation.incoming_expression)
    }
}

impl TryFrom<AggregateProjection> for TwoPhaseAggregateProjection {
    type Error = anyhow::Error;

    fn try_from(aggregate_projection: AggregateProjection) -> Result<Self> {
        let aggregates = aggregate_projection
            .aggregates
            .into_iter()
            .map(|computation| match computation {
                AggregateComputation::Builtin {
                    column,
                    computation,
                } => Ok((column, computation.try_into()?)),
                AggregateComputation::UDAF { .. } => {
                    bail!("UDAFs not supported in two phase aggregation")
                }
            })
            .collect::<Result<Vec<(Column, TwoPhaseAggregation)>>>()?;

        Ok(Self {
            aggregates,
            group_bys: aggregate_projection.group_bys,
        })
    }
}

impl CodeGenerator<ValueBinMergingContext, BinType, syn::Expr> for TwoPhaseAggregateProjection {
    fn generate(&self, input_context: &ValueBinMergingContext) -> syn::Expr {
        let (some_assignments, none_assignments): (Vec<syn::Expr>, Vec<syn::Expr>) = self
            .aggregates
            .iter()
            .enumerate()
            .map(|(i, (_, field_computation))| {
                let term_expr = field_computation.generate(input_context);
                let bin_syn_type = field_computation.expression_type(input_context).syn_type();
                let current_bin_ident = input_context.bin_context.current_bin_ident();
                let i: syn::Index = parse_str(&i.to_string()).unwrap();
                (
                    parse_quote!({let #current_bin_ident = Some(#current_bin_ident.#i.clone()); #term_expr}),
                    parse_quote!({let #current_bin_ident:Option<#bin_syn_type> = None; #term_expr}),
                )
            })
            .unzip();

        let trailing_comma = self.trailing_comma();
        parse_quote!(
            match current_bin {
                Some(current_bin) => {
                    (#(#some_assignments),*#trailing_comma)},
                None => {
                    (#(#none_assignments),*#trailing_comma)}
            }
        )
    }

    fn expression_type(&self, input_context: &ValueBinMergingContext) -> BinType {
        self.bin_type(&input_context.value_context)
    }
}

impl CodeGenerator<CombiningContext, BinType, syn::Expr> for TwoPhaseAggregateProjection {
    fn generate(&self, input_context: &CombiningContext) -> syn::Expr {
        let current_bin_ident = input_context.current_bin_ident();
        let new_bin_ident = input_context.new_bin_ident();
        let some_assignments: Vec<syn::Expr> = self
            .aggregates
            .iter()
            .enumerate()
            .map(|(i, (_, field_computation))| {
                let expr = field_computation.generate(input_context);
                let i: syn::Index = parse_str(&i.to_string()).unwrap();
                parse_quote!({let #current_bin_ident = #current_bin_ident.#i.clone();
                    let #new_bin_ident = #new_bin_ident.#i.clone();  #expr})
            })
            .collect();
        let trailing_comma = self.trailing_comma();
        parse_quote!(
            match #current_bin_ident {
                Some(#current_bin_ident) => {
                    (#(#some_assignments),*#trailing_comma)
                }
                None => #new_bin_ident.clone()
            }
        )
    }

    fn expression_type(&self, input_context: &CombiningContext) -> BinType {
        self.bin_type(&input_context.value_context)
    }
}

impl CodeGenerator<MemoryAddingContext, BinType, syn::Expr> for TwoPhaseAggregateProjection {
    fn generate(&self, input_context: &MemoryAddingContext) -> syn::Expr {
        let trailing_comma = self.trailing_comma();
        let bin_ident = input_context.bin_value_ident();
        let memory_ident = input_context.memory_value_ident();
        let (some_assignments, none_assignments): (Vec<syn::Expr>, Vec<syn::Expr>) = self
            .aggregates
            .iter()
            .enumerate()
            .map(|(i, (_, field_computation))| {
                let expr = field_computation.generate(input_context);
                let i: syn::Index = parse_str(&i.to_string()).unwrap();
                (
                    parse_quote!({let #memory_ident = Some(#memory_ident.#i);
                    let #bin_ident = #bin_ident.#i;
                     #expr}),
                    parse_quote!({let #memory_ident = None;
                        let #bin_ident = #bin_ident.#i;
                         #expr}),
                )
            })
            .unzip();
        parse_quote!(
            match current {
                Some((i, current)) => {
                    (i +1, (#(#some_assignments),*#trailing_comma))},
                None => {
                    (1, (#(#none_assignments),*#trailing_comma))}
            }
        )
    }

    fn expression_type(&self, input_context: &MemoryAddingContext) -> BinType {
        self.memory_type(&input_context.value_context)
    }
}

impl CodeGenerator<MemoryRemovingContext, BinType, syn::Expr> for TwoPhaseAggregateProjection {
    fn generate(&self, input_context: &MemoryRemovingContext) -> syn::Expr {
        let removals: Vec<syn::Expr> = self
            .aggregates
            .iter()
            .enumerate()
            .map(|(i, (_, field_computation))| {
                let expr = field_computation.generate(input_context);
                let i: syn::Index = parse_str(&i.to_string()).unwrap();
                parse_quote!({let current = current.1.#i;
                    let bin_value = bin_value.#i;
                     #expr.unwrap()})
            })
            .collect();

        let trailing_comma = self.trailing_comma();
        parse_quote!(
            if current.0 == 1 {
                None
            } else {
                Some((current.0 - 1, (#(#removals),*#trailing_comma)))
            }
        )
    }

    fn expression_type(&self, input_context: &MemoryRemovingContext) -> BinType {
        BinType::Option(Box::new(self.memory_type(&input_context.value_context)))
    }
}

impl CodeGenerator<BinAggregatingContext, StructDef, syn::Expr> for TwoPhaseAggregateProjection {
    fn generate(&self, input_context: &BinAggregatingContext) -> syn::Expr {
        let mut assignments: Vec<_> = self
            .aggregates
            .iter()
            .enumerate()
            .map(|(i, (field_name, field_computation))| {
                let name = field_name.name.clone();
                let alias = field_name.relation.clone();
                let data_type = field_computation.expression_type(input_context);
                let expr = field_computation.generate(input_context);
                // TODO: not have to jump into StructField space.
                let field_ident = StructField::new(name, alias, data_type).field_ident();
                let i: syn::Index = parse_str(&i.to_string()).unwrap();
                let bin_name = input_context.bin_name();
                parse_quote!(#field_ident: {let #bin_name = &#bin_name.#i; #expr})
            })
            .collect();

        assignments.extend(
            self.group_bys
                .iter()
                .map(|(column, aggregate_result_extraction)| {
                    aggregate_result_extraction.get_column_assignment(
                        column,
                        input_context.window_ident(),
                        input_context.key_ident(),
                    )
                }),
        );
        let output_type = self.expression_type(input_context).get_type();
        parse_quote!(
            {
                #output_type {
                    #(#assignments),*
                }
            }
        )
    }

    fn expression_type(&self, _input_context: &BinAggregatingContext) -> StructDef {
        self.output_struct()
    }
}

impl CodeGenerator<MemoryAggregatingContext, StructDef, syn::Expr> for TwoPhaseAggregateProjection {
    fn generate(&self, input_context: &MemoryAggregatingContext) -> syn::Expr {
        let mut assignments: Vec<_> = self
            .aggregates
            .iter()
            .enumerate()
            .map(|(i, (field_name, field_computation))| {
                let name = field_name.name.clone();
                let alias = field_name.relation.clone();
                let data_type = field_computation.expression_type(input_context);
                let expr = field_computation.generate(input_context);
                let field_ident = StructField::new(name, alias, data_type).field_ident();
                let bin_ident = input_context.bin_name();
                let i: syn::Index = parse_str(&i.to_string()).unwrap();
                parse_quote!(#field_ident: {let #bin_ident = &#bin_ident.1.#i; #expr})
            })
            .collect();

        assignments.extend(
            self.group_bys
                .iter()
                .map(|(column, aggregate_result_extraction)| {
                    aggregate_result_extraction.get_column_assignment(
                        column,
                        input_context.window_ident(),
                        input_context.key_ident(),
                    )
                }),
        );
        let output_type = self.expression_type(input_context).get_type();
        parse_quote!(
            {
                #output_type {
                    #(#assignments),*
                }
            }
        )
    }

    fn expression_type(&self, _input_context: &MemoryAggregatingContext) -> StructDef {
        self.output_struct()
    }
}

impl TwoPhaseAggregateProjection {
    pub fn output_struct(&self) -> StructDef {
        let mut fields: Vec<StructField> = self
            .aggregates
            .iter()
            .map(|(column, computation)| {
                let field_type = computation.output_type_def(&ValuePointerContext::new());
                StructField::new(column.name.clone(), column.relation.clone(), field_type)
            })
            .collect();
        fields.extend(
            self.group_bys
                .iter()
                .map(|(struct_field, _aggregate_result_extraction)| struct_field.clone()),
        );
        StructDef::for_fields(fields)
    }

    fn trailing_comma(&self) -> Option<TokenStream> {
        if self.aggregates.len() == 1 {
            Some(quote!(,))
        } else {
            None
        }
    }

    pub(crate) fn bin_type(&self, input_context: &ValuePointerContext) -> BinType {
        let bin_types: Vec<_> = self
            .aggregates
            .iter()
            .map(|(_, computation)| computation.bin_type(input_context))
            .collect();
        BinType::Tuple(bin_types)
    }

    pub(crate) fn memory_type(&self, input_context: &ValuePointerContext) -> BinType {
        let mem_types: Vec<_> = self
            .aggregates
            .iter()
            .map(|(_, computation)| computation.mem_type(input_context))
            .collect();
        BinType::Tuple(vec![BinType::Usize, BinType::Tuple(mem_types)])
    }
}

#[derive(Debug, Clone)]
pub struct TwoPhaseAggregation {
    pub incoming_expression: Expression,
    pub aggregator: Aggregator,
}

impl CodeGenerator<ValueBinMergingContext, BinType, syn::Expr> for TwoPhaseAggregation {
    fn generate(&self, input_context: &ValueBinMergingContext) -> syn::Expr {
        let expr = self
            .incoming_expression
            .generate(&input_context.value_context);
        let current_bin_ident = input_context.bin_context.current_bin_ident();
        // TODO: factor this out.
        let aggregate_type = data_type_as_syn_type(
            self.intermediate_type_def(&input_context.value_context)
                .as_datatype()
                .expect("aggregates shouldn't return structs"),
        );
        let input_nullable = self
            .incoming_expression
            .expression_type(&input_context.value_context)
            .is_optional();
        match (&self.aggregator, input_nullable) {
            (Aggregator::Count, true) => parse_quote!({
                let  count = #current_bin_ident.unwrap_or(0);
                let addition = if #expr.is_some() {1} else {0};
                count + addition
            }),
            (Aggregator::Count, false) => parse_quote!({ #current_bin_ident.unwrap_or(0) + 1 }),
            (Aggregator::Sum, true) => parse_quote!({
                match (#current_bin_ident.flatten(), #expr) {
                    (Some(value), Some(addition)) => Some(value + (addition as #aggregate_type)),
                    (Some(value), None) => Some(value),
                    (None, Some(addition)) => Some(addition as #aggregate_type),
                    (None, None) => None,
                }
            }),
            (Aggregator::Sum, false) => parse_quote!({
                match #current_bin_ident {
                    Some(value) => value + (#expr as #aggregate_type),
                    None => (#expr as #aggregate_type),
                }
            }),
            (Aggregator::Min, true) => parse_quote!({
                match (#current_bin_ident.flatten(), #expr) {
                    (Some(value), Some(new_value)) => Some(value.min(new_value)),
                    (Some(value), None) => Some(value),
                    (None, Some(new_value)) => Some(new_value),
                    (None, None) => None,
                }
            }),
            (Aggregator::Min, false) => parse_quote!({
                match #current_bin_ident {
                    Some(value) => value.min(#expr),
                    None => #expr
                }
            }),
            (Aggregator::Max, true) => parse_quote!({
                match (#current_bin_ident.flatten(), #expr) {
                    (Some(value), Some(new_value)) => Some(value.max(new_value)),
                    (Some(value), None) => Some(value),
                    (None, Some(new_value)) => Some(new_value),
                    (None, None) => None,
                }
            }),
            (Aggregator::Max, false) => parse_quote!({
                match #current_bin_ident {
                    Some(value) => value.max(#expr),
                    None => #expr
                }
            }),
            (Aggregator::Avg, true) => parse_quote!({
                match (#current_bin_ident.flatten(), #expr) {
                    (Some((count, sum)), Some(value)) => Some((count + 1, sum + (value as #aggregate_type))),
                    (Some((count, sum)), None) => Some((count, sum)),
                    (None, Some(value)) => Some((1, value as #aggregate_type)),
                    (None, None) => None,
                }
            }),
            (Aggregator::Avg, false) => parse_quote!({
                match #current_bin_ident {
                    Some((count, sum)) => (count + 1, sum + (#expr as #aggregate_type)),
                    None => (1, #expr as #aggregate_type)
                }
            }),
            (Aggregator::CountDistinct, _) => unreachable!("no two phase for count distinct"),
        }
    }

    fn expression_type(&self, input_context: &ValueBinMergingContext) -> BinType {
        self.bin_type(&input_context.value_context)
    }
}

impl CodeGenerator<CombiningContext, BinType, syn::Expr> for TwoPhaseAggregation {
    fn generate(&self, input_context: &CombiningContext) -> syn::Expr {
        let input_nullable = self
            .incoming_expression
            .expression_type(&input_context.value_context)
            .is_optional();
        let current_bin_ident = input_context.current_bin_ident();
        let new_bin_ident = input_context.new_bin_ident();
        match (&self.aggregator, input_nullable) {
            (Aggregator::Count, _) => parse_quote!({ #current_bin_ident + #new_bin_ident }),
            (Aggregator::Sum, true) => parse_quote!({
                match (#current_bin_ident, #new_bin_ident) {
                    (Some(value), Some(addition)) => Some(value + addition),
                    (Some(value), None) => Some(value),
                    (None, Some(addition)) => Some(addition),
                    (None, None) => None,
                }
            }),
            (Aggregator::Sum, false) => parse_quote!({ #current_bin_ident + #new_bin_ident }),
            (Aggregator::Min, true) => parse_quote!({
                match (#current_bin_ident, #new_bin_ident) {
                    (Some(value), Some(new_value)) => Some(value.min(new_value)),
                    (Some(value), None) => Some(value),
                    (None, Some(new_value)) => Some(new_value),
                    (None, None) => None,
                }
            }),
            (Aggregator::Min, false) => parse_quote!({ #current_bin_ident.min(#new_bin_ident) }),
            (Aggregator::Max, true) => parse_quote!({
                match (#current_bin_ident, #new_bin_ident) {
                    (Some(value), Some(new_value)) => Some(value.max(new_value)),
                    (Some(value), None) => Some(value),
                    (None, Some(new_value)) => Some(new_value),
                    (None, None) => None,
                }
            }),
            (Aggregator::Max, false) => parse_quote!({ #current_bin_ident.max(#new_bin_ident) }),
            (Aggregator::Avg, true) => parse_quote!({
                match (#current_bin_ident, #new_bin_ident) {
                    (Some((current_count, current_sum)), Some((new_count, new_sum))) => {
                        Some((current_count + new_count, current_sum + new_sum))
                    }
                    (Some((count, sum)), None) => Some((count, sum)),
                    (None, Some((count, sum))) => Some((count, sum)),
                    (None, None) => None,
                }
            }),
            (Aggregator::Avg, false) => {
                parse_quote!({ (#current_bin_ident.0 + #new_bin_ident.0, #current_bin_ident.1 + #new_bin_ident.1) })
            }
            (Aggregator::CountDistinct, _) => unreachable!("no two phase for count distinct"),
        }
    }

    fn expression_type(&self, input_context: &CombiningContext) -> BinType {
        self.bin_type(&input_context.value_context)
    }
}

impl CodeGenerator<MemoryAddingContext, BinType, syn::Expr> for TwoPhaseAggregation {
    fn generate(&self, input_context: &MemoryAddingContext) -> syn::Expr {
        let input_type = self
            .incoming_expression
            .expression_type(&input_context.value_context);
        let input_nullable = input_type.is_optional();
        let expr_type = data_type_as_syn_type(
            self.intermediate_type_def(&input_context.value_context)
                .as_datatype()
                .unwrap(),
        );
        let memory_ident = input_context.memory_value_ident();
        let bin_value_ident = input_context.bin_value_ident();
        match (&self.aggregator, input_nullable) {
            (Aggregator::Count, _) => parse_quote!({
                arroyo_worker::operators::aggregating_window::count_add(#memory_ident, #bin_value_ident)
            }),
            (Aggregator::Sum, true) => parse_quote!({
                arroyo_worker::operators::aggregating_window::nullable_sum_add::<#expr_type>(#memory_ident, #bin_value_ident)
            }),
            (Aggregator::Sum, false) => parse_quote!({
                arroyo_worker::operators::aggregating_window::non_nullable_sum_add::<#expr_type>(#memory_ident, #bin_value_ident)
            }),
            (Aggregator::Min, true) => parse_quote!({
                arroyo_worker::operators::aggregating_window::nullable_heap_add::<#expr_type>(#memory_ident, #bin_value_ident)
            }),
            (Aggregator::Min, false) => parse_quote!({
                arroyo_worker::operators::aggregating_window::non_nullable_heap_add::<#expr_type>(#memory_ident, #bin_value_ident)
            }),
            (Aggregator::Max, true) => parse_quote!({
                arroyo_worker::operators::aggregating_window::nullable_heap_add::<#expr_type>(#memory_ident, #bin_value_ident)
            }),
            (Aggregator::Max, false) => parse_quote!({
                arroyo_worker::operators::aggregating_window::non_nullable_heap_add::<#expr_type>(#memory_ident, #bin_value_ident)
            }),
            (Aggregator::Avg, true) => parse_quote!({
                arroyo_worker::operators::aggregating_window::nullable_average_add::<#expr_type>(#memory_ident, #bin_value_ident)
            }),
            (Aggregator::Avg, false) => parse_quote!({
                arroyo_worker::operators::aggregating_window::non_nullable_average_add::<#expr_type>(#memory_ident, #bin_value_ident)
            }),
            (Aggregator::CountDistinct, true) => todo!(),
            (Aggregator::CountDistinct, false) => todo!(),
        }
    }

    fn expression_type(&self, input_context: &MemoryAddingContext) -> BinType {
        self.mem_type(&input_context.value_context)
    }
}

impl CodeGenerator<MemoryRemovingContext, BinType, syn::Expr> for TwoPhaseAggregation {
    fn generate(&self, input_context: &MemoryRemovingContext) -> syn::Expr {
        let input_type = self
            .incoming_expression
            .expression_type(&input_context.value_context);
        let input_nullable = input_type.is_optional();
        let expr_type = data_type_as_syn_type(
            self.intermediate_type_def(&input_context.value_context)
                .as_datatype()
                .unwrap(),
        );
        let memory_ident = input_context.memory_value_ident();
        let bin_value_ident = input_context.bin_value_ident();
        match (&self.aggregator, input_nullable) {
            (Aggregator::Count, true) | (Aggregator::Count, false) => parse_quote!({
                arroyo_worker::operators::aggregating_window::count_remove(#memory_ident, #bin_value_ident)
            }),
            (Aggregator::Sum, true) => parse_quote!({
                arroyo_worker::operators::aggregating_window::nullable_sum_remove::<#expr_type>(#memory_ident, #bin_value_ident)
            }),
            (Aggregator::Sum, false) => parse_quote!({
                arroyo_worker::operators::aggregating_window::non_nullable_sum_remove::<#expr_type>(#memory_ident, #bin_value_ident)
            }),
            (Aggregator::Min, true) | (Aggregator::Max, true) => parse_quote!({
                arroyo_worker::operators::aggregating_window::nullable_heap_remove::<#expr_type>(#memory_ident, #bin_value_ident)
            }),
            (Aggregator::Min, false) | (Aggregator::Max, false) => parse_quote!({
                arroyo_worker::operators::aggregating_window::non_nullable_heap_remove::<#expr_type>(#memory_ident, #bin_value_ident)
            }),
            (Aggregator::Avg, true) => parse_quote!({
                arroyo_worker::operators::aggregating_window::nullable_average_remove::<#expr_type>(#memory_ident, #bin_value_ident)
            }),
            (Aggregator::Avg, false) => parse_quote!({
                arroyo_worker::operators::aggregating_window::non_nullable_average_remove::<#expr_type>(#memory_ident, #bin_value_ident)
            }),
            (Aggregator::CountDistinct, true) => todo!(),
            (Aggregator::CountDistinct, false) => todo!(),
        }
    }

    fn expression_type(&self, input_context: &MemoryRemovingContext) -> BinType {
        self.mem_type(&input_context.value_context)
    }
}

impl CodeGenerator<BinAggregatingContext, TypeDef, syn::Expr> for TwoPhaseAggregation {
    fn generate(&self, input_context: &BinAggregatingContext) -> syn::Expr {
        let input_nullable = self
            .incoming_expression
            .expression_type(&input_context.value_context)
            .is_optional();
        let bin_name = input_context.bin_name();
        match (&self.aggregator, input_nullable) {
            (Aggregator::Count, _)
            | (Aggregator::Sum, _)
            | (Aggregator::Min, _)
            | (Aggregator::Max, _) => parse_quote!(#bin_name.clone()),
            (Aggregator::Avg, true) => parse_quote!(match #bin_name {
                Some((count, sum)) => Some((*sum as f64) / (*count as f64)),
                None => None,
            }),
            (Aggregator::Avg, false) => {
                parse_quote!({ (#bin_name.1 as f64) / (#bin_name.0 as f64) })
            }
            (Aggregator::CountDistinct, true) => unimplemented!(),
            (Aggregator::CountDistinct, false) => unimplemented!(),
        }
    }

    fn expression_type(&self, input_context: &BinAggregatingContext) -> TypeDef {
        self.output_type_def(&input_context.value_context)
    }
}

impl CodeGenerator<MemoryAggregatingContext, TypeDef, syn::Expr> for TwoPhaseAggregation {
    fn generate(&self, input_context: &MemoryAggregatingContext) -> syn::Expr {
        let bin_name = input_context.bin_name();
        let incoming_type = self
            .incoming_expression
            .expression_type(&input_context.value_context);
        let input_nullable = incoming_type.is_optional();
        let expr_type = data_type_as_syn_type(incoming_type.as_datatype().unwrap());
        match (&self.aggregator, input_nullable) {
            (Aggregator::Count, _) => {
                parse_quote!({ arroyo_worker::operators::aggregating_window::count_aggregate(#bin_name) })
            }
            (Aggregator::Sum, true) => parse_quote!({
                arroyo_worker::operators::aggregating_window::nullable_sum_aggregate::<#expr_type>(#bin_name)
            }),
            (Aggregator::Sum, false) => parse_quote!({
                arroyo_worker::operators::aggregating_window::non_nullable_sum_aggregate::<#expr_type>(#bin_name)
            }),
            (Aggregator::Min, true) => parse_quote!({
                arroyo_worker::operators::aggregating_window::nullable_min_heap_aggregate::<#expr_type>(#bin_name)
            }),
            (Aggregator::Min, false) => parse_quote!({
                arroyo_worker::operators::aggregating_window::non_nullable_max_heap_aggregate::<#expr_type>(#bin_name)
            }),
            (Aggregator::Max, true) => parse_quote!({
                arroyo_worker::operators::aggregating_window::nullable_max_heap_aggregate::<#expr_type>(#bin_name)
            }),
            (Aggregator::Max, false) => parse_quote!({
                arroyo_worker::operators::aggregating_window::non_nullable_max_heap_aggregate::<#expr_type>(#bin_name)
            }),
            (Aggregator::Avg, true) => parse_quote!({
                match &#bin_name.2 {
                    Some((count, sum)) => Some((*sum as f64) / (*count as f64)),
                    None => None,
                }
            }),
            (Aggregator::Avg, false) => {
                parse_quote!({ (#bin_name.1 as f64) / (#bin_name.0 as f64) })
            }
            (Aggregator::CountDistinct, true) => unimplemented!(),
            (Aggregator::CountDistinct, false) => unimplemented!(),
        }
    }

    fn expression_type(&self, input_context: &MemoryAggregatingContext) -> TypeDef {
        self.output_type_def(&input_context.value_context)
    }
}

impl TwoPhaseAggregation {
    fn output_type_def(&self, input_context: &ValuePointerContext) -> TypeDef {
        let incoming_type = self.incoming_expression.expression_type(input_context);
        let (data_type, nullable) = match incoming_type {
            TypeDef::StructDef(_, _) => unreachable!(),
            TypeDef::DataType(data_type, nullable) => (data_type, nullable),
        };
        let aggregate_type = match self.aggregator {
            Aggregator::Count => return TypeDef::DataType(DataType::Int64, false),
            Aggregator::Avg => {
                avg_return_type(&data_type).expect("datafusion should've prevented this")
            }
            Aggregator::Sum => {
                sum_return_type(&data_type).expect("datafusion should've prevented this")
            }
            Aggregator::Min | Aggregator::Max => data_type,
            Aggregator::CountDistinct => unimplemented!(),
        };
        TypeDef::DataType(aggregate_type, nullable)
    }

    // the data type to be used in intermediate aggregations. Mainly relevant for average.
    fn intermediate_type_def(&self, input_context: &ValuePointerContext) -> TypeDef {
        let incoming_type = self.incoming_expression.expression_type(input_context);
        let (data_type, nullable) = match incoming_type {
            TypeDef::StructDef(_, _) => unreachable!(),
            TypeDef::DataType(data_type, nullable) => (data_type, nullable),
        };
        let aggregate_type = match self.aggregator {
            Aggregator::Count => return TypeDef::DataType(DataType::Int64, false),
            Aggregator::Avg | Aggregator::Sum => {
                sum_return_type(&data_type).expect("datafusion should've prevented this")
            }
            Aggregator::Min | Aggregator::Max => data_type,
            Aggregator::CountDistinct => unimplemented!(),
        };
        TypeDef::DataType(aggregate_type, nullable)
    }

    fn bin_type(&self, input_context: &ValuePointerContext) -> BinType {
        let input_type = self.incoming_expression.expression_type(input_context);
        let aggregate_type = self
            .intermediate_type_def(input_context)
            .as_datatype()
            .expect("aggregates shouldn't return structs")
            .clone();
        let input_nullable = input_type.is_optional();
        match (&self.aggregator, input_nullable) {
            (Aggregator::Count, _) => BinType::DataType(DataType::Int64),
            (Aggregator::Sum, true) | (Aggregator::Min, true) | (Aggregator::Max, true) => {
                BinType::Option(Box::new(BinType::DataType(aggregate_type)))
            }
            (Aggregator::Sum, false) | (Aggregator::Min, false) | (Aggregator::Max, false) => {
                BinType::DataType(aggregate_type)
            }
            (Aggregator::Avg, true) => BinType::Option(Box::new(BinType::Tuple(vec![
                BinType::DataType(DataType::Int64),
                BinType::DataType(aggregate_type),
            ]))),
            (Aggregator::Avg, false) => BinType::Tuple(vec![
                BinType::DataType(DataType::Int64),
                BinType::DataType(aggregate_type),
            ]),
            (Aggregator::CountDistinct, _) => unimplemented!(),
        }
    }

    fn mem_type(&self, input_context: &ValuePointerContext) -> BinType {
        let aggregate_type_def = self.intermediate_type_def(input_context);
        let aggregate_data_type = aggregate_type_def.as_datatype().unwrap().clone();
        match (
            &self.aggregator,
            self.incoming_expression
                .expression_type(input_context)
                .is_optional(),
        ) {
            (Aggregator::Count, _) => BinType::Tuple(vec![
                BinType::DataType(DataType::Int64),
                BinType::DataType(DataType::Int64),
            ]),
            (Aggregator::Sum, true) => BinType::Tuple(vec![
                BinType::DataType(DataType::Int64),
                BinType::DataType(DataType::Int64),
                BinType::Option(Box::new(BinType::DataType(aggregate_data_type))),
            ]),
            (Aggregator::Min, true) | (Aggregator::Max, true) => BinType::Tuple(vec![
                BinType::DataType(DataType::Int64),
                BinType::BTreeMap(
                    Box::new(BinType::DataType(aggregate_data_type)),
                    Box::new(BinType::Usize),
                ),
            ]),
            (Aggregator::Sum, false) => BinType::Tuple(vec![
                BinType::DataType(DataType::Int64),
                BinType::DataType(aggregate_data_type),
            ]),
            (Aggregator::Min, false) | (Aggregator::Max, false) => BinType::BTreeMap(
                Box::new(BinType::DataType(aggregate_data_type)),
                Box::new(BinType::Usize),
            ),
            (Aggregator::Avg, true) => BinType::Tuple(vec![
                BinType::DataType(DataType::Int64),
                BinType::DataType(DataType::Int64),
                BinType::Option(Box::new(BinType::Tuple(vec![
                    BinType::DataType(DataType::Int64),
                    BinType::DataType(aggregate_data_type),
                ]))),
            ]),
            (Aggregator::Avg, false) => BinType::Tuple(vec![
                BinType::DataType(DataType::Int64),
                BinType::DataType(aggregate_data_type),
            ]),
            (Aggregator::CountDistinct, _) => unimplemented!(),
        }
    }
}
