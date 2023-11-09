use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use arrow_schema::DataType;
use arroyo_datastream::{
    EdgeType, ExpressionReturnType, NonWindowAggregator, Operator, PeriodicWatermark, Program,
    ProgramUdf, SlidingAggregatingTopN, SlidingWindowAggregator, StreamEdge, StreamNode,
    TumblingTopN, TumblingWindowAggregator, WindowAgg, WindowType,
};

use petgraph::graph::{DiGraph, NodeIndex};
use quote::{quote, ToTokens};
use syn::{parse_quote, parse_str, Type};

use crate::expressions::AggregateComputation;
use crate::{
    code_gen::{
        BinAggregatingContext, CodeGenerator, CombiningContext, JoinListsContext, JoinPairContext,
        MemoryAddingContext, MemoryAggregatingContext, MemoryRemovingContext,
        ValueBinMergingContext, ValuePointerContext, VecAggregationContext,
    },
    expressions::{Column, ColumnExpression, Expression, SortExpression},
    external::{ProcessingMode, SinkUpdateType, SqlSink, SqlSource},
    operators::{AggregateProjection, Projection, TwoPhaseAggregateProjection},
    optimizations::optimize,
    pipeline::{
        JoinType, MethodCompiler, RecordTransform, SourceOperator, SqlOperator, WindowFunction,
    },
    types::{StructDef, StructField, StructPair, TypeDef},
    ArroyoSchemaProvider, SqlConfig,
};
use anyhow::Result;
use petgraph::Direction;

#[derive(Debug, Clone)]
pub enum PlanOperator {
    Source(String, SqlSource),
    Watermark(PeriodicWatermark),
    RecordTransform(RecordTransform),
    FusedRecordTransform(FusedRecordTransform),
    Unkey,
    WindowAggregate {
        window: WindowType,
        projection: AggregateProjection,
    },
    NonWindowAggregate {
        input_is_update: bool,
        expiration: Duration,
        projection: TwoPhaseAggregateProjection,
    },
    TumblingWindowTwoPhaseAggregator {
        tumble_width: Duration,
        projection: TwoPhaseAggregateProjection,
    },
    SlidingWindowTwoPhaseAggregator {
        width: Duration,
        slide: Duration,
        projection: TwoPhaseAggregateProjection,
    },
    InstantJoin,
    JoinWithExpiration {
        left_expiration: Duration,
        right_expiration: Duration,
        join_type: JoinType,
    },
    JoinListMerge(JoinType, StructPair),
    JoinPairMerge(JoinType, StructPair),
    Flatten,
    // TODO: figure out naming of various things called 'window'
    WindowFunction(WindowFunctionOperator),
    TumblingLocalAggregator {
        width: Duration,
        projection: TwoPhaseAggregateProjection,
    },
    SlidingAggregatingTopN {
        width: Duration,
        slide: Duration,
        aggregating_projection: TwoPhaseAggregateProjection,
        order_by: Vec<SortExpression>,
        partition_projection: Projection,
        converting_projection: Projection,
        max_elements: usize,
    },
    TumblingTopN {
        width: Duration,
        max_elements: usize,
        window_function: WindowFunctionOperator,
    },
    // for external nodes, mainly sinks.
    StreamOperator(String, Operator),
    ToDebezium,
    FromDebezium,
    FromUpdating,
    Sink(String, SqlSink),
}

#[derive(Debug, Clone)]
pub struct WindowFunctionOperator {
    pub window_function: WindowFunction,
    pub order_by: Vec<SortExpression>,
    pub window_type: WindowType,
    pub result_struct: StructDef,
    pub field_name: String,
}

#[derive(Debug, Clone)]
pub struct FusedRecordTransform {
    pub expressions: Vec<RecordTransform>,
    pub output_types: Vec<PlanType>,
    pub expression_return_type: ExpressionReturnType,
}

impl FusedRecordTransform {
    fn to_operator(&self) -> Operator {
        match self.expression_return_type {
            ExpressionReturnType::Predicate => self.to_predicate_operator(),
            ExpressionReturnType::Record => self.to_record_operator(),
            ExpressionReturnType::OptionalRecord => self.to_optional_record_operator(),
        }
    }

    fn to_predicate_operator(&self) -> Operator {
        let mut predicates = Vec::new();
        let mut names = Vec::new();
        for expression in &self.expressions {
            let RecordTransform::Filter(predicate) = expression else {
                panic!("FusedRecordTransform.to_predicate_operator() called on non-predicate expression");
            };
            names.push("filter");
            predicates.push(predicate.generate(&ValuePointerContext::new()));
        }
        let predicate: syn::Expr = parse_quote!( {
            let arg = &record.value;
            #(#predicates)&&*
        });
        Operator::ExpressionOperator {
            name: format!("sql_fused<{}>", names.join(",")),
            expression: quote!(#predicate).to_string(),
            return_type: ExpressionReturnType::Predicate,
        }
    }

    fn to_record_operator(&self) -> Operator {
        let mut record_expressions: Vec<syn::Stmt> = Vec::new();
        let mut names = Vec::new();
        for i in 0..self.expressions.len() {
            let expression = &self.expressions[i];
            let output_type = &self.output_types[i];
            match expression {
                RecordTransform::ValueProjection(projection) => {
                    names.push("value_project");
                    let record_type = output_type.record_type();
                    let record_expression =
                        ValuePointerContext::new().compile_value_map_expr(projection);
                    record_expressions.push(parse_quote!(
                            let record: #record_type =  #record_expression;
                    ));
                }
                RecordTransform::KeyProjection(projection) => {
                    names.push("key_project");
                    let record_type = output_type.record_type();
                    let record_expression =
                        ValuePointerContext::new().compile_key_map_expression(projection);
                    record_expressions.push(parse_quote!(
                            let record: #record_type = #record_expression;
                    ));
                }
                RecordTransform::TimestampAssignment(timestamp_expression) => {
                    names.push("timestamp_assignment");
                    let record_type = output_type.record_type();
                    let record_expression = ValuePointerContext::new()
                        .compile_timestamp_record_expression(timestamp_expression);
                    record_expressions.push(parse_quote!(
                            let record: #record_type = #record_expression;
                    ));
                }
                RecordTransform::Filter(_) => unreachable!(),
                RecordTransform::UnnestProjection(_) => {
                    unreachable!("unnest projection cannot be fused")
                }
            }
        }
        let combined: syn::Expr = parse_quote!({
            #(#record_expressions)*
            record
        });
        Operator::ExpressionOperator {
            name: format!("sql_fused<{}>", names.join(",")),
            expression: quote!(#combined).to_string(),
            return_type: ExpressionReturnType::Record,
        }
    }

    fn to_optional_record_operator(&self) -> Operator {
        let mut names = Vec::new();
        let mut record_expressions: Vec<syn::Stmt> = Vec::new();
        for i in 0..self.expressions.len() {
            let expression = &self.expressions[i];
            let output_type = &self.output_types[i];
            let is_updating = matches!(output_type, PlanType::Updating(_));
            match (expression, is_updating) {
                (RecordTransform::ValueProjection(projection), false) => {
                    names.push("value_project");
                    let record_type = output_type.record_type();
                    let record_expression =
                        ValuePointerContext::new().compile_value_map_expr(projection);
                    record_expressions.push(parse_quote!(
                            let record: #record_type = #record_expression;
                    ));
                }
                (RecordTransform::ValueProjection(projection), true) => {
                    names.push("updating_value_project");
                    let record_type = output_type.record_type();
                    let record_expression = ValuePointerContext::new()
                        .compile_updating_value_map_expression(projection);
                    record_expressions.push(parse_quote!(
                            let record: #record_type = #record_expression?;
                    ));
                }
                (RecordTransform::KeyProjection(projection), false) => {
                    names.push("key_project");
                    let record_expression =
                        ValuePointerContext::new().compile_key_map_expression(projection);
                    let record_type = output_type.record_type();
                    record_expressions.push(parse_quote!(
                            let record: #record_type = #record_expression;
                    ));
                }
                (RecordTransform::Filter(predicate), false) => {
                    names.push("filter");
                    let predicate_expression =
                        ValuePointerContext::new().compile_filter_expression(predicate);
                    record_expressions.push(parse_quote!(
                        if !#predicate_expression {
                            return None;
                        }
                    ));
                }
                (RecordTransform::Filter(predicate), true) => {
                    names.push("updating_filter");
                    let record_expression = ValuePointerContext::new()
                        .compile_updating_filter_optional_record_expression(predicate);
                    let record_type = output_type.record_type();
                    record_expressions.push(parse_quote!(
                            let record: #record_type = #record_expression?;));
                }
                (RecordTransform::TimestampAssignment(timestamp_expression), false) => {
                    names.push("timestamp_assignment");
                    let record_expression = ValuePointerContext::new()
                        .compile_timestamp_record_expression(timestamp_expression);
                    let record_type = output_type.record_type();
                    record_expressions.push(parse_quote!(
                            let record: #record_type = #record_expression;
                    ));
                }
                (RecordTransform::UnnestProjection(_), _) => {
                    unreachable!("unnest projection cannot be fused")
                }
                _ => unimplemented!(),
            }
        }
        let combined: syn::Expr = parse_quote!({
            #(#record_expressions)*
            Some(record)
        });
        Operator::ExpressionOperator {
            name: format!("sql_fused<{}>", names.join(",")),
            expression: quote!(#combined).to_string(),
            return_type: ExpressionReturnType::OptionalRecord,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PlanNode {
    pub operator: PlanOperator,
    pub output_type: PlanType,
}

impl PlanNode {
    fn into_stream_node(&self, index: usize, sql_config: &SqlConfig) -> StreamNode {
        let name = format!("{}_{}", self.prefix(), index);
        let operator = self.to_operator();
        StreamNode {
            operator_id: name,
            parallelism: sql_config.default_parallelism,
            operator,
        }
    }

    fn from_record_transform(record_transform: RecordTransform, input_node: &PlanNode) -> Self {
        let input_type = &input_node.output_type;
        let output_type = match &record_transform {
            RecordTransform::ValueProjection(value_projection) => {
                input_type.with_value(value_projection.output_struct())
            }
            RecordTransform::KeyProjection(key_projection) => {
                input_type.with_key(key_projection.output_struct())
            }
            RecordTransform::TimestampAssignment(_) | RecordTransform::Filter(_) => {
                input_type.clone()
            }
            RecordTransform::UnnestProjection(p) => input_type.with_value(p.output_struct()),
        };
        PlanNode {
            operator: PlanOperator::RecordTransform(record_transform),
            output_type,
        }
    }

    fn prefix(&self) -> String {
        match &self.operator {
            PlanOperator::Source(name, _) => name.to_string(),
            PlanOperator::Watermark(_) => "watermark".to_string(),
            PlanOperator::RecordTransform(record_transform) => record_transform.name(),
            PlanOperator::FusedRecordTransform(_) => "fused".to_string(),
            PlanOperator::Unkey => "unkey".to_string(),
            PlanOperator::WindowAggregate { .. } => "window_aggregate".to_string(),
            PlanOperator::TumblingWindowTwoPhaseAggregator { .. } => {
                "tumbling_window_two_phase_aggregator".to_string()
            }
            PlanOperator::SlidingWindowTwoPhaseAggregator { .. } => {
                "sliding_window_two_phase_aggregator".to_string()
            }
            PlanOperator::InstantJoin => "instant_join".to_string(),
            PlanOperator::JoinWithExpiration { .. } => "join_with_expiration".to_string(),
            PlanOperator::JoinListMerge(_, _) => "join_list_merge".to_string(),
            PlanOperator::JoinPairMerge(_, _) => "join_pair_merge".to_string(),
            PlanOperator::Flatten => "flatten".to_string(),
            PlanOperator::WindowFunction { .. } => "window_function".to_string(),
            PlanOperator::StreamOperator(name, _) => name.to_string(),
            PlanOperator::TumblingLocalAggregator { .. } => "tumbling_local_aggregator".to_string(),
            PlanOperator::SlidingAggregatingTopN { .. } => "sliding_aggregating_top_n".to_string(),
            PlanOperator::TumblingTopN { .. } => "tumbling_top_n".to_string(),
            PlanOperator::Sink(name, _) => format!("sink_{}", name),
            PlanOperator::ToDebezium => "to_debezium".to_string(),
            PlanOperator::FromDebezium => "from_debezium".to_string(),
            PlanOperator::FromUpdating => "from_updating".to_string(),
            PlanOperator::NonWindowAggregate { .. } => "non_window_aggregate".to_string(),
        }
    }

    fn to_operator(&self) -> Operator {
        match &self.operator {
            PlanOperator::Source(_name, source) => source.operator.clone(),
            PlanOperator::Watermark(watermark) => Operator::Watermark(watermark.clone()),
            PlanOperator::RecordTransform(record_transform) => {
                record_transform.as_operator(self.output_type.is_updating())
            }
            PlanOperator::WindowAggregate { window, projection } => {
                let aggregating_context = VecAggregationContext::new();
                let aggregate_expr = projection.generate(&aggregating_context);
                arroyo_datastream::Operator::Window {
                    typ: window.clone(),
                    agg: Some(WindowAgg::Expression {
                        // TODO: find a way to get a more useful name
                        name: "aggregation".to_string(),
                        expression: quote::quote! { #aggregate_expr }.to_string(),
                    }),
                    flatten: false,
                }
            }
            PlanOperator::TumblingWindowTwoPhaseAggregator {
                tumble_width,
                projection,
            } => {
                let value_bin_merging_context = ValueBinMergingContext::new();
                let bin_type = value_bin_merging_context
                    .bin_syn_type(projection)
                    .into_token_stream()
                    .to_string();
                let bin_merger = value_bin_merging_context
                    .compile_closure(projection)
                    .into_token_stream()
                    .to_string();

                let aggregating_context = BinAggregatingContext::new();
                let aggregator = aggregating_context
                    .compile_closure(projection)
                    .into_token_stream()
                    .to_string();

                arroyo_datastream::Operator::TumblingWindowAggregator(TumblingWindowAggregator {
                    width: *tumble_width,
                    aggregator,
                    bin_merger,
                    bin_type,
                })
            }
            PlanOperator::SlidingWindowTwoPhaseAggregator {
                width,
                slide,
                projection,
            } => {
                let value_bin_merger_context = ValueBinMergingContext::new();
                let bin_type = value_bin_merger_context
                    .bin_syn_type(projection)
                    .into_token_stream()
                    .to_string();
                let bin_merger = value_bin_merger_context
                    .compile_closure(projection)
                    .into_token_stream()
                    .to_string();

                let memory_add_context = MemoryAddingContext::new();
                let in_memory_add = memory_add_context
                    .compile_closure(projection)
                    .into_token_stream()
                    .to_string();
                let mem_type = memory_add_context
                    .memory_type(projection)
                    .into_token_stream()
                    .to_string();

                let memory_remove_context = MemoryRemovingContext::new();
                let in_memory_remove = memory_remove_context
                    .compile_closure(projection)
                    .into_token_stream()
                    .to_string();

                let aggregating_context = MemoryAggregatingContext::new();
                let aggregator = aggregating_context
                    .compile_closure(projection)
                    .into_token_stream()
                    .to_string();

                arroyo_datastream::Operator::SlidingWindowAggregator(SlidingWindowAggregator {
                    width: *width,
                    slide: *slide,
                    aggregator,
                    bin_merger,
                    in_memory_add,
                    in_memory_remove,
                    bin_type,
                    mem_type,
                })
            }
            PlanOperator::InstantJoin => Operator::WindowJoin {
                window: WindowType::Instant,
            },
            PlanOperator::JoinWithExpiration {
                left_expiration,
                right_expiration,
                join_type,
            } => Operator::JoinWithExpiration {
                left_expiration: *left_expiration,
                right_expiration: *right_expiration,
                join_type: join_type.clone().into(),
            },
            PlanOperator::JoinListMerge(join_type, struct_pair) => {
                let context =
                    JoinListsContext::new(struct_pair.left.clone(), struct_pair.right.clone());
                let record_expression = context.compile_list_merge_record_expression(join_type);
                MethodCompiler::record_expression_operator("join_list_merge", record_expression)
            }
            PlanOperator::JoinPairMerge(join_type, struct_pair) => {
                let context =
                    JoinPairContext::new(struct_pair.left.clone(), struct_pair.right.clone());
                match join_type {
                    JoinType::Inner => {
                        let record_expression =
                            context.compile_pair_merge_record_expression(join_type);
                        MethodCompiler::record_expression_operator("join_merge", record_expression)
                    }
                    JoinType::Left | JoinType::Right | JoinType::Full => {
                        let value_expression =
                            context.compile_updating_pair_merge_value_expression(join_type);
                        MethodCompiler::value_updating_operator(
                            "updating_join_merge",
                            value_expression,
                        )
                    }
                }
            }

            PlanOperator::WindowFunction(WindowFunctionOperator {
                window_function,
                order_by,
                window_type,
                result_struct,
                field_name: _,
            }) => {
                let window_field = result_struct.fields.last().unwrap().field_ident();
                let result_struct_name = result_struct.get_type();
                let mut field_assignments: Vec<_> = result_struct
                    .fields
                    .iter()
                    .take(result_struct.fields.len() - 1)
                    .map(|f| {
                        let ident = f.field_ident();
                        quote! { #ident: arg.#ident.clone() }
                    })
                    .collect();

                match window_function {
                    WindowFunction::RowNumber => {
                        field_assignments.push(quote! {
                            #window_field: i as u64
                        });
                    }
                }

                let output_expression = quote!(#result_struct_name {
                    #(#field_assignments, )*
                });

                let sort = if !order_by.is_empty() {
                    let sort_tokens = SortExpression::sort_tuple_expression(order_by);
                    Some(quote!(arg.sort_by_key(|arg| #sort_tokens);))
                } else {
                    None
                };
                arroyo_datastream::Operator::Window {
                    typ: window_type.clone(),
                    agg: Some(WindowAgg::Expression {
                        name: "sql_window".to_string(),
                        expression: quote! {
                            {
                                #sort
                                let mut result = vec![];
                                for (index, arg) in arg.iter().enumerate() {
                                    let i = index + 1;
                                    result.push(#output_expression);
                                }
                                result
                            }
                        }
                        .to_string(),
                    }),
                    flatten: true,
                }
            }
            PlanOperator::StreamOperator(_, stream_operator) => stream_operator.clone(),
            PlanOperator::FusedRecordTransform(fused_record_transform) => {
                fused_record_transform.to_operator()
            }
            PlanOperator::Unkey => arroyo_datastream::Operator::ExpressionOperator {
                name: "unkey".to_string(),
                expression: quote! {
                    arroyo_types::Record {
                        timestamp: record.timestamp,
                        key: None,
                        value: record.value.clone(),
                    }
                }
                .to_string(),
                return_type: arroyo_datastream::ExpressionReturnType::Record,
            },
            PlanOperator::TumblingLocalAggregator { width, projection } => {
                let bin_merging_context = ValueBinMergingContext::new();
                let bin_merger = bin_merging_context
                    .compile_closure(projection)
                    .into_token_stream()
                    .to_string();
                let bin_type = bin_merging_context
                    .bin_syn_type(projection)
                    .into_token_stream()
                    .to_string();

                arroyo_datastream::Operator::TumblingWindowAggregator(TumblingWindowAggregator {
                    width: *width,
                    aggregator: quote!(|key, window, arg| { arg.clone() }).to_string(),
                    bin_merger,
                    bin_type,
                })
            }
            PlanOperator::SlidingAggregatingTopN {
                width,
                slide,
                aggregating_projection,
                order_by,
                partition_projection,
                converting_projection,
                max_elements,
            } => {
                let bin_merger = CombiningContext::new()
                    .compile_closure(aggregating_projection)
                    .into_token_stream()
                    .to_string();
                let bin_type = aggregating_projection
                    .bin_type(&ValuePointerContext::new())
                    .syn_type()
                    .into_token_stream()
                    .to_string();

                let memory_add_context = MemoryAddingContext::new();
                let mem_type = memory_add_context
                    .memory_type(aggregating_projection)
                    .into_token_stream()
                    .to_string();
                let in_memory_add = memory_add_context
                    .compile_closure(aggregating_projection)
                    .into_token_stream()
                    .to_string();

                let memory_remove_context = MemoryRemovingContext::new();
                let in_memory_remove = memory_remove_context
                    .compile_closure(aggregating_projection)
                    .into_token_stream()
                    .to_string();

                let memory_aggregate_context = MemoryAggregatingContext::new();
                let aggregate_expr = aggregating_projection.generate(&memory_aggregate_context);

                let sort_tuple = SortExpression::sort_tuple_type(order_by);
                let sort_key_type = quote!(#sort_tuple).to_string();

                let partition_expr = partition_projection.generate(&ValuePointerContext::new());
                let partition_arg = ValuePointerContext::new().variable_ident();
                let partition_function: syn::ExprClosure =
                    parse_quote!(|#partition_arg| {#partition_expr});
                let projection_expr = converting_projection.generate(&ValuePointerContext::new());

                let sort_tokens = SortExpression::sort_tuple_expression(order_by);

                let mem_ident = memory_aggregate_context.bin_name();
                let key_ident = memory_aggregate_context.key_ident();

                let extractor = quote!(
                    |#key_ident, #mem_ident| {
                        // this window is just there because the aggregate expression depends on it, but it is an
                        // error for the key extract to rely on it
                        let window = arroyo_types::Window::new(std::time::UNIX_EPOCH, std::time::UNIX_EPOCH);
                        let arg = #aggregate_expr;
                        let arg = #projection_expr;

                        #sort_tokens
                    }
                ).to_string();

                let window_ident = memory_aggregate_context.window_ident();

                let aggregator = quote!(|#key_ident, #window_ident, #mem_ident|
                    {
                        let #partition_arg = #aggregate_expr;
                        #projection_expr
                    }
                )
                .to_string();

                arroyo_datastream::Operator::SlidingAggregatingTopN(SlidingAggregatingTopN {
                    width: *width,
                    slide: *slide,
                    bin_merger,
                    in_memory_add,
                    in_memory_remove,
                    partitioning_func: partition_function.into_token_stream().to_string(),
                    extractor,
                    aggregator,
                    bin_type,
                    mem_type,
                    sort_key_type,
                    max_elements: *max_elements,
                })
            }
            PlanOperator::TumblingTopN {
                width,
                max_elements,
                window_function,
            } => {
                let sort_expression =
                    SortExpression::sort_tuple_expression(&window_function.order_by);

                let window_field = window_function
                    .result_struct
                    .fields
                    .last()
                    .unwrap()
                    .field_ident();
                let output_struct = window_function.result_struct.get_type();
                let mut field_assignments: Vec<_> = window_function
                    .result_struct
                    .fields
                    .iter()
                    .take(window_function.result_struct.fields.len() - 1)
                    .map(|f| {
                        let ident = f.field_ident();
                        quote! { #ident: arg.#ident.clone() }
                    })
                    .collect();

                match window_function.window_function {
                    WindowFunction::RowNumber => {
                        field_assignments.push(quote! {
                            #window_field: i as u64
                        });
                    }
                }
                let output_expression = quote!(#output_struct {
                    #(#field_assignments, )*
                });

                let extractor = quote!(
                    |arg| {
                        #sort_expression
                    }
                )
                .to_string();
                let converter = quote!(
                    |arg, i| #output_expression
                )
                .to_string();
                let sort_type = SortExpression::sort_tuple_type(&window_function.order_by);
                let partition_key_type = quote!(#sort_type).to_string();

                arroyo_datastream::Operator::TumblingTopN(TumblingTopN {
                    width: *width,
                    max_elements: *max_elements,
                    extractor,
                    partition_key_type,
                    converter,
                })
            }
            PlanOperator::Flatten => arroyo_datastream::Operator::FlattenOperator {
                name: "flatten".into(),
            },
            PlanOperator::Sink(_, sql_sink) => sql_sink.operator.clone(),
            PlanOperator::ToDebezium => arroyo_datastream::Operator::ExpressionOperator {
                name: "to_debezium".into(),
                expression: quote!({
                    arroyo_types::Record {
                        timestamp: record.timestamp,
                        key: None,
                        value: record.value.clone().into(),
                    }
                })
                .to_string(),
                return_type: ExpressionReturnType::Record,
            },
            PlanOperator::FromDebezium => arroyo_datastream::Operator::ExpressionOperator {
                name: "from_debezium".into(),
                expression: quote!({
                    arroyo_types::Record {
                        timestamp: record.timestamp,
                        key: None,
                        value: record.value.clone().into(),
                    }
                })
                .to_string(),
                return_type: ExpressionReturnType::Record,
            },
            PlanOperator::FromUpdating => Operator::ExpressionOperator {
                name: "from_updating".into(),
                expression: quote!({
                    arroyo_types::Record {
                        timestamp: record.timestamp,
                        key: None,
                        value: record.value.lower(),
                    }
                })
                .to_string(),
                return_type: ExpressionReturnType::Record,
            },
            PlanOperator::NonWindowAggregate {
                input_is_update,
                projection,
                expiration,
            } => {
                if *input_is_update {
                    let memory_aggregate_context = MemoryAggregatingContext::new();

                    let memory_aggregate_closure =
                        memory_aggregate_context.compile_non_windowed_closure(projection);

                    let bin_merge_context = ValueBinMergingContext::new();

                    let current_bin_ident = bin_merge_context.bin_context.current_bin_ident();
                    let arg_ident = bin_merge_context.value_context.variable_ident();

                    let bin_merger_expr = projection.generate(&bin_merge_context);
                    let bin_type = projection
                        .expression_type(&ValueBinMergingContext::new())
                        .syn_type();
                    let memory_type = projection
                        .memory_type(&ValuePointerContext::new())
                        .syn_type();
                    let memory_add = projection.generate(&MemoryAddingContext::new());
                    let memory_removing_context = MemoryRemovingContext::new();
                    let memory_remove = projection.generate(&memory_removing_context);
                    let bin_ident = memory_removing_context.bin_value_ident();
                    let memory_ident = memory_removing_context.memory_value_ident();
                    let bin_merger = quote!(|#arg_ident, #memory_ident| {
                        let #current_bin_ident: Option<#bin_type> = None;
                        let updating_bin = arg.map_over_inner(|#arg_ident| #bin_merger_expr);
                        if let Some(updating_bin) = updating_bin {
                            match updating_bin {
                                arroyo_types::UpdatingData::Retract(retract) => {
                                    let #bin_ident = retract;
                                    let #memory_ident = #memory_ident.expect(&format!("retracting means there should be state for {:?}", retract)).clone();
                                    #memory_remove
                                },
                                arroyo_types::UpdatingData::Update { old, new } => {
                                    let #memory_ident = #memory_ident.expect("retracting means there should be state").clone();
                                    let #bin_ident = old;
                                    let #memory_ident = #memory_remove;
                                    let #bin_ident = new;
                                    Some(#memory_add)
                                },
                                arroyo_types::UpdatingData::Append(append) => {
                                    let #bin_ident = append;
                                    let #memory_ident = #memory_ident.cloned();
                                    Some(#memory_add)
                                }
                            }
                        } else {
                            #memory_ident.map(|mem| mem.clone())
                        }
                    });

                    arroyo_datastream::Operator::NonWindowAggregator(NonWindowAggregator {
                        expiration: *expiration,
                        aggregator: memory_aggregate_closure.into_token_stream().to_string(),
                        bin_merger: bin_merger.into_token_stream().to_string(),
                        bin_type: quote!(#memory_type).to_string(),
                    })
                } else {
                    let bin_merger_context = ValueBinMergingContext::new();
                    let bin_type = bin_merger_context
                        .bin_syn_type(projection)
                        .into_token_stream()
                        .to_string();
                    let bin_merger = bin_merger_context
                        .compile_closure_with_some_result(projection)
                        .into_token_stream()
                        .to_string();

                    let bin_aggregating_context = BinAggregatingContext::new();
                    let aggregator = bin_aggregating_context
                        .compile_non_windowed_closure(projection)
                        .into_token_stream()
                        .to_string();

                    arroyo_datastream::Operator::NonWindowAggregator(NonWindowAggregator {
                        expiration: *expiration,
                        aggregator,
                        bin_merger,
                        bin_type,
                    })
                }
            }
        }
    }

    fn get_all_types(&self) -> HashSet<StructDef> {
        let mut output_types = self.output_type.get_all_types();
        output_types.extend(self.output_type.get_all_types());
        // TODO: populate types only created within operators.
        match &self.operator {
            PlanOperator::JoinPairMerge(join_type, StructPair { left, right })
            | PlanOperator::JoinListMerge(join_type, StructPair { left, right }) => {
                output_types.insert(join_type.join_struct_type(left, right));
            }
            PlanOperator::FusedRecordTransform(fused_record_transform) => {
                fused_record_transform.output_types.iter().for_each(|t| {
                    output_types.extend(t.get_all_types());
                });
            }
            PlanOperator::SlidingAggregatingTopN {
                width: _,
                slide: _,
                aggregating_projection,
                order_by: _,
                partition_projection,
                converting_projection,
                max_elements: _,
            } => {
                output_types.extend(aggregating_projection.output_struct().all_structs());
                output_types.extend(partition_projection.output_struct().all_structs());
                output_types.extend(converting_projection.output_struct().all_structs());
            }
            PlanOperator::NonWindowAggregate {
                input_is_update: _,
                expiration: _,
                projection,
            } => {
                output_types.extend(projection.output_struct().all_structs());
            }
            _ => {}
        }
        output_types
    }
}

#[derive(Debug, Clone)]
pub struct PlanEdge {
    pub edge_type: EdgeType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanType {
    Unkeyed(StructDef),
    UnkeyedList(StructDef),
    Keyed {
        key: StructDef,
        value: StructDef,
    },
    KeyedPair {
        key: StructDef,
        left_value: StructDef,
        right_value: StructDef,
        join_type: JoinType,
    },
    KeyedListPair {
        key: StructDef,
        left_value: StructDef,
        right_value: StructDef,
    },
    KeyedLiteralTypeValue {
        key: Option<StructDef>,
        value: String,
    },
    Debezium {
        key: Option<StructDef>,
        value_type: String,
        values: Vec<StructDef>,
    },
    Updating(Box<PlanType>),
}

impl PlanType {
    fn as_syn_type(&self) -> syn::Type {
        match self {
            PlanType::Unkeyed(value) | PlanType::Keyed { key: _, value } => value.get_type(),
            PlanType::KeyedPair {
                key: _,
                left_value,
                right_value,
                join_type,
            } => {
                let left_type = left_value.get_type();
                let right_type = right_value.get_type();
                match join_type {
                    JoinType::Inner => parse_quote!((#left_type,#right_type)),
                    JoinType::Left => {
                        parse_quote!(arroyo_types::UpdatingData<(#left_type,Option<#right_type>)>)
                    }
                    JoinType::Right => {
                        parse_quote!(arroyo_types::UpdatingData<(Option<#left_type>,#right_type)>)
                    }
                    JoinType::Full => {
                        parse_quote!(arroyo_types::UpdatingData<(Option<#left_type>,Option<#right_type>)>)
                    }
                }
            }
            PlanType::KeyedListPair {
                key: _,
                left_value,
                right_value,
            } => {
                let left_type = left_value.get_type();
                let right_type = right_value.get_type();
                parse_quote!((Vec<#left_type>,Vec<#right_type>))
            }
            PlanType::KeyedLiteralTypeValue { key: _, value } => parse_str(value).unwrap(),
            PlanType::UnkeyedList(value) => {
                let value_type = value.get_type();
                parse_quote!(Vec<#value_type>)
            }
            PlanType::Debezium { value_type, .. } => {
                let v: Type = parse_str(value_type).unwrap();
                parse_quote!(arroyo_types::Debezium<#v>)
            }
            PlanType::Updating(inner_type) => {
                let inner_type = inner_type.as_syn_type();
                parse_quote!(arroyo_types::UpdatingData<#inner_type>)
            }
        }
    }

    fn key_type(&self) -> syn::Type {
        match self {
            PlanType::Unkeyed(_)
            | PlanType::UnkeyedList(_)
            | PlanType::KeyedLiteralTypeValue { key: None, .. }
            | PlanType::Debezium { key: None, .. } => parse_quote!(()),
            PlanType::Keyed { key, .. }
            | PlanType::KeyedPair { key, .. }
            | PlanType::KeyedLiteralTypeValue { key: Some(key), .. }
            | PlanType::KeyedListPair { key, .. }
            | PlanType::Debezium { key: Some(key), .. } => key.get_type(),
            PlanType::Updating(inner) => inner.key_type(),
        }
    }

    fn value_structs(&self) -> Vec<StructDef> {
        match self {
            PlanType::Unkeyed(v) => vec![v.clone()],
            PlanType::UnkeyedList(v) => vec![v.clone()],
            PlanType::Keyed { value, .. } => vec![value.clone()],
            PlanType::KeyedPair {
                left_value,
                right_value,
                ..
            }
            | PlanType::KeyedListPair {
                left_value,
                right_value,
                ..
            } => vec![left_value.clone(), right_value.clone()],
            PlanType::KeyedLiteralTypeValue { .. } => vec![],
            PlanType::Debezium { values, .. } => values.clone(),
            PlanType::Updating(v) => v.value_structs(),
        }
    }

    fn record_type(&self) -> syn::Type {
        let key = self.key_type();
        let value = self.as_syn_type();
        parse_quote!(arroyo_types::Record<#key,#value>)
    }

    fn get_key_struct_names(&self) -> Vec<String> {
        match self {
            PlanType::Unkeyed(_)
            | PlanType::UnkeyedList(_)
            | PlanType::KeyedLiteralTypeValue { key: None, .. }
            | PlanType::Debezium { key: None, .. } => vec![],
            PlanType::Keyed { key, .. }
            | PlanType::KeyedPair { key, .. }
            | PlanType::KeyedLiteralTypeValue { key: Some(key), .. }
            | PlanType::Debezium { key: Some(key), .. }
            | PlanType::KeyedListPair { key, .. } => key.all_names(),
            PlanType::Updating(inner) => inner.get_key_struct_names(),
        }
    }

    fn get_all_types(&self) -> HashSet<StructDef> {
        match self {
            PlanType::Unkeyed(value) | PlanType::UnkeyedList(value) => {
                value.all_structs().into_iter().collect()
            }
            PlanType::Keyed { key, value } => {
                let mut result = key.all_structs();
                result.extend(value.all_structs());
                result.into_iter().collect()
            }
            PlanType::KeyedPair {
                key,
                left_value,
                right_value,
                join_type: _,
            }
            | PlanType::KeyedListPair {
                key,
                left_value,
                right_value,
            } => {
                let mut result = key.all_structs();
                result.extend(left_value.all_structs());
                result.extend(right_value.all_structs());
                result.into_iter().collect()
            }
            PlanType::KeyedLiteralTypeValue { key, value: _ } => match key {
                Some(key) => key.all_structs().into_iter().collect(),
                None => HashSet::new(),
            },
            PlanType::Debezium { key, values, .. } => key
                .iter()
                .flat_map(|key| key.all_structs())
                .chain(values.iter().flat_map(|value| value.all_structs()))
                .collect(),
            PlanType::Updating(inner) => inner.get_all_types(),
        }
    }

    fn get_stream_edge(&self, edge_type: EdgeType) -> StreamEdge {
        let key_type = self.key_type();
        let value_type = self.as_syn_type();
        let key = quote!(#key_type).to_string();
        let value = quote!(#value_type).to_string();
        StreamEdge {
            key,
            value,
            typ: edge_type,
        }
    }

    fn with_key(&self, key: StructDef) -> Self {
        match self {
            PlanType::Unkeyed(value) | PlanType::Keyed { key: _, value } => PlanType::Keyed {
                key,
                value: value.clone(),
            },
            PlanType::UnkeyedList(_) => unreachable!(),
            PlanType::KeyedPair {
                key: _,
                left_value,
                right_value,
                join_type,
            } => PlanType::KeyedPair {
                key,
                left_value: left_value.clone(),
                right_value: right_value.clone(),
                join_type: join_type.clone(),
            },
            PlanType::KeyedListPair {
                key: _,
                left_value,
                right_value,
            } => PlanType::KeyedListPair {
                key,
                left_value: left_value.clone(),
                right_value: right_value.clone(),
            },
            PlanType::KeyedLiteralTypeValue { key: _, value } => PlanType::KeyedLiteralTypeValue {
                key: Some(key),
                value: value.clone(),
            },
            PlanType::Debezium {
                value_type, values, ..
            } => PlanType::Debezium {
                key: Some(key),
                value_type: value_type.clone(),
                values: values.clone(),
            },
            PlanType::Updating(inner) => PlanType::Updating(Box::new(inner.with_key(key))),
        }
    }

    fn with_value(&self, value: StructDef) -> PlanType {
        match self {
            PlanType::Unkeyed(_) => PlanType::Unkeyed(value),
            PlanType::UnkeyedList(_) => PlanType::UnkeyedList(value),
            PlanType::Keyed { .. } => PlanType::Unkeyed(value),
            PlanType::KeyedPair { .. } => unreachable!(),
            PlanType::KeyedListPair { .. } => unreachable!(),
            PlanType::KeyedLiteralTypeValue { .. } => unreachable!(),
            PlanType::Debezium { .. } => unreachable!(),
            PlanType::Updating(inner) => PlanType::Updating(Box::new(inner.with_value(value))),
        }
    }

    pub(crate) fn is_updating(&self) -> bool {
        matches!(self, PlanType::Updating(_))
    }
}

#[derive(Debug)]
pub struct PlanGraph {
    pub graph: DiGraph<PlanNode, PlanEdge>,
    pub types: HashSet<StructDef>,
    pub key_structs: HashSet<String>,
    pub sources: HashMap<String, NodeIndex>,
    pub named_tables: HashMap<String, NodeIndex>,
    pub sql_config: SqlConfig,
    pub saved_sources_used: Vec<i64>,
}

impl PlanGraph {
    pub fn new(sql_config: SqlConfig) -> Self {
        Self {
            graph: DiGraph::new(),
            types: HashSet::new(),
            key_structs: HashSet::new(),
            sources: HashMap::new(),
            named_tables: HashMap::new(),
            sql_config,
            saved_sources_used: vec![],
        }
    }

    pub fn find_used_udfs(&mut self, used_udfs: &mut HashSet<String>) {
        let accumulate_udfs = |ctx: &mut HashSet<String>, e: &mut Expression| match e {
            Expression::RustUdf(r) => {
                ctx.insert(r.name());
            }
            _ => {}
        };

        for node in self.graph.node_weights_mut() {
            match &mut node.operator {
                PlanOperator::Source(_, _) => {}
                PlanOperator::Watermark(_) => {}
                PlanOperator::RecordTransform(ref mut r) => {
                    r.expressions()
                        .for_each(|e| e.traverse_mut(used_udfs, &accumulate_udfs));
                }
                PlanOperator::FusedRecordTransform(ref mut f) => {
                    f.expressions.iter_mut().for_each(|e| {
                        e.expressions()
                            .for_each(|e| e.traverse_mut(used_udfs, &accumulate_udfs))
                    });
                }
                PlanOperator::Unkey => {}
                PlanOperator::WindowAggregate {
                    ref mut projection, ..
                } => {
                    projection.aggregates.iter_mut().for_each(|a| match a {
                        AggregateComputation::Builtin {
                            ref mut computation,
                            ..
                        } => {
                            computation
                                .producing_expression
                                .traverse_mut(used_udfs, &accumulate_udfs);
                        }
                        AggregateComputation::UDAF {
                            ref mut computation,
                            ..
                        } => {
                            used_udfs.insert(computation.name());
                            computation
                                .expressions()
                                .for_each(|e| e.traverse_mut(used_udfs, &accumulate_udfs));
                        }
                    });
                }
                PlanOperator::NonWindowAggregate {
                    ref mut projection, ..
                } => projection
                    .expressions()
                    .for_each(|e| e.traverse_mut(used_udfs, &accumulate_udfs)),
                PlanOperator::TumblingWindowTwoPhaseAggregator {
                    ref mut projection, ..
                } => projection
                    .expressions()
                    .for_each(|e| e.traverse_mut(used_udfs, &accumulate_udfs)),
                PlanOperator::SlidingWindowTwoPhaseAggregator {
                    ref mut projection, ..
                } => projection
                    .expressions()
                    .for_each(|e| e.traverse_mut(used_udfs, &accumulate_udfs)),
                PlanOperator::InstantJoin => {}
                PlanOperator::JoinWithExpiration { .. } => {}
                PlanOperator::JoinListMerge(_, _) => {}
                PlanOperator::JoinPairMerge(_, _) => {}
                PlanOperator::Flatten => {}
                PlanOperator::WindowFunction(w) => {
                    w.order_by
                        .iter_mut()
                        .map(|o| o.expression())
                        .for_each(|e| e.traverse_mut(used_udfs, &accumulate_udfs));
                }
                PlanOperator::TumblingLocalAggregator {
                    ref mut projection, ..
                } => projection
                    .expressions()
                    .for_each(|e| e.traverse_mut(used_udfs, &accumulate_udfs)),
                PlanOperator::SlidingAggregatingTopN {
                    ref mut aggregating_projection,
                    ref mut order_by,
                    ref mut partition_projection,
                    ref mut converting_projection,
                    ..
                } => {
                    aggregating_projection
                        .expressions()
                        .for_each(|e| e.traverse_mut(used_udfs, &accumulate_udfs));
                    order_by
                        .iter_mut()
                        .for_each(|e| e.expression().traverse_mut(used_udfs, &accumulate_udfs));
                    partition_projection
                        .expressions()
                        .for_each(|e| e.traverse_mut(used_udfs, &accumulate_udfs));
                    converting_projection
                        .expressions()
                        .for_each(|e| e.traverse_mut(used_udfs, &accumulate_udfs));
                }
                PlanOperator::TumblingTopN { .. } => {}
                PlanOperator::StreamOperator(_, _) => {}
                PlanOperator::ToDebezium => {}
                PlanOperator::FromDebezium => {}
                PlanOperator::FromUpdating => {}
                PlanOperator::Sink(_, _) => {}
            }
        }
    }

    pub fn add_sql_operator(&mut self, operator: SqlOperator) -> NodeIndex {
        match operator {
            SqlOperator::Source(source_operator) => self.add_sql_source(source_operator),
            SqlOperator::Aggregator(input, projection) => self.add_aggregator(input, projection),
            SqlOperator::JoinOperator(left, right, join_operator) => {
                self.add_join(left, right, join_operator)
            }
            SqlOperator::Window(input, window_operator) => self.add_window(input, window_operator),
            SqlOperator::RecordTransform(input, transform) => {
                self.add_record_transform(input, transform)
            }
            SqlOperator::Sink(name, sql_sink, input) => self.add_sql_sink(name, sql_sink, input),
            SqlOperator::NamedTable(name, input) => {
                let index = self.named_tables.get(&name);
                match index {
                    Some(index) => *index,
                    None => {
                        let index = self.add_sql_operator(*input);
                        self.named_tables.insert(name, index);
                        index
                    }
                }
            }
            SqlOperator::Union(inputs) => self.add_union(inputs),
        }
    }

    fn add_debezium_source(&mut self, source_operator: &SourceOperator) -> NodeIndex {
        let debezium_type = PlanType::Debezium {
            key: None,
            value_type: source_operator
                .source
                .struct_def
                .get_type()
                .to_token_stream()
                .to_string(),
            values: vec![source_operator.source.struct_def.clone()],
        };
        let source_node = self.insert_operator(
            PlanOperator::Source(source_operator.name.clone(), source_operator.source.clone()),
            debezium_type,
        );

        let debezium_edge = PlanEdge {
            edge_type: EdgeType::Forward,
        };

        let from_debezium_node = self.insert_operator(
            PlanOperator::FromDebezium,
            PlanType::Updating(Box::new(PlanType::Unkeyed(
                source_operator.source.struct_def.clone(),
            ))),
        );
        self.graph
            .add_edge(source_node, from_debezium_node, debezium_edge);
        from_debezium_node
    }

    fn add_sql_source(&mut self, source_operator: SourceOperator) -> NodeIndex {
        if let Some(node_index) = self.sources.get(&source_operator.name) {
            return *node_index;
        }
        if let Some(source_id) = source_operator.source.id {
            self.saved_sources_used.push(source_id);
        }
        let mut current_index = match source_operator.source.processing_mode {
            ProcessingMode::Update => self.add_debezium_source(&source_operator),
            ProcessingMode::Append => self.insert_operator(
                PlanOperator::Source(source_operator.name.clone(), source_operator.source.clone()),
                PlanType::Unkeyed(source_operator.source.struct_def.clone()),
            ),
        };
        if let Some(virtual_projection) = source_operator.virtual_field_projection {
            let virtual_plan_type = PlanType::Unkeyed(virtual_projection.output_struct());
            let virtual_index = self.insert_operator(
                PlanOperator::RecordTransform(RecordTransform::ValueProjection(virtual_projection)),
                virtual_plan_type,
            );
            let virtual_edge = PlanEdge {
                edge_type: EdgeType::Forward,
            };
            self.graph
                .add_edge(current_index, virtual_index, virtual_edge);
            current_index = virtual_index;
        }

        if let Some(timestamp_expression) = source_operator.timestamp_override {
            let timestamp_index = self.insert_operator(
                PlanOperator::RecordTransform(RecordTransform::TimestampAssignment(
                    timestamp_expression,
                )),
                self.get_plan_node(current_index).output_type.clone(),
            );
            let timestamp_edge = PlanEdge {
                edge_type: EdgeType::Forward,
            };
            self.graph
                .add_edge(current_index, timestamp_index, timestamp_edge);
            current_index = timestamp_index;
        }

        let strategy = if let Some(watermark_expression) = source_operator.watermark_column {
            let arg_ident = ValuePointerContext::new().variable_ident();
            let expression = watermark_expression.generate(&ValuePointerContext::new());
            let null_checked_expression = if watermark_expression
                .expression_type(&ValuePointerContext::new())
                .is_optional()
            {
                parse_quote!(#expression.unwrap_or_else(|| std::time::SystemTime::now()))
            } else {
                expression
            };

            arroyo_datastream::WatermarkStrategy::Expression {
                expression: quote!({
                   let #arg_ident = record.value.clone();
                   #null_checked_expression
                })
                .to_string(),
            }
        } else {
            arroyo_datastream::WatermarkStrategy::FixedLateness {
                max_lateness: Duration::from_secs(1),
            }
        };

        let watermark_operator = PlanOperator::Watermark(arroyo_datastream::PeriodicWatermark {
            period: Duration::from_secs(1),
            idle_time: source_operator.source.idle_time,
            strategy,
        });
        let watermark_index = self.insert_operator(
            watermark_operator,
            self.get_plan_node(current_index).output_type.clone(),
        );
        let watermark_edge = PlanEdge {
            edge_type: EdgeType::Forward,
        };
        self.graph
            .add_edge(current_index, watermark_index, watermark_edge);
        self.sources.insert(source_operator.name, watermark_index);
        watermark_index
    }

    pub fn insert_operator(&mut self, operator: PlanOperator, typ: PlanType) -> NodeIndex {
        let node = PlanNode {
            operator,
            output_type: typ,
        };
        self.graph.add_node(node)
    }

    fn add_aggregator(
        &mut self,
        input: Box<SqlOperator>,
        aggregate: crate::pipeline::AggregateOperator,
    ) -> NodeIndex {
        if !input.has_window() && matches!(aggregate.window, WindowType::Instant) {
            return self.add_updating_aggregator(input, aggregate);
        }
        let input_index = self.add_sql_operator(*input);

        let key_struct = aggregate.key.output_struct();
        let key_operator =
            PlanOperator::RecordTransform(RecordTransform::KeyProjection(aggregate.key));
        let key_index = self.insert_operator(
            key_operator,
            self.get_plan_node(input_index)
                .output_type
                .with_key(key_struct.clone()),
        );
        let key_edge = PlanEdge {
            edge_type: EdgeType::Forward,
        };

        self.graph.add_edge(input_index, key_index, key_edge);

        let aggregate_projection = aggregate.aggregating;
        let aggregate_struct = aggregate_projection.expression_type(&VecAggregationContext::new());

        let aggregate_operator = PlanOperator::WindowAggregate {
            window: aggregate.window,
            projection: aggregate_projection,
        };

        let aggregate_index = self.insert_operator(
            aggregate_operator,
            PlanType::Keyed {
                key: key_struct.clone(),
                value: aggregate_struct.clone(),
            },
        );

        let aggregate_edge = PlanEdge {
            edge_type: EdgeType::Shuffle,
        };

        self.graph
            .add_edge(key_index, aggregate_index, aggregate_edge);

        aggregate_index
    }

    fn add_join(
        &mut self,
        left: Box<SqlOperator>,
        right: Box<SqlOperator>,
        join_operator: crate::pipeline::JoinOperator,
    ) -> NodeIndex {
        let left_type = left.return_type();
        let right_type = right.return_type();
        // right now left and right either both have or don't have windows.
        let has_window = left.has_window();
        let join_type = join_operator.join_type;
        let left_index = self.add_sql_operator(*left);
        let right_index = self.add_sql_operator(*right);

        let key_struct = join_operator.left_key.output_struct();

        let left_key_operator =
            PlanOperator::RecordTransform(RecordTransform::KeyProjection(join_operator.left_key));
        let right_key_operator =
            PlanOperator::RecordTransform(RecordTransform::KeyProjection(join_operator.right_key));

        let left_key_index = self.insert_operator(
            left_key_operator,
            PlanType::Keyed {
                key: key_struct.clone(),
                value: left_type.clone(),
            },
        );
        let right_key_index = self.insert_operator(
            right_key_operator,
            PlanType::Keyed {
                key: key_struct.clone(),
                value: right_type.clone(),
            },
        );

        let left_key_edge = PlanEdge {
            edge_type: EdgeType::Forward,
        };
        let right_key_edge = PlanEdge {
            edge_type: EdgeType::Forward,
        };

        self.graph
            .add_edge(left_index, left_key_index, left_key_edge);
        self.graph
            .add_edge(right_index, right_key_index, right_key_edge);
        if has_window {
            self.add_post_window_join(
                left_key_index,
                right_key_index,
                key_struct,
                left_type,
                right_type,
                join_type,
            )
        } else {
            self.add_join_with_expiration(
                left_key_index,
                right_key_index,
                key_struct,
                left_type,
                right_type,
                join_type,
            )
        }
    }

    fn add_post_window_join(
        &mut self,
        left_index: NodeIndex,
        right_index: NodeIndex,
        key_struct: StructDef,
        left_struct: StructDef,
        right_struct: StructDef,
        join_type: JoinType,
    ) -> NodeIndex {
        let join_node = PlanOperator::InstantJoin;
        let join_node_output_type = PlanType::KeyedListPair {
            key: key_struct,
            left_value: left_struct.clone(),
            right_value: right_struct.clone(),
        };
        let join_node_index = self.insert_operator(join_node, join_node_output_type);

        let left_join_edge = PlanEdge {
            edge_type: EdgeType::ShuffleJoin(0),
        };
        let right_join_edge = PlanEdge {
            edge_type: EdgeType::ShuffleJoin(1),
        };
        self.graph
            .add_edge(left_index, join_node_index, left_join_edge);
        self.graph
            .add_edge(right_index, join_node_index, right_join_edge);

        let merge_type = join_type.output_struct(&left_struct, &right_struct);
        let merge_operator = PlanOperator::JoinListMerge(
            join_type,
            StructPair {
                left: left_struct,
                right: right_struct,
            },
        );
        let merge_index =
            self.insert_operator(merge_operator, PlanType::UnkeyedList(merge_type.clone()));

        let merge_edge = PlanEdge {
            edge_type: EdgeType::Forward,
        };

        self.graph
            .add_edge(join_node_index, merge_index, merge_edge);

        let flatten_operator = PlanOperator::Flatten;
        let flatten_index = self.insert_operator(flatten_operator, PlanType::Unkeyed(merge_type));
        let flatten_edge = PlanEdge {
            edge_type: EdgeType::Forward,
        };
        self.graph
            .add_edge(merge_index, flatten_index, flatten_edge);

        flatten_index
    }
    fn add_join_with_expiration(
        &mut self,
        left_index: NodeIndex,
        right_index: NodeIndex,
        key_struct: StructDef,
        left_struct: StructDef,
        right_struct: StructDef,
        join_type: JoinType,
    ) -> NodeIndex {
        let join_node = PlanOperator::JoinWithExpiration {
            left_expiration: Duration::from_secs(24 * 60 * 60),
            right_expiration: Duration::from_secs(24 * 60 * 60),
            join_type: join_type.clone(),
        };
        let join_node_output_type = PlanType::KeyedPair {
            key: key_struct.clone(),
            left_value: left_struct.clone(),
            right_value: right_struct.clone(),
            join_type: join_type.clone(),
        };
        let join_node_index = self.insert_operator(join_node, join_node_output_type);

        let left_join_edge = PlanEdge {
            edge_type: EdgeType::ShuffleJoin(0),
        };
        let right_join_edge = PlanEdge {
            edge_type: EdgeType::ShuffleJoin(1),
        };
        self.graph
            .add_edge(left_index, join_node_index, left_join_edge);
        self.graph
            .add_edge(right_index, join_node_index, right_join_edge);

        let merge_type = join_type.output_struct(&left_struct, &right_struct);
        let merge_operator = PlanOperator::JoinPairMerge(
            join_type.clone(),
            StructPair {
                left: left_struct,
                right: right_struct,
            },
        );
        let merge_output_type = match join_type {
            JoinType::Inner => PlanType::Unkeyed(merge_type),
            JoinType::Left | JoinType::Right | JoinType::Full => {
                PlanType::Updating(Box::new(PlanType::Keyed {
                    key: key_struct,
                    value: merge_type,
                }))
            }
        };
        let merge_index = self.insert_operator(merge_operator, merge_output_type);

        let merge_edge = PlanEdge {
            edge_type: EdgeType::Forward,
        };

        self.graph
            .add_edge(join_node_index, merge_index, merge_edge);
        merge_index
    }

    fn add_window(
        &mut self,
        input: Box<SqlOperator>,
        window_operator: crate::pipeline::SqlWindowOperator,
    ) -> NodeIndex {
        let input_type = input.return_type();
        let window_type = if input.has_window() {
            WindowType::Instant
        } else {
            window_operator.window_type
        };
        let input_index = self.add_sql_operator(*input);
        let mut result_type = input_type.clone();
        result_type.fields.push(StructField::new(
            window_operator.field_name.clone(),
            None,
            TypeDef::DataType(DataType::UInt64, false),
        ));
        let partition_struct = window_operator.partition.output_struct();

        let partition_key_node = PlanOperator::RecordTransform(RecordTransform::KeyProjection(
            window_operator.partition,
        ));
        let partition_key_index = self.insert_operator(
            partition_key_node,
            PlanType::Keyed {
                key: partition_struct.clone(),
                value: input_type,
            },
        );
        let partition_key_edge = PlanEdge {
            edge_type: EdgeType::Forward,
        };

        self.graph
            .add_edge(input_index, partition_key_index, partition_key_edge);

        let window_function_node = PlanOperator::WindowFunction(WindowFunctionOperator {
            window_function: window_operator.window_fn,
            order_by: window_operator.order_by,
            window_type,
            result_struct: result_type.clone(),
            field_name: window_operator.field_name,
        });
        let window_function_index = self.insert_operator(
            window_function_node,
            PlanType::Keyed {
                key: partition_struct,
                value: result_type.clone(),
            },
        );
        let window_function_edge = PlanEdge {
            edge_type: EdgeType::Shuffle,
        };
        self.graph.add_edge(
            partition_key_index,
            window_function_index,
            window_function_edge,
        );
        let unkey_index =
            self.insert_operator(PlanOperator::Unkey, PlanType::Unkeyed(result_type.clone()));
        self.graph.add_edge(
            window_function_index,
            unkey_index,
            PlanEdge {
                edge_type: EdgeType::Forward,
            },
        );
        unkey_index
    }

    fn add_record_transform(
        &mut self,
        input: Box<SqlOperator>,
        transform: RecordTransform,
    ) -> NodeIndex {
        let input_index = self.add_sql_operator(*input);

        let plan_node = PlanNode::from_record_transform(transform, self.get_plan_node(input_index));

        let plan_node_index = self.graph.add_node(plan_node);
        let edge = PlanEdge {
            edge_type: EdgeType::Forward,
        };
        self.graph.add_edge(input_index, plan_node_index, edge);
        plan_node_index
    }

    fn get_plan_node(&self, node_index: NodeIndex) -> &PlanNode {
        self.graph.node_weight(node_index).unwrap()
    }

    fn add_sql_sink(
        &mut self,
        name: String,
        sql_sink: crate::external::SqlSink,
        input: Box<SqlOperator>,
    ) -> NodeIndex {
        let input_index = self.add_sql_operator(*input);
        let input_node = self.get_plan_node(input_index);
        if let PlanType::Updating(inner) = &input_node.output_type {
            let value_type = inner.as_syn_type();
            let debezium_type = PlanType::Debezium {
                key: None,
                value_type: quote!(#value_type).to_string(),
                values: inner.value_structs(),
            };
            let debezium_index =
                self.insert_operator(PlanOperator::ToDebezium, debezium_type.clone());

            let edge = PlanEdge {
                edge_type: EdgeType::Forward,
            };
            self.graph.add_edge(input_index, debezium_index, edge);

            let plan_node = PlanOperator::Sink(name, sql_sink);
            let plan_node_index = self.insert_operator(plan_node, debezium_type);

            let debezium_edge = PlanEdge {
                edge_type: EdgeType::Forward,
            };

            self.graph
                .add_edge(debezium_index, plan_node_index, debezium_edge);
            plan_node_index
        } else if matches!(sql_sink.updating_type, SinkUpdateType::Force) {
            let value_type = input_node.output_type.as_syn_type();
            let debezium_type = PlanType::Debezium {
                key: None,
                value_type: quote!(#value_type).to_string(),
                values: input_node.output_type.value_structs(),
            };
            let debezium_index =
                self.insert_operator(PlanOperator::ToDebezium, debezium_type.clone());
            let edge = PlanEdge {
                edge_type: EdgeType::Forward,
            };
            self.graph.add_edge(input_index, debezium_index, edge);

            let plan_node = PlanOperator::Sink(name, sql_sink);
            let plan_node_index = self.insert_operator(plan_node, debezium_type);

            let debezium_edge = PlanEdge {
                edge_type: EdgeType::Forward,
            };

            self.graph
                .add_edge(debezium_index, plan_node_index, debezium_edge);
            plan_node_index
        } else {
            let plan_node = PlanOperator::Sink(name, sql_sink);
            let plan_node_index = self.insert_operator(plan_node, input_node.output_type.clone());
            let edge = PlanEdge {
                edge_type: EdgeType::Forward,
            };
            self.graph.add_edge(input_index, plan_node_index, edge);
            plan_node_index
        }
    }

    fn add_updating_aggregator(
        &mut self,
        input: Box<SqlOperator>,
        aggregate: crate::pipeline::AggregateOperator,
    ) -> NodeIndex {
        let input_index = self.add_sql_operator(*input);

        let input_node = self.get_plan_node(input_index);
        let input_updating = input_node.output_type.is_updating();

        let key_struct = aggregate.key.output_struct();
        let key_operator =
            PlanOperator::RecordTransform(RecordTransform::KeyProjection(aggregate.key));
        let key_index = self.insert_operator(
            key_operator,
            self.get_plan_node(input_index)
                .output_type
                .with_key(key_struct.clone()),
        );
        let key_edge = PlanEdge {
            edge_type: EdgeType::Forward,
        };
        self.graph.add_edge(input_index, key_index, key_edge);
        let aggregate_projection = aggregate.aggregating;

        let aggregate_struct = aggregate_projection.expression_type(&VecAggregationContext::new());
        let aggregate_operator = PlanOperator::NonWindowAggregate {
            input_is_update: input_updating,
            expiration: Duration::from_secs(60 * 60 * 24),
            projection: aggregate_projection.clone().try_into().unwrap(),
        };

        let aggregate_index = self.insert_operator(
            aggregate_operator,
            PlanType::Updating(Box::new(PlanType::Keyed {
                key: key_struct.clone(),
                value: aggregate_struct.clone(),
            })),
        );
        let aggregate_edge = PlanEdge {
            edge_type: EdgeType::Shuffle,
        };
        self.graph
            .add_edge(key_index, aggregate_index, aggregate_edge);

        // unkey the operator
        let unkey_operator = PlanOperator::Unkey;
        let unkey_index = self.insert_operator(
            unkey_operator,
            PlanType::Updating(Box::new(PlanType::Unkeyed(aggregate_struct.clone()))),
        );
        let unkey_edge = PlanEdge {
            edge_type: EdgeType::Forward,
        };
        self.graph
            .add_edge(aggregate_index, unkey_index, unkey_edge);

        if input_updating || !aggregate_projection.aggregates.is_empty() {
            unkey_index
        } else {
            // this is a select distinct, without any aggregates -- so there's no possibility
            // of retractions and we can change it back to non-updating
            let index = self.insert_operator(
                PlanOperator::FromUpdating,
                PlanType::Unkeyed(aggregate_struct),
            );
            let edge = PlanEdge {
                edge_type: EdgeType::Forward,
            };

            self.graph.add_edge(unkey_index, index, edge);

            index
        }
    }

    fn add_union(&mut self, inputs: Vec<SqlOperator>) -> NodeIndex {
        let input_node_indices = inputs
            .into_iter()
            .map(|input| (input.return_type(), self.add_sql_operator(input)))
            .collect::<Vec<_>>();
        let (first_struct, first_index) = input_node_indices[0].clone();
        let first_input = self.get_plan_node(first_index);
        let union_node = self.insert_operator(PlanOperator::Unkey, first_input.output_type.clone());
        for (input_struct, mut input_index) in input_node_indices {
            if first_struct != input_struct {
                // create a record transformation from input_struct to first struct.
                // We've validated the lengths and types
                let fields = first_struct
                    .fields
                    .iter()
                    .zip(input_struct.fields.iter())
                    .map(|(f1, f2)| {
                        (
                            Column {
                                relation: f1.alias.clone(),
                                name: f1.name(),
                            },
                            Expression::Column(ColumnExpression::new(f2.clone())),
                        )
                    })
                    .collect();
                let projection = RecordTransform::ValueProjection(Projection {
                    fields,
                    format: None,
                });
                let input_node = self.get_plan_node(input_index);
                let plan_node = PlanNode::from_record_transform(projection, input_node);
                let edge = PlanEdge {
                    edge_type: EdgeType::Forward,
                };
                let conversion_index = self.graph.add_node(plan_node);
                self.graph.add_edge(input_index, conversion_index, edge);
                input_index = conversion_index;
            }
            // now merge into union node.
            let edge = PlanEdge {
                edge_type: EdgeType::Forward,
            };
            self.graph.add_edge(input_index, union_node, edge);
        }
        union_node
    }
}

impl From<PlanGraph> for DiGraph<StreamNode, StreamEdge> {
    fn from(val: PlanGraph) -> Self {
        val.graph.map(
            |index: NodeIndex, node| node.into_stream_node(index.index(), &val.sql_config),
            |index, edge| {
                let source_index = val.graph.edge_endpoints(index).unwrap().0;
                let source_node = val.graph.node_weight(source_index).unwrap();
                source_node
                    .output_type
                    .get_stream_edge(edge.edge_type.clone())
            },
        )
    }
}

pub fn get_program(
    mut plan_graph: PlanGraph,
    schema_provider: ArroyoSchemaProvider,
) -> Result<(Program, Vec<i64>)> {
    optimize(&mut plan_graph.graph);

    let mut key_structs = HashSet::new();
    let sources = plan_graph.saved_sources_used.clone();
    plan_graph.graph.node_weights().for_each(|node| {
        let key_names = node.output_type.get_key_struct_names();
        key_structs.extend(key_names);
    });

    let mut used_udfs = HashSet::new();
    plan_graph.find_used_udfs(&mut used_udfs);

    // find all types that are produced by a source or consumed by a sink
    let connector_types: HashSet<_> = plan_graph
        .graph
        .externals(Direction::Incoming)
        .chain(
            plan_graph
                .graph
                .externals(Direction::Outgoing)
                .flat_map(|idx| {
                    plan_graph
                        .graph
                        .neighbors_directed(idx, Direction::Incoming)
                }),
        )
        .flat_map(|node| plan_graph.graph.node_weight(node).unwrap().get_all_types())
        .flat_map(|s| s.all_structs_including_named())
        .map(|t| t.struct_name())
        .collect();

    let types: HashSet<_> = plan_graph
        .graph
        .node_weights()
        .flat_map(|node| node.get_all_types())
        .collect();

    let mut other_defs: Vec<_> = types
        .iter()
        .map(|s| s.def(key_structs.contains(&s.struct_name())))
        .collect();

    let all_types: HashSet<_> = types
        .iter()
        .flat_map(|s| s.all_structs_including_named())
        .collect();

    other_defs.extend(
        all_types
            .iter()
            .filter(|t| connector_types.contains(&t.struct_name()))
            .map(|s| s.generate_serializer_items().to_string()),
    );

    other_defs.extend(
        schema_provider
            .source_defs
            .into_iter()
            .filter(|(k, _)| plan_graph.sources.contains_key(k))
            .map(|(_, v)| v),
    );

    // add only the used udfs to the program
    let mut udfs: HashMap<String, ProgramUdf> = HashMap::new();
    used_udfs.iter().for_each(|u| {
        udfs.insert(
            u.clone(),
            ProgramUdf {
                name: u.clone(),
                definition: schema_provider.udf_defs.get(u).unwrap().def.clone(),
            },
        );
    });

    let graph: DiGraph<StreamNode, StreamEdge> = plan_graph.into();

    Ok((
        Program {
            // For now, we don't export any types from SQL into WASM, as there is a problem with doing serde
            // in wasm
            types: vec![],
            other_defs,
            udfs: udfs.values().cloned().collect(),
            graph,
        },
        sources,
    ))
}
