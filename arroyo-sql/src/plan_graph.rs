use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use arrow_schema::DataType;
use arroyo_datastream::{
    EdgeType, ExpressionReturnType, Operator, Program, SlidingAggregatingTopN,
    SlidingWindowAggregator, StreamEdge, StreamNode, TumblingTopN, TumblingWindowAggregator,
    WatermarkType, WindowAgg, WindowType,
};
use petgraph::graph::{DiGraph, NodeIndex};
use quote::quote;
use syn::{parse_quote, parse_str};

use crate::{
    expressions::SortExpression,
    external::{SqlSink, SqlSource},
    operators::{AggregateProjection, GroupByKind, Projection, TwoPhaseAggregateProjection},
    optimizations::optimize,
    pipeline::{
        AggregatingStrategy, JoinType, MethodCompiler, RecordTransform, SourceOperator,
        SqlOperator, WindowFunction,
    },
    types::{StructDef, StructField, StructPair, TypeDef},
    ArroyoSchemaProvider, SqlConfig,
};
use anyhow::Result;

#[derive(Debug, Clone)]
pub enum PlanOperator {
    Source(String, SqlSource),
    Watermark(WatermarkType),
    RecordTransform(RecordTransform),
    FusedRecordTransform(FusedRecordTransform),
    Unkey,
    WindowAggregate {
        window: WindowType,
        projection: AggregateProjection,
    },
    WindowMerge {
        key_struct: StructDef,
        value_struct: StructDef,
        group_by_kind: GroupByKind,
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
        group_by_projection: Projection,
        group_by_kind: GroupByKind,
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
        for expression in &self.expressions {
            let RecordTransform::Filter(predicate)= expression else {
                panic!("FusedRecordTransform.to_predicate_operator() called on non-predicate expression");
            };
            predicates.push(predicate.to_syn_expression());
        }
        let predicate: syn::Expr = parse_quote!( {
            let arg = &record.value;
            #(#predicates)&&*
        });
        Operator::ExpressionOperator {
            name: "fused".to_string(),
            expression: quote!(#predicate).to_string(),
            return_type: ExpressionReturnType::Predicate,
        }
    }

    fn to_record_operator(&self) -> Operator {
        let mut record_expressions: Vec<syn::Stmt> = Vec::new();
        for i in 0..self.expressions.len() {
            let expression = &self.expressions[i];
            let output_type = &self.output_types[i];
            match expression {
                RecordTransform::ValueProjection(projection) => {
                    let expr = projection.to_syn_expression();
                    let record_type = output_type.record_type();
                    record_expressions.push(parse_quote!(

                            let record: #record_type = { let arg = &record.value;
                                arroyo_types::Record {
                                timestamp: record.timestamp,
                                key: None,
                                value: #expr
                        }
                    };
                    ));
                }
                RecordTransform::KeyProjection(projection) => {
                    let expr = projection.to_syn_expression();
                    let record_type = output_type.record_type();
                    record_expressions.push(parse_quote!(

                            let record: #record_type = { let arg = &record.value;
                                arroyo_types::Record {
                                timestamp: record.timestamp,
                                key: Some(#expr),
                                value: record.value.clone()
                        }
                    };
                    ));
                }
                RecordTransform::TimestampAssignment(timestamp_expression) => {
                    let expr = timestamp_expression.to_syn_expression();
                    let record_type = output_type.record_type();
                    record_expressions.push(parse_quote!(

                            let record: #record_type = { let arg = &record.value;
                                arroyo_types::Record {
                                timestamp: #expr,
                                key: record.key.clone(),
                                value: record.value.clone()
                        }
                    };
                    ));
                }
                RecordTransform::Filter(_) => unreachable!(),
            }
        }
        let combined: syn::Expr = parse_quote!({
            #(#record_expressions)*
            record
        });
        Operator::ExpressionOperator {
            name: "fused".to_string(),
            expression: quote!(#combined).to_string(),
            return_type: ExpressionReturnType::Record,
        }
    }

    fn to_optional_record_operator(&self) -> Operator {
        let mut record_expressions: Vec<syn::Stmt> = Vec::new();
        for i in 0..self.expressions.len() {
            let expression = &self.expressions[i];
            let output_type = &self.output_types[i];
            match expression {
                RecordTransform::ValueProjection(projection) => {
                    let expr = projection.to_syn_expression();
                    let record_type = output_type.record_type();
                    record_expressions.push(parse_quote!(

                            let record: #record_type = { let arg = &record.value;
                                arroyo_types::Record {
                                timestamp: record.timestamp,
                                key: None,
                                value: #expr
                        }
                    };
                    ));
                }
                RecordTransform::KeyProjection(projection) => {
                    let expr = projection.to_syn_expression();
                    let record_type = output_type.record_type();
                    record_expressions.push(parse_quote!(

                            let record: #record_type = { let arg = &record.value;
                                arroyo_types::Record {
                                timestamp: record.timestamp,
                                key: Some(#expr),
                                value: record.value.clone()
                        }
                    };
                    ));
                }
                RecordTransform::Filter(predicate) => {
                    let expr = predicate.to_syn_expression();
                    record_expressions.push(parse_quote!(
                        if !{let arg = &record.value;#expr} {
                            return None;
                        }
                    ));
                }
                RecordTransform::TimestampAssignment(timestamp_expression) => {
                    let expr = timestamp_expression.to_syn_expression();
                    let record_type = output_type.record_type();
                    record_expressions.push(parse_quote!(

                            let record: #record_type = { let arg = &record.value;
                                arroyo_types::Record {
                                timestamp: #expr,
                                key: record.key.clone(),
                                value: record.value.clone()
                        }
                    };
                    ));
                }
            }
        }
        let combined: syn::Expr = parse_quote!({
            #(#record_expressions)*
            Some(record)
        });
        Operator::ExpressionOperator {
            name: "fused".to_string(),
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
        let operator = self.to_operator(sql_config);
        StreamNode {
            operator_id: name,
            parallelism: sql_config.default_parallelism,
            operator,
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
            PlanOperator::WindowMerge { .. } => "window_merge".to_string(),
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
        }
    }

    fn to_operator(&self, sql_config: &SqlConfig) -> Operator {
        match &self.operator {
            PlanOperator::Source(_name, source) => source.get_operator(sql_config),
            PlanOperator::Watermark(watermark) => Operator::Watermark(watermark.clone()),
            PlanOperator::RecordTransform(record_transform) => record_transform.as_operator(),
            PlanOperator::WindowAggregate { window, projection } => {
                let aggregate_expr = projection.to_syn_expression();
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
            PlanOperator::WindowMerge {
                key_struct,
                value_struct,
                group_by_kind,
            } => {
                let merge_expr = group_by_kind.to_syn_expression(key_struct, value_struct);
                let merge_struct_type =
                    SqlOperator::merge_struct_type(key_struct, value_struct).get_type();
                let expression: syn::Expr = parse_quote!(
                    {
                        let aggregate = record.value.clone();
                        let key = record.key.clone().unwrap();
                        let timestamp = record.timestamp.clone();
                        let arg = #merge_struct_type { key, aggregate , timestamp};
                        let value = #merge_expr;
                        arroyo_types::Record {
                            timestamp: record.timestamp,
                            key: None,
                            value,
                        }
                    }
                );
                Operator::ExpressionOperator {
                    name: "aggregation".to_string(),
                    expression: quote!(#expression).to_string(),
                    return_type: arroyo_datastream::ExpressionReturnType::Record,
                }
            }
            PlanOperator::TumblingWindowTwoPhaseAggregator {
                tumble_width,
                projection,
            } => {
                let aggregate_expr = projection.tumbling_aggregation_syn_expression();
                let bin_merger = projection.bin_merger_syn_expression();
                let bin_type = projection.bin_type();
                arroyo_datastream::Operator::TumblingWindowAggregator(TumblingWindowAggregator {
                    width: *tumble_width,
                    aggregator: quote!(|arg| {#aggregate_expr}).to_string(),
                    bin_merger: quote!(|arg, current_bin| {#bin_merger}).to_string(),
                    bin_type: quote!(#bin_type).to_string(),
                })
            }
            PlanOperator::SlidingWindowTwoPhaseAggregator {
                width,
                slide,
                projection,
            } => {
                let aggregate_expr = projection.sliding_aggregation_syn_expression();
                let bin_merger = projection.bin_merger_syn_expression();
                let in_memory_add = projection.memory_add_syn_expression();
                let in_memory_remove = projection.memory_remove_syn_expression();
                let bin_type = projection.bin_type();
                let mem_type = projection.memory_type();
                arroyo_datastream::Operator::SlidingWindowAggregator(SlidingWindowAggregator {
                    width: *width,
                    slide: *slide,
                    aggregator: quote!(|arg| {#aggregate_expr}).to_string(),
                    bin_merger: quote!(|arg, current_bin| {#bin_merger}).to_string(),
                    in_memory_add: quote!(|current, bin_value| {#in_memory_add}).to_string(),
                    in_memory_remove: quote!(|current, bin_value| {#in_memory_remove}).to_string(),
                    bin_type: quote!(#bin_type).to_string(),
                    mem_type: quote!(#mem_type).to_string(),
                })
            }
            PlanOperator::InstantJoin => Operator::WindowJoin {
                window: WindowType::Instant,
            },
            PlanOperator::JoinWithExpiration {
                left_expiration,
                right_expiration,
                join_type: _,
            } => Operator::JoinWithExpiration {
                left_expiration: *left_expiration,
                right_expiration: *right_expiration,
            },
            PlanOperator::JoinListMerge(join_type, struct_pair) => {
                let merge_struct =
                    join_type.join_struct_type(&struct_pair.left, &struct_pair.right);
                let merge_expr =
                    join_type.merge_syn_expression(&struct_pair.left, &struct_pair.right);
                MethodCompiler::join_merge_operator(
                    "join_merge",
                    join_type.clone(),
                    merge_struct.get_type(),
                    merge_expr,
                )
                .unwrap()
            }
            PlanOperator::JoinPairMerge(join_type, struct_pair) => {
                let merge_struct =
                    join_type.join_struct_type(&struct_pair.left, &struct_pair.right);
                let merge_expr =
                    join_type.merge_syn_expression(&struct_pair.left, &struct_pair.right);
                assert!(*join_type == JoinType::Inner);
                MethodCompiler::merge_pair_operator(
                    "join_merge",
                    merge_struct.get_type(),
                    merge_expr,
                )
                .unwrap()
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

                let sort = if order_by.len() > 0 {
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
                let bin_merger = projection.bin_merger_syn_expression();
                let bin_type = projection.bin_type();
                arroyo_datastream::Operator::TumblingWindowAggregator(TumblingWindowAggregator {
                    width: *width,
                    aggregator: quote!(|arg| { arg.clone() }).to_string(),
                    bin_merger: quote!(|arg, current_bin| {#bin_merger}).to_string(),
                    bin_type: quote!(#bin_type).to_string(),
                })
            }
            PlanOperator::SlidingAggregatingTopN {
                width,
                slide,
                aggregating_projection,
                group_by_projection,
                group_by_kind,
                order_by,
                partition_projection,
                converting_projection,
                max_elements,
            } => {
                let bin_type = aggregating_projection.bin_type();
                let bin_merger = aggregating_projection.combine_bin_syn_expr();
                let in_memory_add = aggregating_projection.memory_add_syn_expression();
                let in_memory_remove = aggregating_projection.memory_remove_syn_expression();
                let aggregate_expr = aggregating_projection.sliding_aggregation_syn_expression();
                let mem_type = aggregating_projection.memory_type();

                let sort_tuple = SortExpression::sort_tuple_type(order_by);
                let sort_key_type = quote!(#sort_tuple).to_string();

                let partition_function = partition_projection.to_syn_expression();
                let projection_expr = converting_projection.to_syn_expression();

                let sort_tokens = SortExpression::sort_tuple_expression(order_by);

                let aggregate_struct = aggregating_projection.output_struct();
                let key_struct = group_by_projection.output_struct();
                let merge_struct = SqlOperator::merge_struct_type(&key_struct, &aggregate_struct);
                let merge_expr = group_by_kind.to_syn_expression(&key_struct, &aggregate_struct);
                let merge_struct_ident = merge_struct.get_type();

                let extractor = quote!(
                    |key, arg| {
                        let key = key.clone();
                        let arg = #merge_struct_ident{key, aggregate: { #aggregate_expr}, timestamp: std::time::UNIX_EPOCH};
                        let arg = #merge_expr;
                        let arg = #projection_expr;

                        #sort_tokens
                    }
                ).to_string();

                let aggregator = quote!(|timestamp, key, aggregate_value|
                    {
                        let key = key.clone();
                        let arg = #merge_struct_ident{key, aggregate: {let arg = aggregate_value; #aggregate_expr}, timestamp};
                        let arg = #merge_expr;
                        #projection_expr
                    }
                ).to_string();
                let operator =
                    arroyo_datastream::Operator::SlidingAggregatingTopN(SlidingAggregatingTopN {
                        width: *width,
                        slide: *slide,
                        bin_merger: quote!(|arg, current_bin| {#bin_merger}).to_string(),
                        in_memory_add: quote!(|current, bin_value| {#in_memory_add}).to_string(),
                        in_memory_remove: quote!(|current, bin_value| {#in_memory_remove})
                            .to_string(),
                        partitioning_func: quote!(|arg| {#partition_function}).to_string(),
                        extractor,
                        aggregator,
                        bin_type: quote!(#bin_type).to_string(),
                        mem_type: quote!(#mem_type).to_string(),
                        sort_key_type,
                        max_elements: *max_elements,
                    });
                println!("sliding logical plan operator {:#?}", self);
                println!("sliding physical plan operator {:#?}", operator);
                operator
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
            PlanOperator::Sink(_sink_name, sql_sink) => {
                match &sql_sink.sink_config {
                    arroyo_datastream::SinkConfig::Kafka {
                        bootstrap_servers,
                        topic,
                        client_configs,
                    } => {
                        arroyo_datastream::Operator::KafkaSink {
                            topic: topic.clone(),
                            // split by comma
                            bootstrap_servers: bootstrap_servers
                                .split(',')
                                .map(|s| s.to_string())
                                .collect(),
                            client_configs: client_configs.clone(),
                        }
                    }
                    arroyo_datastream::SinkConfig::Console => {
                        arroyo_datastream::Operator::ConsoleSink
                    }
                    arroyo_datastream::SinkConfig::File { directory } => {
                        arroyo_datastream::Operator::FileSink {
                            dir: directory.into(),
                        }
                    }
                    arroyo_datastream::SinkConfig::Grpc => arroyo_datastream::Operator::GrpcSink,
                    arroyo_datastream::SinkConfig::Null => arroyo_datastream::Operator::NullSink,
                }
            }
        }
    }

    fn get_all_types(&self) -> HashSet<StructDef> {
        let mut output_types = self.output_type.get_all_types();
        output_types.extend(self.output_type.get_all_types());
        // TODO: populate types only created within operators.
        match &self.operator {
            PlanOperator::WindowMerge {
                key_struct,
                value_struct,
                group_by_kind: _,
            } => {
                let merge_struct_type = SqlOperator::merge_struct_type(key_struct, value_struct);
                output_types.insert(merge_struct_type);
            }
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
                group_by_projection,
                group_by_kind,
                order_by: _,
                partition_projection,
                converting_projection,
                max_elements: _,
            } => {
                output_types.extend(aggregating_projection.output_struct().all_structs());
                output_types.extend(group_by_projection.output_struct().all_structs());
                output_types.extend(partition_projection.output_struct().all_structs());
                output_types.extend(converting_projection.output_struct().all_structs());
                output_types.extend(
                    converting_projection
                        .truncated_return_type(aggregating_projection.field_names.len())
                        .all_structs(),
                );

                let aggregate_struct = aggregating_projection.output_struct();
                let key_struct = group_by_projection.output_struct();
                let merge_struct = SqlOperator::merge_struct_type(&key_struct, &aggregate_struct);
                output_types.extend(
                    group_by_kind
                        .output_struct(&key_struct, &aggregate_struct)
                        .all_structs(),
                );
                output_types.extend(merge_struct.all_structs());
            }

            _ => {}
        }
        output_types
    }
}

#[derive(Debug, Clone)]
pub struct PlanEdge {
    pub edge_data_type: PlanType,
    pub edge_type: EdgeType,
}
impl PlanEdge {
    fn into_stream_edge(&self) -> StreamEdge {
        match &self.edge_data_type {
            PlanType::Unkeyed(value_struct) => {
                StreamEdge::unkeyed_edge(value_struct.struct_name(), self.edge_type.clone())
            }
            PlanType::Keyed { key, value } => StreamEdge::keyed_edge(
                key.struct_name(),
                value.struct_name(),
                self.edge_type.clone(),
            ),
            PlanType::KeyedPair {
                key,
                left_value,
                right_value,
            } => StreamEdge::keyed_edge(
                key.struct_name(),
                format!(
                    "({},{})",
                    left_value.struct_name(),
                    right_value.struct_name()
                ),
                self.edge_type.clone(),
            ),
            PlanType::KeyedListPair {
                key,
                left_value,
                right_value,
            } => StreamEdge::keyed_edge(
                key.struct_name(),
                format!(
                    "(Vec<{}>,Vec<{}>)",
                    left_value.struct_name(),
                    right_value.struct_name()
                ),
                self.edge_type.clone(),
            ),
            PlanType::KeyedLiteralTypeValue { key, value } => {
                StreamEdge::keyed_edge(key.struct_name(), value.clone(), self.edge_type.clone())
            }
            PlanType::UnkeyedList(value_struct) => StreamEdge::unkeyed_edge(
                format!("Vec<{}>", value_struct.struct_name()),
                self.edge_type.clone(),
            ),
        }
    }
}

#[derive(Debug, Clone)]
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
    },
    KeyedListPair {
        key: StructDef,
        left_value: StructDef,
        right_value: StructDef,
    },
    KeyedLiteralTypeValue {
        key: StructDef,
        value: String,
    },
}

impl PlanType {
    fn as_syn_type(&self) -> syn::Type {
        match self {
            PlanType::Unkeyed(value) | PlanType::Keyed { key: _, value } => value.get_type(),
            PlanType::KeyedPair {
                key: _,
                left_value,
                right_value,
            } => {
                let left_type = left_value.get_type();
                let right_type = right_value.get_type();
                parse_quote!((#left_type,#right_type))
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
        }
    }

    fn key_type(&self) -> syn::Type {
        match self {
            PlanType::Unkeyed(_) | PlanType::UnkeyedList(_) => parse_quote!(()),
            PlanType::Keyed { key, .. }
            | PlanType::KeyedPair { key, .. }
            | PlanType::KeyedLiteralTypeValue { key, .. }
            | PlanType::KeyedListPair { key, .. } => key.get_type(),
        }
    }

    fn record_type(&self) -> syn::Type {
        let key = self.key_type();
        let value = self.as_syn_type();
        parse_quote!(arroyo_types::Record<#key,#value>)
    }

    fn get_key_struct_names(&self) -> Vec<String> {
        match self {
            PlanType::Unkeyed(_) | PlanType::UnkeyedList(_) => vec![],
            PlanType::Keyed { key, .. }
            | PlanType::KeyedPair { key, .. }
            | PlanType::KeyedLiteralTypeValue { key, .. }
            | PlanType::KeyedListPair { key, .. } => key.all_names(),
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
            PlanType::KeyedLiteralTypeValue { key, value: _ } => {
                key.all_structs().into_iter().collect()
            }
        }
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
        }
    }

    fn add_sql_source(&mut self, source_operator: SourceOperator) -> NodeIndex {
        if let Some(node_index) = self.sources.get(&source_operator.name) {
            return *node_index;
        }
        if let Some(source_id) = source_operator.source.id {
            self.saved_sources_used.push(source_id);
        }
        let mut current_type = PlanType::Unkeyed(source_operator.source.struct_def.clone());
        let mut current_index = self.insert_operator(
            PlanOperator::Source(source_operator.name.clone(), source_operator.source.clone()),
            current_type.clone(),
        );
        if let Some(virtual_projection) = source_operator.virtual_field_projection {
            let virtual_plan_type = PlanType::Unkeyed(virtual_projection.output_struct());
            let virtual_index = self.insert_operator(
                PlanOperator::RecordTransform(RecordTransform::ValueProjection(virtual_projection)),
                virtual_plan_type.clone(),
            );
            let virtual_edge = PlanEdge {
                edge_data_type: current_type.clone(),
                edge_type: EdgeType::Forward,
            };
            self.graph
                .add_edge(current_index, virtual_index, virtual_edge);
            current_index = virtual_index;
            current_type = virtual_plan_type;
        }

        if let Some(timestamp_expression) = source_operator.timestamp_override {
            let timestamp_index = self.insert_operator(
                PlanOperator::RecordTransform(RecordTransform::TimestampAssignment(
                    timestamp_expression,
                )),
                current_type.clone(),
            );
            let timestamp_edge = PlanEdge {
                edge_data_type: current_type.clone(),
                edge_type: EdgeType::Forward,
            };
            self.graph
                .add_edge(current_index, timestamp_index, timestamp_edge);
            current_index = timestamp_index;
        }
        let watermark = if let Some(watermark_expression) = source_operator.watermark_column {
            let expression = watermark_expression.to_syn_expression();
            let null_checked_expression = if watermark_expression.nullable() {
                parse_quote!(#expression.unwrap_or_else(|| std::time::SystemTime::now()))
            } else {
                expression
            };

            arroyo_datastream::WatermarkType::Expression {
                period: Duration::from_secs(1),
                expression: quote!({
                   let arg = record.value.clone();
                   #null_checked_expression
                })
                .to_string(),
            }
        } else {
            arroyo_datastream::WatermarkType::FixedLateness {
                period: Duration::from_secs(1),
                max_lateness: Duration::from_secs(1),
            }
        };
        let watermark_operator = PlanOperator::Watermark(watermark);
        let watermark_index = self.insert_operator(watermark_operator, current_type.clone());
        let watermark_edge = PlanEdge {
            edge_data_type: current_type,
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
        let input_type = input.return_type();
        let output_type = aggregate.output_struct();
        let key_struct = aggregate.key.output_struct();
        let input_index = self.add_sql_operator(*input);
        let key_operator =
            PlanOperator::RecordTransform(RecordTransform::KeyProjection(aggregate.key));
        let key_index = self.insert_operator(
            key_operator,
            PlanType::Keyed {
                key: key_struct.clone(),
                value: input_type.clone(),
            },
        );
        let key_edge = PlanEdge {
            edge_data_type: PlanType::Unkeyed(input_type.clone()),
            edge_type: EdgeType::Forward,
        };
        self.graph.add_edge(input_index, key_index, key_edge);
        let AggregatingStrategy::AggregateProjection(aggregate_projection) = aggregate.aggregating else {
            panic!("two phase not supported here, make that after constructing the plan graph")
        };
        let aggregate_struct = aggregate_projection.output_struct();
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
            edge_data_type: PlanType::Keyed {
                key: key_struct.clone(),
                value: input_type,
            },
            edge_type: EdgeType::Shuffle,
        };
        self.graph
            .add_edge(key_index, aggregate_index, aggregate_edge);
        let merge_node = PlanOperator::WindowMerge {
            key_struct: key_struct.clone(),
            value_struct: aggregate_struct.clone(),
            group_by_kind: aggregate.merge,
        };
        let merge_index = self.insert_operator(
            merge_node,
            PlanType::Keyed {
                key: key_struct.clone(),
                value: output_type,
            },
        );
        let merge_edge = PlanEdge {
            edge_data_type: PlanType::Keyed {
                key: key_struct,
                value: aggregate_struct,
            },
            edge_type: EdgeType::Forward,
        };
        self.graph
            .add_edge(aggregate_index, merge_index, merge_edge);

        merge_index
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
            edge_data_type: PlanType::Unkeyed(left_type.clone()),
            edge_type: EdgeType::Forward,
        };
        let right_key_edge = PlanEdge {
            edge_data_type: PlanType::Unkeyed(right_type.clone()),
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
            key: key_struct.clone(),
            left_value: left_struct.clone(),
            right_value: right_struct.clone(),
        };
        let join_node_index = self.insert_operator(join_node, join_node_output_type.clone());

        let left_join_edge = PlanEdge {
            edge_data_type: PlanType::Keyed {
                key: key_struct.clone(),
                value: left_struct.clone(),
            },
            edge_type: EdgeType::ShuffleJoin(0),
        };
        let right_join_edge = PlanEdge {
            edge_data_type: PlanType::Keyed {
                key: key_struct,
                value: right_struct.clone(),
            },
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
            edge_data_type: join_node_output_type,
            edge_type: EdgeType::Forward,
        };

        self.graph
            .add_edge(join_node_index, merge_index, merge_edge);

        let flatten_operator = PlanOperator::Flatten;
        let flatten_index =
            self.insert_operator(flatten_operator, PlanType::Unkeyed(merge_type.clone()));
        let flatten_edge = PlanEdge {
            edge_data_type: PlanType::UnkeyedList(merge_type),
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
            join_type: JoinType::Inner,
        };
        let join_node_output_type = PlanType::KeyedPair {
            key: key_struct.clone(),
            left_value: left_struct.clone(),
            right_value: right_struct.clone(),
        };
        let join_node_index = self.insert_operator(join_node, join_node_output_type.clone());

        let left_join_edge = PlanEdge {
            edge_data_type: PlanType::Keyed {
                key: key_struct.clone(),
                value: left_struct.clone(),
            },
            edge_type: EdgeType::ShuffleJoin(0),
        };
        let right_join_edge = PlanEdge {
            edge_data_type: PlanType::Keyed {
                key: key_struct,
                value: right_struct.clone(),
            },
            edge_type: EdgeType::ShuffleJoin(1),
        };
        self.graph
            .add_edge(left_index, join_node_index, left_join_edge);
        self.graph
            .add_edge(right_index, join_node_index, right_join_edge);

        let merge_type = join_type.output_struct(&left_struct, &right_struct);
        let merge_operator = PlanOperator::JoinPairMerge(
            join_type,
            StructPair {
                left: left_struct,
                right: right_struct,
            },
        );
        let merge_index = self.insert_operator(merge_operator, PlanType::Unkeyed(merge_type));

        let merge_edge = PlanEdge {
            edge_data_type: join_node_output_type,
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
        let input_index = self.add_sql_operator(*input);
        let mut result_type = input_type.clone();
        result_type.fields.push(StructField {
            name: window_operator.field_name.clone(),
            alias: None,
            data_type: TypeDef::DataType(DataType::UInt64, false),
        });
        let partition_struct = window_operator.partition.output_struct();

        let partition_key_node = PlanOperator::RecordTransform(RecordTransform::KeyProjection(
            window_operator.partition,
        ));
        let partition_key_index = self.insert_operator(
            partition_key_node,
            PlanType::Keyed {
                key: partition_struct.clone(),
                value: input_type.clone(),
            },
        );
        let partition_key_edge = PlanEdge {
            edge_data_type: PlanType::Unkeyed(input_type.clone()),
            edge_type: EdgeType::Forward,
        };

        self.graph
            .add_edge(input_index, partition_key_index, partition_key_edge);

        let window_function_node = PlanOperator::WindowFunction(WindowFunctionOperator {
            window_function: window_operator.window_fn,
            order_by: window_operator.order_by,
            window_type: window_operator.window,
            result_struct: result_type.clone(),
            field_name: window_operator.field_name,
        });
        let window_function_index = self.insert_operator(
            window_function_node,
            PlanType::Keyed {
                key: partition_struct.clone(),
                value: result_type.clone(),
            },
        );
        let window_function_edge = PlanEdge {
            edge_data_type: PlanType::Keyed {
                key: partition_struct.clone(),
                value: input_type,
            },
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
                edge_data_type: PlanType::Keyed {
                    key: partition_struct,
                    value: result_type.clone(),
                },
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
        let input_type = input.return_type();
        let return_type = transform.output_struct(input_type.clone());
        let input_index = self.add_sql_operator(*input);
        let plan_node = PlanOperator::RecordTransform(transform);
        let plan_node_index = self.insert_operator(plan_node, PlanType::Unkeyed(return_type));
        let edge = PlanEdge {
            edge_data_type: PlanType::Unkeyed(input_type),
            edge_type: EdgeType::Forward,
        };
        self.graph.add_edge(input_index, plan_node_index, edge);
        plan_node_index
    }

    fn add_sql_sink(
        &mut self,
        name: String,
        sql_sink: crate::external::SqlSink,
        input: Box<SqlOperator>,
    ) -> NodeIndex {
        let input_type = input.return_type();
        let input_index = self.add_sql_operator(*input);
        let plan_node = PlanOperator::Sink(name, sql_sink);
        let plan_node_index =
            self.insert_operator(plan_node, PlanType::Unkeyed(input_type.clone()));
        let edge = PlanEdge {
            edge_data_type: PlanType::Unkeyed(input_type),
            edge_type: EdgeType::Forward,
        };
        self.graph.add_edge(input_index, plan_node_index, edge);
        plan_node_index
    }
}

impl From<PlanGraph> for DiGraph<StreamNode, StreamEdge> {
    fn from(val: PlanGraph) -> Self {
        val.graph.map(
            |index: NodeIndex, node| node.into_stream_node(index.index(), &val.sql_config),
            |_index, edge| edge.into_stream_edge(),
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

    let types: HashSet<_> = plan_graph
        .graph
        .node_weights()
        .flat_map(|node| node.get_all_types())
        .collect();

    let mut other_defs: Vec<_> = types
        .iter()
        .map(|s| s.def(key_structs.contains(&s.struct_name())))
        .collect();

    other_defs.extend(
        schema_provider
            .source_defs
            .into_iter()
            .filter(|(k, _)| plan_graph.sources.contains_key(k))
            .map(|(_, v)| v),
    );

    other_defs.push(format!(
        "mod udfs {{ {} }}",
        schema_provider
            .udf_defs
            .values()
            .map(|u| u.def.as_str())
            .collect::<Vec<_>>()
            .join("\n\n")
    ));

    let graph: DiGraph<StreamNode, StreamEdge> = plan_graph.into();

    Ok((
        Program {
            // For now, we don't export any types from SQL into WASM, as there is a problem with doing serde
            // in wasm
            types: vec![],
            other_defs,
            graph,
        },
        sources,
    ))
}
