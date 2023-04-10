use anyhow::Context;
use arroyo_datastream::{Operator, Program};
use arroyo_rpc::grpc::api::create_sql_job::Sink;
use arroyo_rpc::grpc::api::sink::SinkType;
use arroyo_rpc::grpc::api::{
    self, connection, create_pipeline_req, BuiltinSink, CreatePipelineReq, CreateSqlJob,
    PipelineDef, PipelineGraphReq, PipelineGraphResp, PipelineProgram, SqlError, SqlErrors,
};
use arroyo_sql::{ArroyoSchemaProvider, SqlConfig};

use cornucopia_async::GenericClient;
use deadpool_postgres::Transaction;
use petgraph::Direction;
use prost::Message;
use tracing::log::info;

use std::str::FromStr;
use tonic::Status;
use tracing::warn;

use crate::queries::api_queries;
use crate::queries::api_queries::DbPipeline;
use crate::sources::auth_config_to_hashmap;
use crate::types::public::PipelineType;
use crate::{
    connections, handle_db_error, log_and_map, optimizations, required_field, sinks,
    sources::{self, Source},
    AuthData,
};

async fn compile_sql<'e, E>(
    sql: &CreateSqlJob,
    auth_data: &AuthData,
    tx: &E,
) -> Result<(Program, Vec<i64>, Vec<i64>), Status>
where
    E: GenericClient,
{
    let mut schema_provider = ArroyoSchemaProvider::new();

    for source in sources::get_sources(auth_data, tx).await? {
        let s: Source = source.try_into().map_err(log_and_map)?;
        s.register(&mut schema_provider, auth_data);
    }

    let sinks = sinks::get_sinks(auth_data, tx).await?;

    let mut used_sink_ids = vec![];

    let sink = match sql.sink.as_ref().ok_or_else(|| required_field("sink"))? {
        Sink::Builtin(builtin) => match BuiltinSink::from_i32(*builtin).unwrap() {
            BuiltinSink::Null => Operator::NullSink,
            BuiltinSink::Web => Operator::GrpcSink,
            BuiltinSink::Log => Operator::ConsoleSink,
        },
        Sink::User(name) => {
            let sink = sinks.into_iter().find(|s| s.name == *name).ok_or_else(|| {
                Status::failed_precondition(format!("No sink with name '{}'", name))
            })?;

            used_sink_ids.push(sink.id);

            match sink.sink_type.unwrap() {
                SinkType::Kafka(k) => {
                    let connection =
                        connections::get_connection(auth_data, &k.connection, tx).await?;
                    let connection::ConnectionType::Kafka(kafka) = connection.connection_type.unwrap() else {
                        panic!("kafka sink {} [{}] configured with non-kafka connection", name, auth_data.organization_id);
                    };
                    Operator::KafkaSink {
                        topic: k.topic,
                        bootstrap_servers: kafka
                            .bootstrap_servers
                            .split(',')
                            .map(|t| t.to_string())
                            .collect(),
                        client_configs: auth_config_to_hashmap(kafka.auth_config),
                    }
                }
            }
        }
    };

    let (program, sources) = arroyo_sql::parse_and_get_program(
        &sql.query,
        schema_provider,
        SqlConfig {
            default_parallelism: sql.parallelism as usize,
            sink,
        },
    )
    .await
    .with_context(|| "failed to generate SQL program")
    .map_err(|err| {
        warn!("{:?}", err);
        Status::invalid_argument(format!("{}", err.root_cause()))
    })?;

    Ok((
        program,
        sources.into_iter().map(|s| s.id).collect(),
        used_sink_ids,
    ))
}

fn set_parallelism(program: &mut Program, parallelism: usize) {
    for node in program.graph.node_weights_mut() {
        node.parallelism = parallelism;
    }
}

async fn set_default_parallelism(_auth: &AuthData, program: &mut Program) -> Result<(), Status> {
    let sources = program.graph.externals(Direction::Incoming);

    let mut parallelism = 1;

    for source_idx in sources {
        let source = program.graph.node_weight(source_idx).unwrap();
        parallelism = parallelism.max(match &source.operator {
            Operator::FileSource { .. } => 1,
            Operator::ImpulseSource { spec, .. } => {
                let eps = match spec {
                    arroyo_datastream::ImpulseSpec::Delay(d) => 1.0 / d.as_secs_f32(),
                    arroyo_datastream::ImpulseSpec::EventsPerSecond(eps) => *eps,
                };

                (eps / 50_000.0).round() as usize
            }
            Operator::KafkaSource { .. } => {
                // for now, just use a default parallelism of 1 for kafka until we have autoscaling
                1

                // let (tx, _) = channel(1);
                // let kafka = KafkaTester::new(
                //     KafkaConnection {
                //         bootstrap_servers: bootstrap_servers.join(","),
                //     },
                //     Some(topic.clone()),
                //     None,
                //     tx,
                // );

                // kafka
                //     .topic_metadata()
                //     .await?
                //     .partitions
                //     .min(auth.org_metadata.max_parallelism as usize)
            }
            Operator::NexmarkSource {
                first_event_rate, ..
            } => (*first_event_rate as f32 / 50_000.0).round() as usize,
            op => panic!("Found non-source in a source position in graph: {:?}", op),
        });
    }

    info!("Setting pipeline parallelism to {}", parallelism);
    set_parallelism(program, parallelism);
    Ok(())
}

pub(crate) async fn create_pipeline<'a>(
    req: CreatePipelineReq,
    auth: AuthData,
    tx: &Transaction<'a>,
) -> Result<i64, Status> {
    let pipeline_type;
    let mut program;
    let sources;
    let sinks;
    let text;
    let compute_parallelism;

    match req.config.ok_or_else(|| required_field("config"))? {
        create_pipeline_req::Config::Program(bytes) => {
            if !auth.org_metadata.can_create_programs {
                return Err(Status::invalid_argument(
                    "Your plan does not allow you to call this API.",
                ));
            }
            pipeline_type = PipelineType::rust;
            program = PipelineProgram::decode(&bytes[..])
                .map_err(log_and_map)?
                .try_into()
                .map_err(log_and_map)?;
            sources = vec![];
            sinks = vec![];
            text = None;
            compute_parallelism = false;
        }
        create_pipeline_req::Config::Sql(sql) => {
            if sql.parallelism > auth.org_metadata.max_parallelism as u64 {
                return Err(Status::invalid_argument(format!(
                    "Your plan allows you to run pipelines up to parallelism {};
                    contact support@arroyo.systems for an increase",
                    auth.org_metadata.max_parallelism
                )));
            }

            pipeline_type = PipelineType::sql;
            (program, sources, sinks) = compile_sql(&sql, &auth, tx).await?;
            text = Some(sql.query);
            compute_parallelism = sql.parallelism == 0;
        }
    };

    optimizations::optimize(&mut program.graph);

    if program.graph.node_count() > auth.org_metadata.max_operators as usize {
        return Err(Status::invalid_argument(
            format!("This pipeline is too large to create under your plan, which only allows pipelines up to {} nodes;
                contact support@arroyo.systems for an increase", auth.org_metadata.max_operators)));
    }

    let errors = program.validate_graph();
    if !errors.is_empty() {
        let errs: Vec<String> = errors.iter().map(|s| format!("  * {}\n", s)).collect();

        return Err(Status::failed_precondition(format!(
            "Program validation failed:\n{}",
            errs.join("")
        )));
    }

    // TODO: this is very hacky
    let is_preview = req.name.starts_with("preview");
    if is_preview {
        set_parallelism(&mut program, 1);
    } else if compute_parallelism {
        set_default_parallelism(&auth, &mut program).await?;
    }

    let proto_program: PipelineProgram = program.try_into().map_err(log_and_map)?;

    let program = proto_program.encode_to_vec();
    let version = 2;

    if req.name.is_empty() {
        return Err(required_field("name"));
    }

    let pipeline_id = api_queries::create_pipeline()
        .bind(
            tx,
            &auth.organization_id,
            &auth.user_id,
            &req.name,
            &pipeline_type,
            &version,
        )
        .one()
        .await
        .map_err(|e| handle_db_error("pipeline", e))?;

    api_queries::create_pipeline_definition()
        .bind(
            tx,
            &auth.organization_id,
            &auth.user_id,
            &pipeline_id,
            &version,
            &text,
            &program,
        )
        .one()
        .await
        .map_err(log_and_map)?;

    if !is_preview {
        for source in sources {
            api_queries::add_pipeline_source()
                .bind(tx, &pipeline_id, &source)
                .await
                .map_err(log_and_map)?;
        }

        for sink in sinks {
            api_queries::add_pipeline_sink()
                .bind(tx, &pipeline_id, &sink)
                .await
                .map_err(log_and_map)?;
        }
    }

    Ok(pipeline_id)
}

impl TryInto<PipelineDef> for DbPipeline {
    type Error = Status;

    fn try_into(self) -> Result<PipelineDef, Self::Error> {
        let program: Program = PipelineProgram::decode(&self.program[..])
            .map_err(log_and_map)?
            .try_into()
            .map_err(log_and_map)?;

        Ok(PipelineDef {
            pipeline_id: format!("{}", self.id),
            name: self.name,
            r#type: format!("{:?}", self.r#type),
            definition: self.textual_repr,
            job_graph: Some(program.as_job_graph()),
        })
    }
}

pub(crate) async fn get_pipeline(
    id: &str,
    auth: &AuthData,
    db: &impl GenericClient,
) -> Result<PipelineDef, Status> {
    if let Ok(id) = i64::from_str(id) {
        let res = api_queries::get_pipeline()
            .bind(db, &id, &auth.organization_id)
            .opt()
            .await
            .map_err(log_and_map)?;

        if let Some(res) = res {
            return res.try_into();
        }
    }

    Err(Status::not_found(format!("No pipeline with id {}", id)))
}

pub(crate) async fn sql_graph(
    req: PipelineGraphReq,
    auth: AuthData,
    client: &impl GenericClient,
) -> Result<PipelineGraphResp, Status> {
    let sql = CreateSqlJob {
        query: req.query,
        parallelism: 1,
        sink: Some(Sink::Builtin(BuiltinSink::Null as i32)),
    };

    match compile_sql(&sql, &auth, client).await {
        Ok((mut program, _, _)) => {
            optimizations::optimize(&mut program.graph);
            Ok(PipelineGraphResp {
                result: Some(api::pipeline_graph_resp::Result::JobGraph(
                    program.as_job_graph(),
                )),
            })
        }
        Err(err) => match err.code() {
            tonic::Code::InvalidArgument => Ok(PipelineGraphResp {
                result: Some(api::pipeline_graph_resp::Result::Errors(SqlErrors {
                    errors: vec![SqlError {
                        message: err.message().to_string(),
                    }],
                })),
            }),
            _ => Err(err),
        },
    }
}
