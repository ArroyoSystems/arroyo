use super::get_nats_client;
use super::get_nats_client_config;
use super::ConnectorType;
use super::NatsConfig;
use super::NatsState;
use super::NatsTable;
use arroyo_operator::context::ArrowContext;
use arroyo_operator::operator::SourceOperator;
use arroyo_operator::SourceFinishType;
use arroyo_rpc::formats::BadData;
use arroyo_rpc::formats::{Format, Framing};
use arroyo_rpc::grpc::StopMode;
use arroyo_rpc::grpc::TableConfig;
use arroyo_rpc::ControlMessage;
use arroyo_rpc::ControlResp;
use arroyo_rpc::OperatorConfig;
use arroyo_types::UserError;
use async_nats::jetstream::consumer;
use async_trait::async_trait;
use futures::StreamExt;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::time::Duration;
use tokio::select;
use tracing::debug;
use tracing::info;

pub struct NatsSourceFunc {
    pub stream: String,
    pub servers: String,
    pub connection: NatsConfig,
    pub table: NatsTable,
    pub format: Format,
    pub framing: Option<Framing>,
    pub bad_data: Option<BadData>,
    pub consumer_config: HashMap<String, String>,
    pub messages_per_second: NonZeroU32,
}

#[async_trait]
impl SourceOperator for NatsSourceFunc {
    fn name(&self) -> String {
        format!("NATS-{}", self.stream)
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        arroyo_state::global_table_config("n", "NATS source state")
    }

    async fn run(&mut self, ctx: &mut ArrowContext) -> SourceFinishType {
        match self.run_int(ctx).await {
            Ok(res) => res,
            Err(err) => {
                ctx.control_tx
                    .send(ControlResp::Error {
                        operator_id: ctx.task_info.operator_id.clone(),
                        task_index: ctx.task_info.task_index,
                        message: err.name.clone(),
                        details: err.details.clone(),
                    })
                    .await
                    .unwrap();
                panic!("{}: {}", err.name, err.details);
            }
        }
    }
}

impl NatsSourceFunc {
    pub fn from_config(config: &str) -> Self {
        let config: OperatorConfig =
            serde_json::from_str(config).expect("Invalid config for NatSourceFunc");
        let connection: NatsConfig = serde_json::from_value(config.connection)
            .expect("Invalid connection config for NatsSourceFunc");
        let table: NatsTable =
            serde_json::from_value(config.table).expect("Invalid table config for NatsSourceFunc");
        let format = config
            .format
            .expect("NATS source must have a format configured");
        let framing = config.framing;

        let stream = match &table.connector_type {
            ConnectorType::Source { ref stream, .. } => stream,
            _ => panic!("NATS source must have a stream configured"),
        };

        let consumer_default_config = get_nats_client_config(&connection, &table);

        Self {
            stream: stream.clone().unwrap(),
            servers: connection.servers.clone(),
            connection,
            table,
            format,
            framing,
            bad_data: config.bad_data,
            consumer_config: consumer_default_config,
            messages_per_second: NonZeroU32::new(
                config
                    .rate_limit
                    .map(|l| l.messages_per_second)
                    .unwrap_or(u32::MAX),
            )
            .unwrap(),
        }
    }

    async fn get_nats_stream(
        &mut self,
        stream_name: String,
    ) -> async_nats::jetstream::stream::Stream {
        let client = get_nats_client(&self.connection, &self.table)
            .await
            .unwrap();
        let jetstream = async_nats::jetstream::new(client);
        let mut stream = jetstream.get_stream(&stream_name).await.unwrap();
        let stream_info = stream.info().await.unwrap();

        info!("<---------------------------------------------->");
        info!("Stream - timestamp of creation: {}", &stream_info.created);
        info!(
            "Stream - lowest sequence number still present: {}",
            &stream_info.state.first_sequence
        );
        info!(
            "Stream - last sequence number assigned to a message: {}",
            &stream_info.state.last_sequence
        );
        info!(
            "Stream - time that the last message was received: {}",
            &stream_info.state.last_timestamp
        );
        info!(
            "Stream - number of messages contained: {}",
            &stream_info.state.messages
        );
        info!(
            "Stream - number of bytes contained: {}",
            &stream_info.state.bytes
        );
        info!(
            "Stream - number of consumers: {}",
            &stream_info.state.consumer_count
        );

        stream
    }

    async fn create_nats_consumer(
        &mut self,
        stream: &async_nats::jetstream::stream::Stream,
        sequence_number: u64,
        ctx: &mut ArrowContext,
    ) -> consumer::Consumer<consumer::pull::Config> {
        match sequence_number {
            1 => info!(
                ">> No state found for NATS, starting from sequence number #{}",
                &sequence_number
            ),
            _ => info!(
                ">> Found state for NATS, starting from sequence number #{}",
                &sequence_number
            ),
        };

        let deliver_policy = {
            if sequence_number == 1 {
                consumer::DeliverPolicy::All
            } else {
                consumer::DeliverPolicy::ByStartSequence {
                    start_sequence: sequence_number,
                }
            }
        };

        let consumer_name = format!(
            "{}-{}",
            self.stream.clone(),
            &ctx.task_info.operator_id.clone().replace("operator_", "")
        );

        // TODO: Generate this `consumer_config` via a function that parses
        // all optional parameters passed in the `client_configs` of the `table.json`
        // and merges with the default values
        let consumer_config = consumer::pull::Config {
            name: Some(consumer_name.clone()),
            replay_policy: consumer::ReplayPolicy::Instant,
            inactive_threshold: Duration::from_secs(60),
            ack_policy: consumer::AckPolicy::Explicit,
            ack_wait: Duration::from_secs(60),
            num_replicas: 1,
            deliver_policy,
            ..Default::default()
        };

        match stream.delete_consumer(&consumer_name).await {
            Ok(_) => {
                info!(
                    ">> Existing consumer deleted. Recreating consumer with new `start_sequence`."
                )
            }
            Err(_) => {
                info!(">> No existing consumer found, proceeding with the creation of a new one.")
            }
        }

        let mut consumer = stream
            .create_consumer(consumer_config.clone())
            .await
            .unwrap();

        let consumer_info = consumer.info().await.unwrap();
        info!(
            "Consumer - timestamp of creation: {}",
            &consumer_info.created
        );
        info!(
            "Consumer - last stream sequence of aknowledged messagee: {}",
            &consumer_info.ack_floor.stream_sequence
        );
        info!(
            "Consumer - last consumer sequence of aknowledged message: {}",
            &consumer_info.ack_floor.consumer_sequence
        );
        info!(
            "Consumer delivered messages: {}",
            &consumer_info.num_ack_pending
        );
        info!(
            "Consumer pending ack messages: {}",
            &consumer_info.num_pending
        );
        info!(
            "Consumer waiting delivery messages: {}",
            &consumer_info.num_waiting
        );
        info!("<--------------------------------------------->");
        consumer
    }

    async fn get_start_sequence_number(&self, ctx: &mut ArrowContext) -> u64 {
        let state = ctx
            .table_manager
            .get_global_keyed_state::<String, NatsState>("n")
            .await
            .map_err(|err| UserError::new("failed to get global key value", err.to_string()))
            .unwrap()
            .get_all();

        if !state.is_empty() {
            let state_sequence_number = state
                .get(&ctx.task_info.operator_id)
                .map(|nats_state| nats_state.stream_sequence_number)
                .unwrap();
            state_sequence_number + 1
        } else {
            1
        }
    }

    async fn run_int(&mut self, ctx: &mut ArrowContext) -> Result<SourceFinishType, UserError> {
        let start_sequence = self.get_start_sequence_number(ctx).await;
        let stream = self.get_nats_stream(self.stream.clone()).await;
        let consumer = self
            .create_nats_consumer(&stream, start_sequence, ctx)
            .await;
        let mut messages = consumer.messages().await.unwrap();
        let mut sequence_numbers: HashMap<String, NatsState> = HashMap::new();

        ctx.initialize_deserializer(
            self.format.clone(),
            self.framing.clone(),
            self.bad_data.clone(),
        );

        loop {
            select! {
                message = messages.next() => {
                    match message {
                        Some(Ok(msg)) => {
                            let payload = msg.payload.as_ref();
                            let message_info = msg.info().unwrap();
                            let timestamp = message_info.published.into() ;

                            ctx.deserialize_slice(&payload, timestamp).await?;

                            debug!("---------------------------------------------->");
                            debug!(
                                "Delivered stream sequence: {}",
                                message_info.stream_sequence
                            );
                            debug!(
                                "Delivered consumer sequence: {}",
                                message_info.consumer_sequence
                            );
                            debug!(
                                "Delivered message stream: {}",
                                message_info.stream
                            );
                            debug!(
                                "Delivered message consumer: {}",
                                message_info.consumer
                            );
                            debug!(
                                "Delivered message published: {}",
                                message_info.published
                            );
                            debug!(
                                "Delivered message pending: {}",
                                message_info.pending
                            );
                            debug!(
                                "Delivered message delivered: {}",
                                message_info.delivered
                            );

                            if ctx.should_flush() {
                                ctx.flush_buffer().await?;
                            }

                            sequence_numbers.insert(
                                ctx.task_info.operator_id.clone(),
                                NatsState {
                                    stream_name: self.stream.clone(),
                                    stream_sequence_number: message_info.stream_sequence.clone()
                                }
                            );

                            // TODO: Has ACK to happens here at every message? Maybe it can be
                            // done by ack only the last message before checkpointing in the below
                            // `ControlMessage::Checkpoint` match.
                            msg.ack().await.unwrap();
                        },
                        Some(Err(msg)) => {
                            return Err(UserError::new("NATS message error", msg.to_string()));
                        },
                        None => {
                            break
                            info!("Finished reading message from {}", self.stream);
                        },
                    }
                }
                control_message = ctx.control_rx.recv() => {
                    match control_message {
                        Some(ControlMessage::Checkpoint(c)) => {
                            debug!("Starting checkpointing {}", ctx.task_info.task_index);
                            let state = ctx.table_manager
                                .get_global_keyed_state("n")
                                .await
                                .map_err(|err| UserError::new("failed to get global key value", err.to_string()))?;

                            // TODO: Should this be parallelized?
                            for (stream_name, sequence_number) in &sequence_numbers {
                                state.insert(stream_name.clone(), sequence_number.clone()).await;
                            }

                            let state_sequence_number = state
                                .get(&ctx.task_info.operator_id)
                                .map(|nats_state| nats_state.stream_sequence_number)
                                .unwrap();


                            if self.start_checkpoint(c, ctx).await {
                                return Ok(SourceFinishType::Immediate);
                            }

                            info!("Checkpoint done at sequence number #{}", state_sequence_number);
                        }
                        Some(ControlMessage::Stop { mode }) => {
                            info!("Stopping NATS source: {:?}", mode);
                            match mode {
                                StopMode::Graceful => {
                                    return Ok(SourceFinishType::Graceful);
                                }
                                StopMode::Immediate => {
                                    return Ok(SourceFinishType::Immediate);
                                }
                            }
                        }
                        Some(ControlMessage::Commit { .. }) => {
                            unreachable!("Sources shouldn't receive commit messages");
                        }
                        Some(ControlMessage::LoadCompacted {compacted}) => {
                            ctx.load_compacted(compacted).await;
                        }
                        Some(ControlMessage::NoOp) => {}
                        None => {}
                    }
                }
            }
        }
        Ok(SourceFinishType::Graceful)
    }
}
