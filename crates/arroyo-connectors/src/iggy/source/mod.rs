use anyhow::Result;
use arroyo_operator::context::{SourceCollector, SourceContext};
use arroyo_operator::operator::SourceOperator;
use arroyo_operator::SourceFinishType;
use arroyo_rpc::formats::{BadData, Format, Framing};
use arroyo_rpc::grpc::rpc::{StopMode, TableConfig};
use arroyo_rpc::{ControlMessage, MetadataField};
use arroyo_state::global_table_config;
use arroyo_types::*;
use async_trait::async_trait;
use bincode::{Decode, Encode};
use governor::{Quota, RateLimiter};
use iggy::client::{Client, MessageClient, TopicClient, UserClient};
use iggy::clients::client::IggyClient;
use iggy::consumer::Consumer;
use iggy::identifier::Identifier;
use iggy::messages::poll_messages::PollingStrategy;
use iggy::models::messages::PolledMessage;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::str::FromStr;
use std::time::{Duration, SystemTime};
use tokio::time::sleep;
use tracing::{error, info};

use crate::iggy::IggySourceOffset;

#[derive(Copy, Clone, Debug, Encode, Decode, PartialEq, PartialOrd)]
pub struct IggyState {
    offset: u64,
}

pub struct IggySourceFunc {
    pub stream: String,
    pub topic: String,
    pub partition_id: u32,
    pub consumer_id: u32,
    pub endpoint: String,
    pub _transport: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub offset_mode: IggySourceOffset,
    pub auto_commit: bool,
    pub format: Format,
    pub framing: Option<Framing>,
    pub bad_data: Option<BadData>,
    pub client: Option<IggyClient>,
    pub messages_per_second: NonZeroU32,
    pub metadata_fields: Vec<MetadataField>,
}

impl IggySourceFunc {
    async fn init_client(&mut self) -> Result<()> {
        info!("Creating Iggy client for {}", self.endpoint);

        let client = IggyClient::default();

        client.connect().await?;

        if let (Some(username), Some(password)) = (&self.username, &self.password) {
            client.login_user(username, password).await?;
        }

        self.client = Some(client);
        Ok(())
    }

    async fn run_int(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> Result<SourceFinishType, UserError> {
        if self.client.is_none() {
            self.init_client().await.map_err(|e| {
                UserError::new("Could not create Iggy client", format!("Error: {:?}", e))
            })?;
        }

        let rate_limiter = RateLimiter::direct(Quota::per_second(self.messages_per_second));

        collector.initialize_deserializer(
            self.format.clone(),
            self.framing.clone(),
            self.bad_data.clone(),
            &self.metadata_fields,
        );

        let mut offset = match self.offset_mode {
            IggySourceOffset::Earliest => 0,
            IggySourceOffset::Latest => {
                let stream_id = Identifier::from_str(&self.stream).map_err(|e| {
                    UserError::new("Invalid stream identifier", format!("Error: {:?}", e))
                })?;

                let topic_id = Identifier::from_str(&self.topic).map_err(|e| {
                    UserError::new("Invalid topic identifier", format!("Error: {:?}", e))
                })?;

                let topic_details = self
                    .client
                    .as_ref()
                    .unwrap()
                    .get_topic(&stream_id, &topic_id)
                    .await
                    .map_err(|e| {
                        UserError::new("Could not get topic info", format!("Error: {:?}", e))
                    })?;

                let unwrapped_topic_details = topic_details.unwrap();
                let partition = unwrapped_topic_details
                    .partitions
                    .iter()
                    .find(|p| p.id == self.partition_id)
                    .ok_or_else(|| {
                        UserError::new(
                            "Partition not found",
                            format!(
                                "Partition {} not found in topic {}",
                                self.partition_id, self.topic
                            ),
                        )
                    })?;

                partition.current_offset
            }
        };

        let consumer = Consumer::new(Identifier::numeric(self.consumer_id).unwrap());

        loop {
            if let Ok(control_message) = ctx.control_rx.try_recv() {
                match control_message {
                    ControlMessage::Checkpoint(barrier) => {
                        let state = IggyState { offset };
                        let table = ctx
                            .table_manager
                            .get_global_keyed_state("iggy")
                            .await
                            .map_err(|err| {
                                UserError::new("Failed to get global key value", err.to_string())
                            })?;
                        table.insert(self.partition_id, state).await;

                        let client_temp = self.client.take();
                        let result = self.start_checkpoint(barrier, ctx, collector).await;
                        self.client = client_temp;

                        if result {
                            return Ok(SourceFinishType::Immediate);
                        }
                    }
                    ControlMessage::Stop { mode } => {
                        info!("Received stop command with mode {:?}", mode);
                        match mode {
                            StopMode::Graceful => {
                                return Ok(SourceFinishType::Graceful);
                            }
                            StopMode::Immediate => {
                                return Ok(SourceFinishType::Immediate);
                            }
                        }
                    }
                    ControlMessage::LoadCompacted { compacted } => {
                        ctx.load_compacted(compacted).await;
                    }
                    ControlMessage::NoOp => {}
                    ControlMessage::Commit { .. } => {
                        unreachable!("Sources shouldn't receive commit messages");
                    }
                }
            }

            let stream_id = Identifier::from_str(&self.stream).map_err(|e| {
                UserError::new("Invalid stream identifier", format!("Error: {:?}", e))
            })?;

            let topic_id = Identifier::from_str(&self.topic).map_err(|e| {
                UserError::new("Invalid topic identifier", format!("Error: {:?}", e))
            })?;

            let polling_strategy = PollingStrategy::offset(offset);

            let messages_per_batch = 100;

            let polled_messages = match self
                .client
                .as_ref()
                .unwrap()
                .poll_messages(
                    &stream_id,
                    &topic_id,
                    Some(self.partition_id),
                    &consumer,
                    &polling_strategy,
                    messages_per_batch,
                    self.auto_commit,
                )
                .await
            {
                Ok(messages) => messages,
                Err(e) => {
                    error!("Error polling messages: {:?}", e);

                    sleep(Duration::from_millis(1000)).await;
                    continue;
                }
            };

            if polled_messages.messages.is_empty() {
                sleep(Duration::from_millis(100)).await;
                continue;
            }

            for message in &polled_messages.messages {
                offset = message.offset + 1;

                self.process_message(message, collector).await?;

                if rate_limiter.check().is_err() {
                    sleep(Duration::from_millis(10)).await;
                }
            }

            if collector.should_flush() {
                collector.flush_buffer().await?;
            }
        }
    }

    async fn process_message(
        &self,
        message: &PolledMessage,
        collector: &mut SourceCollector,
    ) -> Result<(), UserError> {
        let timestamp =
            { SystemTime::UNIX_EPOCH + Duration::from_millis(message.timestamp / 1000) };

        let mut additional_fields = HashMap::new();
        for field in &self.metadata_fields {
            match field.key.as_str() {
                "offset" => {
                    additional_fields.insert(
                        field.field_name.as_str(),
                        arroyo_formats::de::FieldValueType::Int64(Some(message.offset as i64)),
                    );
                }
                "partition" => {
                    additional_fields.insert(
                        field.field_name.as_str(),
                        arroyo_formats::de::FieldValueType::Int32(Some(self.partition_id as i32)),
                    );
                }
                "stream" => {
                    additional_fields.insert(
                        field.field_name.as_str(),
                        arroyo_formats::de::FieldValueType::String(Some(&self.stream)),
                    );
                }
                "topic" => {
                    additional_fields.insert(
                        field.field_name.as_str(),
                        arroyo_formats::de::FieldValueType::String(Some(&self.topic)),
                    );
                }
                "timestamp" => {
                    additional_fields.insert(
                        field.field_name.as_str(),
                        arroyo_formats::de::FieldValueType::Int64(Some(message.timestamp as i64)),
                    );
                }
                _ => {}
            }
        }

        collector
            .deserialize_slice(
                &message.payload,
                timestamp,
                if additional_fields.is_empty() {
                    None
                } else {
                    Some(&additional_fields)
                },
            )
            .await?;

        Ok(())
    }
}

#[async_trait]
impl SourceOperator for IggySourceFunc {
    async fn run(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> SourceFinishType {
        match self.run_int(ctx, collector).await {
            Ok(r) => r,
            Err(e) => {
                ctx.report_error(e.name.clone(), e.details.clone()).await;
                panic!("{}: {}", e.name, e.details);
            }
        }
    }

    fn name(&self) -> String {
        format!("iggy-{}-{}", self.stream, self.topic)
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        global_table_config("iggy", "iggy source state")
    }
}
