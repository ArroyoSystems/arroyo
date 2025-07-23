use async_trait::async_trait;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use arroyo_rpc::formats::{BadData, Format, Framing};
use arroyo_rpc::{grpc::rpc::StopMode, ControlMessage, MetadataField};
use arroyo_types::{SignalMessage, UserError, Watermark};
use governor::{Quota, RateLimiter as GovernorRateLimiter};

use crate::zmq::{ConnectionPattern, ZmqConfig};
use arroyo_operator::context::{SourceCollector, SourceContext};
use arroyo_operator::operator::SourceOperator;
use arroyo_operator::SourceFinishType;
use arroyo_rpc::grpc::rpc::TableConfig;
use tokio::select;
use tokio::time::MissedTickBehavior;

#[cfg(test)]
mod test;

pub struct ZmqSourceFunc {
    pub config: ZmqConfig,
    pub format: Format,
    pub framing: Option<Framing>,
    pub bad_data: Option<BadData>,
    pub messages_per_second: NonZeroU32,
    pub subscribed: Arc<AtomicBool>,
    pub metadata_fields: Vec<MetadataField>,
}

enum ZmqThreadEvent {
    Message(Vec<u8>),
    Error(UserError),
    Shutdown,
}

#[async_trait]
impl SourceOperator for ZmqSourceFunc {
    fn name(&self) -> String {
        let url = self.config.url.sub_env_vars().unwrap();
        format!("zmq-source-{url}")
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        arroyo_state::global_table_config("zmq", "zmq source state")
    }

    async fn run(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> SourceFinishType {
        match self.run_int(ctx, collector).await {
            Ok(r) => r,
            Err(e) => {
                ctx.report_error(&e.name, &e.details).await;

                panic!("{}: {}", e.name, e.details);
            }
        }
    }
}

impl ZmqSourceFunc {
    pub fn new(
        config: ZmqConfig,
        format: Format,
        framing: Option<Framing>,
        bad_data: Option<BadData>,
        messages_per_second: u32,
        metadata_fields: Vec<MetadataField>,
    ) -> Self {
        Self {
            config,
            format,
            framing,
            bad_data,
            messages_per_second: NonZeroU32::new(messages_per_second).unwrap(),
            subscribed: Arc::new(AtomicBool::new(false)),
            metadata_fields,
        }
    }

    /// Returns the subscription status flag. External readers should use
    /// `Ordering::SeqCst` when calling `.load()` to ensure proper synchronization.
    pub fn subscribed(&self) -> Arc<AtomicBool> {
        self.subscribed.clone()
    }

    async fn run_int(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> Result<SourceFinishType, UserError> {
        assert_eq!(
            self.metadata_fields.len(),
            0,
            "ZMQ connector does not support metadata, but found: {:?}",
            self.metadata_fields
        );

        if ctx.task_info.task_index > 0 {
            tracing::warn!(
                "Zmq Consumer {}-{} can only be executed on a single worker...",
                ctx.task_info.operator_id,
                ctx.task_info.task_index
            );
            collector
                .broadcast(SignalMessage::Watermark(Watermark::Idle))
                .await;
        }

        collector.initialize_deserializer(
            self.format.clone(),
            self.framing.clone(),
            self.bad_data.clone(),
            &self.metadata_fields,
        );

        let config_url = match self.config.url.sub_env_vars() {
            Ok(config_url) => config_url,
            Err(err) => {
                return Err(UserError::new(
                    "ZmqSourceError".to_string(),
                    format!("Failed to resolve url environment variables: {err}"),
                ));
            }
        };

        let mut flush_ticker = tokio::time::interval(Duration::from_millis(50));
        flush_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let (zmq_event_tx, mut zmq_event_rx) = tokio::sync::mpsc::channel::<ZmqThreadEvent>(1000);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        let connection_pattern = self.config.connection_pattern;
        let messages_per_second = self.messages_per_second;
        let subscribed = self.subscribed();

        // Use spawn_blocking to run ZMQ operations in a dedicated thread as ZMQ sockets are not thread safe
        // and Tokio does not guarantee that tasks will always be scheduled on the same thread.
        //
        // From the ZMQ guide (https://zguide.zeromq.org/docs/chapter2/#Multithreading-with-ZeroMQ):
        //     Don't share ZeroMQ sockets between threads. ZeroMQ sockets are not threadsafe.
        //     Technically it's possible to migrate a socket from one thread to another but it demands
        //     skill. The only place where it's remotely sane to share sockets between threads are in
        //     language bindings that need to do magic like garbage collection on sockets.
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Could not create Tokio runtime.");
            runtime.block_on(async move {
                let context = zmq::Context::new();

                let socket = match context.socket(zmq::SocketType::PULL) {
                    Ok(socket) => socket,
                    Err(err) => {
                        zmq_event_tx
                            .send(ZmqThreadEvent::Error(UserError::new(
                                "ZmqSourceError".to_string(),
                                format!("Failed to create zmq socket: {err}"),
                            )))
                            .await
                            .unwrap();
                        return;
                    }
                };

                if let Err(e) = socket.set_rcvtimeo(100) {
                    zmq_event_tx
                        .send(ZmqThreadEvent::Error(UserError {
                            name: "ZmqSourceError".to_string(),
                            details: format!("Failed to set socket timeout: {e}"),
                        }))
                        .await
                        .unwrap();
                    return;
                }

                let result = match connection_pattern {
                    ConnectionPattern::Bind => socket.bind(&config_url),
                    ConnectionPattern::Connect => socket.connect(&config_url),
                };

                if let Err(e) = result {
                    let operation = match connection_pattern {
                        ConnectionPattern::Bind => "bind",
                        ConnectionPattern::Connect => "connect",
                    };
                    zmq_event_tx
                        .send(ZmqThreadEvent::Error(UserError {
                            name: "ZmqSourceError".to_string(),
                            details: format!("Failed to {} to {}: {}", operation, &config_url, e),
                        }))
                        .await
                        .unwrap();
                    return;
                }

                subscribed.store(true, Ordering::SeqCst);

                let rate_limiter =
                    GovernorRateLimiter::direct(Quota::per_second(messages_per_second));
                let shutdown_rx = shutdown_rx;

                loop {
                    if shutdown_rx.has_changed().unwrap_or(false) && *shutdown_rx.borrow() {
                        break;
                    }

                    match socket.recv_bytes(0) {
                        Ok(payload) => {
                            if zmq_event_tx
                                .send(ZmqThreadEvent::Message(payload))
                                .await
                                .is_err()
                            {
                                // Event channel closed, main task has shut down
                                break;
                            }
                            rate_limiter.until_ready().await;
                        }
                        Err(zmq::Error::EAGAIN) => {
                            // No message available, continue polling
                        }
                        Err(err) => {
                            if zmq_event_tx
                                .send(ZmqThreadEvent::Error(UserError::new(
                                    "Failed to receive ZMQ message.",
                                    err.to_string(),
                                )))
                                .await
                                .is_err()
                            {
                                // Event channel closed, main task has shut down
                                break;
                            }
                        }
                    }
                }

                subscribed.store(false, Ordering::SeqCst);

                let disconnect_result = match connection_pattern {
                    ConnectionPattern::Bind => socket.unbind(&config_url),
                    ConnectionPattern::Connect => socket.disconnect(&config_url),
                };

                if let Err(err) = disconnect_result {
                    tracing::error!("Failed to disconnect ZMQ socket: {}", err);
                }

                let _ = zmq_event_tx.send(ZmqThreadEvent::Shutdown).await;
            })
        });

        loop {
            select! {
                event = zmq_event_rx.recv() => {
                    match event {
                        Some(event) => {
                            match event {
                                ZmqThreadEvent::Message(payload) => {
                                    if let Err(err) = collector
                                        .deserialize_slice(&payload, SystemTime::now(), None)
                                        .await
                                    {
                                        shutdown_tx.send(true).ok();
                                        return Err(err);
                                    }
                                }
                                ZmqThreadEvent::Error(err) => {
                                    shutdown_tx.send(true).ok();
                                    return Err(err);
                                }
                                ZmqThreadEvent::Shutdown => {
                                    return Ok(SourceFinishType::Immediate);
                                }
                            }
                        }
                        None => {
                            // Event channel closed, ZMQ thread has shut down
                            tracing::debug!("ZMQ event channel closed, main task finishing");
                            return Ok(SourceFinishType::Immediate);
                        }
                    }
                }
                _ = flush_ticker.tick() => {
                    if collector.should_flush() {
                        if let Err(err) = collector.flush_buffer().await {
                            shutdown_tx.send(true).ok();
                            return Err(err);
                        }
                    }
                }
                control_message = ctx.control_rx.recv() => {
                    match control_message {
                        Some(ControlMessage::Checkpoint(c)) => {
                            tracing::debug!("starting checkpointing {}", ctx.task_info.task_index);
                            if self.start_checkpoint(c, ctx, collector).await {
                                shutdown_tx.send(true).ok();
                                return Ok(SourceFinishType::Immediate);
                            }
                        }
                        Some(ControlMessage::Stop { mode }) => {
                            tracing::info!("Stopping Zmq source: {:?}", mode);
                            shutdown_tx.send(true).ok();
                            match mode {
                                StopMode::Graceful => return Ok(SourceFinishType::Graceful),
                                StopMode::Immediate => return Ok(SourceFinishType::Immediate),
                            }
                        }
                        Some(ControlMessage::Commit { .. }) => {
                            unreachable!("Sources shouldn't receive commit messages");
                        }
                        Some(ControlMessage::LoadCompacted { compacted }) => {
                            ctx.load_compacted(compacted).await;
                        }
                        Some(ControlMessage::NoOp) => {}
                        None => {}
                    }
                }
            }
        }
    }
}
