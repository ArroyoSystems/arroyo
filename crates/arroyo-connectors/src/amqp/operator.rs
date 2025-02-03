use arroyo_operator::{context::SourceCollector, operator::SourceOperator, SourceFinishType};
use lapin::{
    options::*, publisher_confirm::Confirmation, types::FieldTable, BasicProperties, Connection,
    ConnectionProperties, Result,
};

#[derive(Debug)]
pub struct AmqpSourceFunc {}

impl AmqpSourceFunc {
    async fn run_int(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> Result<SourceFinishType, UserError> {
        /// Manages the main loop for consuming messages from the AMQP stream
        /// It first creates a connection and handles any errors that occur during this process.
        /// It sets up a ticker to periodically flush the message collector's buffer.
        /// Inside the loop, it uses the select! macro to handle different asynchronous events: receiving a message from the consumer, ticking the flush ticker, or receiving control messages from the context.
        /// Depending on the event, it processes the message, flushes the buffer if needed, handles errors, or processes control messages such as checkpoints, stopping, committing, or loading compacted data.
        /// The function ensures that the stream is read and processed continuously until a stop condition is met.
        let conn = Connection::connect(&addr, ConnectionProperties::default()).await?;
        todo!()
        // loop
        // as in the Kafka one, need a flush ticker on the SourceCollector
        // control message parsing on lines 240-ish
    }
}

// todo add config - incrementally

#[async_trait]
impl SourceOperator for RabbitmqStreamSourceFunc {
    fn name(&self) -> String {
        format!("amqp-lapin-{}", self.stream)
    }

    async fn run(
        &mut self,
        ctx: &mut arroyo_operator::context::SourceContext,
        collector: &mut SourceCollector,
    ) -> SourceFinishType {
        collector.initialize_deserializer(
            self.format.clone(),
            self.framing.clone(),
            self.bad_data.clone(),
            &[],
        );

        match self.run_int(ctx, collector).await {
            ok(r) => r,
            err(e) => {
                ctx.report_error(e.name.clone(), e.details.clone()).await;

                panic!("{}: {}", e.name, e.details);
            }
        }
    }
}
