use arroyo_operator::{context::SourceCollector, operator::SourceOperator, SourceFinishType};
use lapin::{
    options::*, publisher_confirm::Confirmation, types::FieldTable, BasicProperties, Connection,
    ConnectionProperties, Result,
};
use futures_lite::StreamExt;

#[derive(Debug)]
pub struct AmqpSourceFunc {
    pub topic: String,


}

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
        let channel = conn.create_channel().await.expect("create_channel");
        let queue_name =  format!( "arroyo-{}-{}-consumer", ctx.task_info.job_id, ctx.task_info.operator_id);
        let queue = channel.queue_declare(&queue_name, QueueDeclareOptions::default(), FieldTable::default()).await.expect("queue_declare");
        let mut consumer = channel.basic_consume(queue, consumer_tag, options, arguments)


        // loop
        // as in the Kafka one, need a flush ticker on the SourceCollector
        // control message parsing on lines 240-ish
        // https://github.com/amqp-rs/lapin/blob/main/examples/consumer.rs
        // the docs first create a consumer and a channel, but here not sure what to do
        // todo might add governor of rate limiting bevcomes necessary https://crates.io/crates/governor
        let mut flush_ticker = tokio::time::interval(Duration::from_millis(50));
        flush_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            select! {
                delivery = consumer.next().await {
                    Ok(delivery) =>{
                        // match
                        todo!("parse the message from amqp")
                        if let Some(v) = delivery.parse{
                            let timestamp = delivery.timestamp().
                        }
                        

                    }
                }
                 _ = flush_ticker.tick() => {
                    if collector.should_flush() {
                        collector.flush_buffer().await?;
                    }
                }

            }
        }
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
