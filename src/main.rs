mod application;
mod logger;
mod producer;

use producer::{amqp::AMQPProducer, kafka::KafkaProducer, Producer};

#[actix::main]
#[doc(hidden)]
/// Where the magic happens 🌌
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup the indexer
    application::initialize()?;

    // Setup the producer
    let producer_type = producer::get_type()?;
    if !(producer_type == producer::Type::Kafka || producer_type == producer::Type::AMQP) {
        log::error!(
            "Only kafka & amqp are currently supported as a producer_type, more to come soon!"
        );
    }

    // Setup the indexer
    let indexer = application::create_near_indexer();
    let block_stream = indexer.streamer();

    // Spawn consumers based on specified type
    match producer_type {
        producer::Type::Kafka => {
            let mut producer = KafkaProducer::new().await;
            actix::spawn(async move { producer.consume(block_stream).await });
        }
        producer::Type::AMQP => {
            let mut producer = AMQPProducer::new().await;
            actix::spawn(async move { producer.consume(block_stream).await });
        }
        _ => log::error!("This should never happen, praise be to the aether."),
    }

    // Wait til a SIG-INT
    tokio::signal::ctrl_c().await?;
    log::info!("Shutting down nymdex...");

    Ok(())
}
