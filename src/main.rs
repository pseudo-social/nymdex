mod logger;
mod application;
mod producer;

use kafka::client::{Compression, KafkaClient};
use producer::{KafkaConfig, KafkaProducer, Producer, ProducerType};
use std::{time::Duration};

#[actix::main]
#[doc(hidden)]
/// Where the magic happens 🌌
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup the indexer
    application::initialize()?;

    // Setup the producer
    let producer_type = producer::get_type()?;
    if producer_type != ProducerType::Kafka {
        log::error!("Only kafka is currently supported as a producer_type");
    }

    // Configure kafka
    let kafka_configuration = KafkaConfig {
        client_id: "nym-test-client".into(),
        brokers: vec!["localhost:9092".into()],
        topic: "blocks".into(),
        compression: Compression::NONE,
        required_acks: kafka::client::RequiredAcks::One,
        conn_idle_timeout: Duration::from_secs(15),
        ack_timeout: Duration::from_secs(5),
    };
    log::info!("{:?}", kafka_configuration);

    // Build our kafkaesque nightmare!
    let kafka_client = KafkaClient::new(kafka_configuration.clone().brokers.clone());
    let mut kafka_producer = KafkaProducer::new(kafka_client, kafka_configuration.clone());

    // Setup the indexer
    let indexer = application::create_near_indexer();
    let block_stream = indexer.streamer();
    
    // Consume the incoming messages
    actix::spawn(async move { kafka_producer.consume(block_stream).await });

    // Wait til a SIG-INT
    tokio::signal::ctrl_c().await?;
    log::info!("Shutting down nymdex...");

    Ok(())
}