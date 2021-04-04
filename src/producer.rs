//!
//! Abstract away who's receiving data by publishing it to an array of known producers.
//!

use std::{sync::{Arc, Mutex}, time::Duration};

use kafka::client::{KafkaClient, Compression, RequiredAcks};
use kafka::producer::{Record};

/// Get the type of producer from the environment variables, fail if not supported or invalid
pub fn get_type() -> Result<Type, Box<dyn std::error::Error>> {
  let value = std::env::var("PRODUCER_TYPE")?;

  match value.as_str() {
      "kafka" => Ok(Type::Kafka),
      // TODO: Implement me!
      // "rabbitmq" => Ok(Type::RabbitMQ),
      // "redis" => Ok(Type::Redis),
      // "grpc" => Ok(Type::GRPC),
      _ => Err("Invalid producer type must be either: kafka, rabbitmq, redis, grpc".into())
  }
}

/// Get the producer queue name from either the environment or the default "near_messages"
pub fn get_producer_queue_name() -> String {
  match std::env::var("PRODUCER_QUEUE_NAME") {
    Ok(result) => result,
    Err(_) => "near_messages".into()
  }
}

/// Describes the available producer types
#[derive(PartialEq)]
#[allow(dead_code)]
pub enum Type {
    Kafka,
    RabbitMQ,
    GRPC,
    Redis,
}

#[async_trait::async_trait]
/// A trait to abstract generic producers
pub trait Producer<T, C, E> {
  /// Create an instance of a producer
  fn new(client: T, configuration: C) -> Self;
  /// Produce a message
  fn produce(&self, message: near_indexer::StreamerMessage) -> Result<(), E>;
  /// Consume data from a MPSC receiver and call produce
  async fn consume(&mut self, streamer: tokio::sync::mpsc::Receiver<near_indexer::StreamerMessage>);
}

/// Kafka configuration
#[derive(Debug, Clone)]
pub struct KafkaConfig {
  pub client_id: String,
  pub brokers: Vec<String>,
  pub compression: Compression,
  pub required_acks: RequiredAcks,
  pub conn_idle_timeout: Duration,
  pub ack_timeout: Duration,
}

/// Kafka producer
pub struct KafkaProducer {
  producer: Arc<Mutex<kafka::producer::Producer>>,
}

/// The implementation for a kafka producer
#[async_trait::async_trait]
impl Producer<KafkaClient, KafkaConfig, kafka::Error> for KafkaProducer {
  fn new(mut client: KafkaClient, configuration: KafkaConfig) -> Self {
    client.set_client_id(configuration.client_id.clone());
    client.load_metadata_all().expect("Failed to connect/load metadata for the KafkaClient");

    let producer = kafka::producer::Producer::from_client(client)
      .with_ack_timeout(configuration.ack_timeout)
      .with_required_acks(configuration.required_acks)
      .with_compression(configuration.compression)
      .with_connection_idle_timeout(configuration.conn_idle_timeout)
      .create()
      .expect("Failed to initialize the producer");

    Self {
      producer: Arc::new(Mutex::new(producer)),
    }
  }

  fn produce(&self, message: near_indexer::StreamerMessage) -> Result<(), kafka::Error> {
    let json = serde_json::to_string(&message).unwrap();
    let topic = get_producer_queue_name();
    let record = Record::from_value(&topic.as_str(), json.clone());
    log::info!("Producing Kafka message: {}", json.clone());
    self.producer.lock().unwrap().send(&record)
  }

  async fn consume(&mut self, mut streamer: tokio::sync::mpsc::Receiver<near_indexer::StreamerMessage>) {
    while let Some(message) = streamer.recv().await {
      self.produce(message).expect("Failed to produce Kafka message");
    }
  }
}
