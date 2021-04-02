//!
//! Abstract away who's receiving data by publishing it to an array of known producers.
//!

use std::{sync::{Arc, Mutex}, time::Duration};

use kafka::client::{KafkaClient, Compression, RequiredAcks};
use kafka::producer::{Record};

/// Get the type of producer from the environment variables, fail if not supported or invalid
pub fn get_type() -> Result<ProducerType, Box<dyn std::error::Error>> {
  let value = std::env::var("PRODUCER_TYPE")?;

  match value.as_str() {
      "kafka" => Ok(ProducerType::Kafka),
      // TODO: Implement me!
      // "rabbitmq" => Ok(ProducerType::RabbitMQ),
      // "redis" => Ok(ProducerType::Redis),
      // "grpc" => Ok(ProducerType::GRPC),
      _ => Err("Invalid producer type must be either: kafka, rabbitmq, redis, grpc".into())
  }
}

/// Describes the available producer types
#[derive(PartialEq)]
#[allow(dead_code)]
pub enum ProducerType {
    Kafka,
    RabbitMQ,
    GRPC,
    Redis,
}

#[async_trait::async_trait]
/// A trait to abstract generic producers
pub trait Producer<T, C, E> {
  fn new(client: T, configuration: C) -> Self;
  fn produce(&self, message: near_indexer::StreamerMessage) -> Result<(), E>;
  async fn consume(&mut self, streamer: tokio::sync::mpsc::Receiver<near_indexer::StreamerMessage>);
}

/// Kafka configuration
#[derive(Debug, Clone)]
pub struct KafkaConfig {
  pub client_id: String,
  pub brokers: Vec<String>,
  pub topic: String,
  pub compression: Compression,
  pub required_acks: RequiredAcks,
  pub conn_idle_timeout: Duration,
  pub ack_timeout: Duration,
}

/// Kafka producer
pub struct KafkaProducer {
  configuration: KafkaConfig,
  producer: Arc<Mutex<kafka::producer::Producer>>,
}

/// The implementation for a kafka producer
#[async_trait::async_trait]
impl Producer<KafkaClient, KafkaConfig, kafka::Error> for KafkaProducer {
  fn new(mut client: KafkaClient, configuration: KafkaConfig) -> Self {
    client.set_client_id(configuration.client_id.clone());

    let producer = kafka::producer::Producer::from_client(client)
      .with_ack_timeout(configuration.ack_timeout)
      .with_required_acks(configuration.required_acks)
      .with_compression(configuration.compression)
      .with_connection_idle_timeout(configuration.conn_idle_timeout)
      .create()
      .expect("Failed to initialize the producer");

    Self {
      configuration,
      producer: Arc::new(Mutex::new(producer)),
    }
  }

  fn produce(&self, message: near_indexer::StreamerMessage) -> Result<(), kafka::Error> {
    let json = serde_json::to_string(&message).unwrap();
    let record = Record::from_value(self.configuration.topic.as_str(), json);
    self.producer.lock().unwrap().send(&record)
  }

  async fn consume(&mut self, mut streamer: tokio::sync::mpsc::Receiver<near_indexer::StreamerMessage>) {
    while let Some(message) = streamer.recv().await {
      self.produce(message).expect("Failed to produce Kafka message");
    }
  }
}
