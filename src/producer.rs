//!
//! Abstract away who's receiving data by publishing it to an array of known producers.
//!

use std::{sync::{Arc, Mutex}, time::Duration};

use kafka::client::{KafkaClient, Compression, RequiredAcks};
use kafka::producer::{Record};

/// Get the type of producer from the environment variables, fail if not supported
pub fn get_type() -> Result<ProducerType, Box<dyn std::error::Error>> {
  let value = std::env::var("PRODUCER_TYPE")?;

  match value.as_str() {
      "kafka" => Ok(ProducerType::Kafka),
      // "rabbitmq" => Ok(ProducerType::RabbitMQ),
      // "redis" => Ok(ProducerType::Redis),
      // "grpc" => Ok(ProducerType::GRPC),
      _ => Err("Invalid producer type must be either: kafka, rabbitmq, redis, grpc".into())
  }
}

/// Describes the availabe producer types
#[allow(dead_code)]
#[derive(PartialEq)]
pub enum ProducerType {
    Kafka,
    RabbitMQ,
    GRPC,
    Redis,
}

/// A trait to abstract generic producers
pub trait Producer<T, C, E> {
  fn new(client: T, configuration: C) -> Self;
  fn produce(&self, message: near_indexer::StreamerMessage) -> Result<(), E>;
}

/// Kafka configuration
#[derive(Debug, Clone)]
pub struct KafkaConfig {
  client_id: String,
  brokers: Vec<String>,
  topic: String,
  input_file: Option<String>,
  compression: Compression,
  required_acks: RequiredAcks,
  batch_size: usize,
  conn_idle_timeout: Duration,
  ack_timeout: Duration,
}

/// Kafka producer
pub struct KafkaProducer {
  configuration: KafkaConfig,
  producer: Arc<Mutex<kafka::producer::Producer>>,
}

/// The implementation for a kafka producer
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
}
