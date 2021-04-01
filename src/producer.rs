use std::{sync::Arc, time::Duration};

use kafka::client::{KafkaClient, Compression, RequiredAcks};
use kafka::producer::{Record};

#[derive(PartialEq)]
pub enum ProducerType {
    Kafka,
    RabbitMQ,
    GRPC,
    Redis,
}

pub trait Producer<C, E> {
    fn new(configuration: C) -> Self;
    fn produce(&self, message: near_indexer::StreamerMessage) -> Result<(), E>;
}


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

pub struct KafkaProducer {
  configuration: KafkaConfig,
  client: Arc<KafkaClient>,
  producer: Arc<kafka::producer::Producer>,
}

impl Producer<KafkaConfig, kafka::Error> for KafkaProducer {
  fn new(configuration: KafkaConfig) -> Self {
    let client = Arc::new(KafkaClient::new(configuration.brokers.clone()));
    client.set_client_id(configuration.client_id);

    let producer = kafka::producer::Producer::from_client(client.clone())
      .with_ack_timeout(configuration.ack_timeout)
      .with_required_acks(configuration.required_acks)
      .with_compression(configuration.compression)
      .with_connection_idle_timeout(configuration.conn_idle_timeout)
      .create()
      .expect("Failed to initialize the producer");

    Self {
      configuration: configuration.clone(),
      client,
      producer: Arc::new(producer),
    }
  }

  fn produce(&self, message: near_indexer::StreamerMessage) -> Result<(), kafka::Error> {
    let json = serde_json::to_string(&message).unwrap();
    let record = Record::from_value(&self.configuration.topic, json);
    self.producer.send(&record)
  }
}