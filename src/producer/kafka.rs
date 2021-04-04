//! A an implementation of `Producer` for `kafka`

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use kafka::client::{Compression, RequiredAcks};
use kafka::producer::Record;

use crate::producer::{
    get_producer_client_id, get_producer_queue_name, get_producer_url, Producer,
};

/// Kafka configuration
#[derive(Debug, Clone)]
pub struct Config {
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
impl Producer for KafkaProducer {
    type Error = kafka::Error;

    fn new() -> Self {
        // Configure kafka
        let configuration = Config {
            client_id: get_producer_client_id().into(),
            brokers: vec![get_producer_url().into()],
            compression: kafka::client::Compression::NONE,
            required_acks: kafka::client::RequiredAcks::One,
            conn_idle_timeout: Duration::from_secs(15),
            ack_timeout: Duration::from_secs(5),
        };
        // Log the configuration
        log::info!("{:?}", configuration);

        // Create a kafka client
        let mut client = kafka::client::KafkaClient::new(configuration.clone().brokers.clone());
        // Set the client_id from our configuration
        client.set_client_id(configuration.client_id.clone());
        // IMPORTANT: This actually creates the connection the kafka broker.
        client
            .load_metadata_all()
            .expect("Failed to connect/load metadata for the KafkaClient");

        // Build our producer from the client we've already created
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

    fn produce(&self, message: near_indexer::StreamerMessage) -> Result<(), Self::Error> {
        // Build our kafka record to produce
        let json = serde_json::to_string(&message).unwrap();
        let topic = get_producer_queue_name();
        let record = Record::from_value(&topic.as_str(), json.clone());

        // Avoid processing the logs at all if level is not Info
        if log::log_enabled!(log::Level::Info) {
            log::info!("Producing Kafka message: {}", json.clone());
        }

        // Produce to the topic!
        self.producer.lock().unwrap().send(&record)
    }

    async fn consume(
        &mut self,
        mut streamer: tokio::sync::mpsc::Receiver<near_indexer::StreamerMessage>,
    ) {
        while let Some(message) = streamer.recv().await {
            self.produce(message)
                .expect("Failed to produce Kafka message");
        }
    }
}
