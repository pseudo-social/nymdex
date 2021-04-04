//! A an implementation of `Producer` for any AMQP service

use std::sync::{Arc, Mutex};

use crate::producer::{get_producer_client_id, get_producer_url, Producer};

use super::get_producer_queue_name;

/// AMQP configuration
#[derive(Debug, Clone)]
pub struct Config {
    pub client_id: String, // Rename this
    pub queue_name: String,
}
#[allow(dead_code)]
/// AMQP producer
pub struct AMQPProducer {
    connection: Arc<Mutex<lapin::Connection>>,
}

/// The implementation for a AMQP producer
#[async_trait::async_trait]
impl Producer for AMQPProducer {
    type Error = Box<dyn std::error::Error>;

    async fn new() -> Self {
        // Configure the AMQP client
        let configuration = Config {
            client_id: get_producer_client_id(),
            queue_name: get_producer_queue_name(),
        };

        // Configure the AMQP connection
        let connection = lapin::Connection::connect(
            get_producer_url().as_str(),
            lapin::ConnectionProperties::default(),
        )
        .await
        .expect("Failed to connect to the AMQP client");

        // Create the queue or log it's existence
        let create_channel = connection.create_channel().await.unwrap().queue_declare(
            configuration.queue_name.as_str(),
            lapin::options::QueueDeclareOptions::default(),
            lapin::types::FieldTable::default(),
        );

        match create_channel.await {
            Ok(_) => log::info!("Created queue with name \"{}\"", configuration.queue_name),
            Err(_) => log::info!(
                "Used already existing queue with name \"{}\"",
                configuration.queue_name
            ),
        }

        Self {
            connection: Arc::new(Mutex::new(connection)),
        }
    }

    fn produce(&self, message: near_indexer::StreamerMessage) -> Result<(), Self::Error> {
        let _json = serde_json::to_string(&message).unwrap();

        Ok(())
    }

    async fn consume(
        &mut self,
        mut streamer: tokio::sync::mpsc::Receiver<near_indexer::StreamerMessage>,
    ) {
        while let Some(message) = streamer.recv().await {
            self.produce(message)
                .expect("Failed to produce AMQP message");
        }
    }
}
