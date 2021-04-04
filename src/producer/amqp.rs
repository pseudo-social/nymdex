//! A an implementation of `Producer` for any AMQP service

use std::sync::{Arc, Mutex};

use tokio_amqp::*;

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
    configuration: Config,
    connection: Arc<Mutex<lapin::Connection>>,
    channel: Arc<Mutex<lapin::Channel>>,
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
            lapin::ConnectionProperties::default().with_tokio(),
        )
        .await
        .expect("Failed to connect to the AMQP client");

        // Create the AMQP channel
        connection
            .create_channel()
            .await
            .unwrap()
            .queue_declare(
                configuration.queue_name.as_str(),
                lapin::options::QueueDeclareOptions::default(),
                lapin::types::FieldTable::default(),
            )
            .await
            .unwrap();

        let channel = connection.create_channel().await.unwrap();

        Self {
            configuration,
            connection: Arc::new(Mutex::new(connection)),
            channel: Arc::new(Mutex::new(channel)),
        }
    }

    async fn produce(&self, message: near_indexer::StreamerMessage) -> Result<(), Self::Error> {
        // Build or AMQP queue entry
        let json = serde_json::to_string(&message).unwrap();
        let queue_name = get_producer_queue_name();

        // Avoid processing the logs at all if level is not Info
        if log::log_enabled!(log::Level::Info) {
            log::info!("Producing AMQP message: {}", json.clone());
        }

        // Build message publish future
        let published_message = self.channel.lock().unwrap().basic_publish(
            queue_name.as_str(),
            self.configuration.queue_name.as_str(),
            lapin::options::BasicPublishOptions::default(),
            json.as_bytes().clone().to_vec(),
            lapin::BasicProperties::default()
        );

        // Await the stupidly complicated chain of futures... 😭
        let confirmation = published_message.await?.await?;

        // Confirm the result from the publish
        match confirmation { 
            lapin::publisher_confirm::Confirmation::NotRequested => Ok(()),
            _ => Err("Failed to publish AMQP message".into()),
        }

    }

    async fn consume(
        &mut self,
        mut streamer: tokio::sync::mpsc::Receiver<near_indexer::StreamerMessage>,
    ) {
        while let Some(message) = streamer.recv().await {
            self.produce(message)
                .await
                .expect("Failed to produce AMQP message");
        }
    }
}
