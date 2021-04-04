//!
//! Helpers and the generic `Producer` trait
//!

pub mod amqp;
pub mod kafka;

/// Get the type of producer from the environment variables, fail if not supported or invalid
pub fn get_type() -> Result<Type, Box<dyn std::error::Error>> {
    let value = std::env::var("PRODUCER_TYPE")?;

    match value.as_str() {
        "kafka" => Ok(Type::Kafka),
        "amqp" => Ok(Type::AMQP),
        // TODO: Implement me!
        // "redis" => Ok(Type::Redis),
        // "grpc" => Ok(Type::GRPC),
        _ => Err("Invalid producer type must be either: kafka, amqp, redis, grpc".into()),
    }
}

/// Get the producer client id from either the environment or the default "near_producer"
pub fn get_producer_client_id() -> String {
    match std::env::var("PRODUCER_CLIENT_ID") {
        Ok(result) => result,
        Err(_) => "near_producer".into(),
    }
}

/// Get the producer queue name from either the environment or the default "near_messages"
pub fn get_producer_queue_name() -> String {
    match std::env::var("PRODUCER_QUEUE_NAME") {
        Ok(result) => result,
        Err(_) => "near_messages".into(),
    }
}

/// Get the producer url from either the environment
pub fn get_producer_url() -> String {
    std::env::var("PRODUCER_URL")
        .expect("PRODUCER_URL must be set, how else could you connect?")
        .into()
}

/// Describes the available producer types
#[derive(PartialEq)]
#[allow(dead_code)]
pub enum Type {
    Kafka,
    AMQP,
    GRPC,
    Redis,
}

#[async_trait::async_trait]
/// A trait to abstract generic producers
pub trait Producer {
    /// Error type
    type Error;

    /// Create an instance of a producer
    async fn new() -> Self;
    /// Produce a message
    async fn produce(&self, message: near_indexer::StreamerMessage) -> Result<(), Self::Error>;
    /// Consume data from a MPSC receiver and call produce
    async fn consume(
        &mut self,
        streamer: tokio::sync::mpsc::Receiver<near_indexer::StreamerMessage>,
    );
}
