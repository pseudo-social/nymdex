mod logger;
mod application;
mod producer;

use producer::{ProducerType};

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

    // Setup the indexer
    let indexer = application::create_near_indexer();
    let block_stream = indexer.streamer();
    
    // Spawn the block_consumer future
    actix::spawn(application::block_consumer(block_stream));

    // Wait til a SIG-INT
    tokio::signal::ctrl_c().await?;
    log::info!("Shutting down nymdex...");

    Ok(())
}