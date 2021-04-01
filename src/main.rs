mod logger;
mod producer;

use producer::{ProducerType};

#[actix::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup the indexer
    initialize()?;

    // Setup the producer
    let producer_type = producer::get_type()?;
    if producer_type != ProducerType::Kafka {
        log::error!("Only kafka is currently supported as a producer_type");
    }

    // Setup the indexer
    let indexer = create_near_indexer();
    let block_stream = indexer.streamer();
    
    // Spawn the block_consumer future
    actix::spawn(block_consumer(block_stream));
    // actix::spawn(block_producer(producer_type));

    // Wait til a SIG-INT
    tokio::signal::ctrl_c().await?;
    log::info!("Shutting down nymdex...");

    Ok(())
}

/// Initialize logging and load environment variables
fn initialize() -> Result<(), Box<dyn std::error::Error>> { 
    dotenv::dotenv()?;
    logger::init(None);

    Ok(())
}

/// Create the NEAR indexer configuration
fn configure_near_indexer(sync_mode: Option<near_indexer::SyncModeEnum>) -> near_indexer::IndexerConfig {
    near_indexer::IndexerConfig {
        home_dir: std::path::PathBuf::from(near_indexer::get_default_home()),
        sync_mode: sync_mode.unwrap_or(near_indexer::SyncModeEnum::FromInterruption),
        await_for_node_synced: near_indexer::AwaitForNodeSyncedEnum::WaitForFullSync,
    }
}

/// Configure the NEAR indexer with default settings
fn configure_near_indexer_default() -> near_indexer::IndexerConfig {
    configure_near_indexer(None)
}

/// Create the near indexer clients
fn create_near_indexer() -> near_indexer::Indexer {
    let indexer_config = configure_near_indexer_default();
    near_indexer::Indexer::new(indexer_config)
}

// Handle the incoming blocks from the Receiver
async fn block_consumer(mut stream: tokio::sync::mpsc::Receiver<near_indexer::StreamerMessage>) {
    while let Some(streamer_message) = stream.recv().await {
        eprintln!("{}", serde_json::to_value(streamer_message).unwrap());
    }
}