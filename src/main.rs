mod logger;
mod producer;

use producer::{ProducerType};

#[actix::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup general configration
    initialize()?;

    // Setup the producer
    let producer_type = get_producer_type()?;
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

fn initialize() -> Result<(), Box<dyn std::error::Error>> { 
    dotenv::dotenv()?;
    logger::init(None);

    Ok(())
}

fn configure_near_indexer(sync_mode: Option<near_indexer::SyncModeEnum>) -> near_indexer::IndexerConfig {
    near_indexer::IndexerConfig {
        home_dir: std::path::PathBuf::from(near_indexer::get_default_home()),
        sync_mode: sync_mode.unwrap_or(near_indexer::SyncModeEnum::FromInterruption),
        await_for_node_synced: near_indexer::AwaitForNodeSyncedEnum::WaitForFullSync,
    }
}

fn configure_near_indexer_default() -> near_indexer::IndexerConfig {
    configure_near_indexer(None)
}

fn create_near_indexer() -> near_indexer::Indexer {
    let indexer_config = configure_near_indexer_default();
    near_indexer::Indexer::new(indexer_config)
}

async fn block_consumer(mut stream: tokio::sync::mpsc::Receiver<near_indexer::StreamerMessage>) {
    while let Some(streamer_message) = stream.recv().await {
        eprintln!("{}", serde_json::to_value(streamer_message).unwrap());
    }
}

fn get_producer_type() -> Result<ProducerType, Box<dyn std::error::Error>> {
    let value = std::env::var("PRODUCER_TYPE")?;

    match value.as_str() {
        "kafka" => Ok(ProducerType::Kafka),
        "rabbitmq" => Ok(ProducerType::RabbitMQ),
        "redis" => Ok(ProducerType::Redis),
        "grpc" => Ok(ProducerType::GRPC),
        _ => Err("Invalid producer type must be either: kafka, rabbitmq, redis, grpc".into())
    }
}
