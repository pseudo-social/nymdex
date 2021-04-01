use crate::logger;

/// Initialize logging and load environment variables
pub fn initialize() -> Result<(), Box<dyn std::error::Error>> { 
  dotenv::dotenv()?;
  logger::init(None);

  Ok(())
}

/// Create the NEAR indexer configuration
pub fn configure_near_indexer(sync_mode: Option<near_indexer::SyncModeEnum>) -> near_indexer::IndexerConfig {
  near_indexer::IndexerConfig {
      home_dir: std::path::PathBuf::from(near_indexer::get_default_home()),
      sync_mode: sync_mode.unwrap_or(near_indexer::SyncModeEnum::FromInterruption),
      await_for_node_synced: near_indexer::AwaitForNodeSyncedEnum::WaitForFullSync,
  }
}

/// Configure the NEAR indexer with default settings
pub fn configure_near_indexer_default() -> near_indexer::IndexerConfig {
  configure_near_indexer(None)
}

/// Create the near indexer clients
pub fn create_near_indexer() -> near_indexer::Indexer {
  let indexer_config = configure_near_indexer_default();
  near_indexer::Indexer::new(indexer_config)
}

// Handle the incoming blocks from the Receiver
pub async fn block_consumer(mut stream: tokio::sync::mpsc::Receiver<near_indexer::StreamerMessage>) {
  while let Some(streamer_message) = stream.recv().await {
      eprintln!("{}", serde_json::to_value(streamer_message).unwrap());
  }
}