use diesel::{Connection, PgConnection};

mod logger;
mod schema;

#[actix::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    initialize()?;

    let database_connection = establish_database_connection()?;
    let indexer = create_near_indexer();
    let block_stream = indexer.streamer();
    
    actix::spawn(block_consumer(database_connection, block_stream)).await?;

    Ok(())
}

fn initialize() -> Result<(), Box<dyn std::error::Error>> { 
    dotenv::dotenv()?;
    logger::init(None);

    Ok(())
}

fn establish_database_connection() -> Result<PgConnection, Box<dyn std::error::Error>> {
    let database_url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set");
    Ok(PgConnection::establish(&database_url)
        .expect(&format!("Failed to connect postgres at {}", database_url)))
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

async fn block_consumer(_database_connection: PgConnection, mut stream: tokio::sync::mpsc::Receiver<near_indexer::StreamerMessage>) {
    while let Some(streamer_message) = stream.recv().await {
        eprintln!("{}", serde_json::to_value(streamer_message).unwrap());
    }
}