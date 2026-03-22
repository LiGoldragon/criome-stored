use std::sync::Arc;

use clap::Parser;
use std::path::PathBuf;

use samskara_core::boot;

/// criome-stored — content-addressed store agent.
/// Runs as an MCP server over stdio. Owns a CozoDB for blob metadata
/// and a criome-store for the actual content-addressed bytes.
#[derive(Parser)]
#[command(name = "criome-stored", about = "Content-addressed store agent — MCP server mode")]
struct Args {
    /// Path to the sqlite-backed CozoDB database.
    #[arg(long, value_name = "DB_PATH")]
    db_path: Option<PathBuf>,

    /// Path to the content-addressed store directory.
    #[arg(long, value_name = "STORE_PATH")]
    store_path: Option<PathBuf>,

    /// Use an in-memory database instead of sqlite.
    #[arg(long)]
    memory: bool,
}

/// Relations with eternal dignity in this agent.
const ETERNAL_RELATIONS: &[&str] = &[
    "Dignity", "Phase",
];


/// Run the full genesis sequence for the store agent.
fn genesis(db: &criome_cozo::CriomeDb) -> Result<(), Box<dyn std::error::Error>> {
    tracing::info!("fresh database — running genesis");

    // 1. Core infrastructure (Phase, Dignity, world_schema, VCS)
    boot::core_genesis(db)?;

    // 2. Store contract relations
    criome_store_contract::init(db)?;
    tracing::info!("store contract relations loaded");

    // 3. Store domain relations (blob, ingestion, blob_tag)
    boot::load_cozo_script(db, include_str!("../schema/stored-world-init.cozo"))?;
    tracing::info!("store domain relations created");

    // 4. Finalize
    boot::finalize_genesis(db, ETERNAL_RELATIONS)?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();

    let args = Args::parse();

    // Default paths under ~/.criome/
    let home = dirs::home_dir().expect("no home directory");
    let criome_dir = home.join(".criome");

    let db_path = args.db_path.unwrap_or_else(|| criome_dir.join("stored.db"));
    let store_path = args.store_path.unwrap_or_else(|| criome_dir.join("store"));

    // Open CriomeDb
    let db = if args.memory {
        tracing::info!("opening in-memory db");
        criome_cozo::CriomeDb::open_memory()?
    } else {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        tracing::info!("opening sqlite db at {}", db_path.display());
        criome_cozo::CriomeDb::open_sqlite(&db_path)?
    };

    // Idempotent boot
    if boot::is_initialized(&db) {
        tracing::info!("database already initialized — skipping genesis");
    } else {
        genesis(&db)?;
    }

    // Open content-addressed store
    let store = criome_store::Store::open(&store_path)?;
    tracing::info!("content store at {}", store_path.display());

    // Start MCP server on stdio
    let db = Arc::new(db);
    let store = Arc::new(store);
    let server = crate::mcp::StoredMcp::new(db, store);

    tracing::info!("criome-stored MCP server starting on stdio");
    let service = rmcp::ServiceExt::serve(server, rmcp::transport::stdio()).await?;
    service.waiting().await?;

    Ok(())
}

mod mcp;
