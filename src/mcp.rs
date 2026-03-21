use std::sync::Arc;

use rmcp::{
    ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{ServerCapabilities, ServerInfo},
    schemars, tool, tool_handler, tool_router,
};

use criome_cozo::CriomeDb;
use criome_store::Store;

// Re-export core param types
pub use samskara_core::mcp::{
    QueryParams, DescribeRelationParams, CommitWorldParams, RestoreWorldParams,
};

// ── Store-specific param types ───────────────────────────────────

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct StoreIngestParams {
    /// Filesystem path of the file to ingest
    pub path: String,
    /// MIME type of the content
    pub media_type: String,
    /// Origin system (e.g. "annas-archive", "local")
    #[serde(default)]
    pub origin: Option<String>,
    /// Reference in origin system (e.g. anna's archive MD5)
    #[serde(default)]
    pub origin_ref: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct StoreExportParams {
    /// Blake3 content hash (hex)
    pub hash: String,
    /// Destination filesystem path
    pub dest: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct StoreMetaParams {
    /// Blake3 content hash (hex)
    pub hash: String,
}

// ── Server struct ────────────────────────────────────────────────

#[derive(Clone)]
pub struct StoredMcp {
    db: Arc<CriomeDb>,
    store: Arc<Store>,
    tool_router: ToolRouter<Self>,
}

impl StoredMcp {
    pub fn new(db: Arc<CriomeDb>, store: Arc<Store>) -> Self {
        Self {
            db,
            store,
            tool_router: Self::tool_router(),
        }
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for StoredMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            instructions: Some(
                "criome-stored — content-addressed store agent. Ingest, export, \
                 and query blobs by blake3 hash. World state is version-controlled."
                    .into(),
            ),
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            ..Default::default()
        }
    }
}

#[tool_router]
impl StoredMcp {
    // ── Generic tools (delegated to samskara-core) ───────────────

    #[tool(description = "Execute CozoScript against the store database. Returns CozoScript tuples.")]
    async fn query(&self, Parameters(params): Parameters<QueryParams>) -> String {
        samskara_core::mcp::query(self.db.clone(), params.script).await
    }

    #[tool(description = "List all stored relations in the database.")]
    async fn list_relations(&self) -> String {
        samskara_core::mcp::list_relations(self.db.clone()).await
    }

    #[tool(description = "Show the schema (columns and types) of a specific relation.")]
    async fn describe_relation(
        &self,
        Parameters(params): Parameters<DescribeRelationParams>,
    ) -> String {
        samskara_core::mcp::describe_relation(self.db.clone(), params.name).await
    }

    #[tool(description = "Commit the current world state.")]
    async fn commit_world(
        &self,
        Parameters(params): Parameters<CommitWorldParams>,
    ) -> String {
        samskara_core::mcp::commit_world(self.db.clone(), params).await
    }

    #[tool(description = "Restore the world state to a specific commit.")]
    async fn restore_world(
        &self,
        Parameters(params): Parameters<RestoreWorldParams>,
    ) -> String {
        samskara_core::mcp::restore_world(self.db.clone(), params.commit_id).await
    }

    // ── Store-specific tools ─────────────────────────────────────

    #[tool(description = "Ingest a file from a filesystem path into the content-addressed store. Returns blake3 hash and size.")]
    async fn store_ingest(
        &self,
        Parameters(params): Parameters<StoreIngestParams>,
    ) -> String {
        let store = self.store.clone();
        let db = self.db.clone();
        let result = tokio::task::spawn_blocking(move || {
            let data = std::fs::read(&params.path)
                .map_err(|e| format!("read failed: {e}"))?;

            let hash = store.put(&data).map_err(|e| e.to_string())?;
            let meta = store.meta(&hash).map_err(|e| e.to_string())?
                .ok_or("blob stored but meta missing")?;

            let esc = |s: &str| s.replace('\\', "\\\\").replace('"', "\\\"");
            let origin = params.origin.as_deref().unwrap_or("local");
            let origin_ref = params.origin_ref.as_deref().unwrap_or("");
            let now = chrono::Utc::now().to_rfc3339();

            // Write blob index
            let script = format!(
                r#"?[hash, size, compressed_size, media_type, origin, origin_ref, phase, dignity] <- [[
                    "{}", {}, {}, "{}", "{}", "{}", "manifest", "seen"
                ]]
                :put blob {{ hash => size, compressed_size, media_type, origin, origin_ref, phase, dignity }}"#,
                hash.to_hex(), meta.size, meta.compressed_size,
                esc(&params.media_type), esc(origin), esc(origin_ref),
            );
            db.run_script_raw(&script).map_err(|e| e.to_string())?;

            // Write ingestion log
            let ingest_id = blake3::hash(format!("{}:{}", hash.to_hex(), now).as_bytes())
                .to_hex().to_string();
            let log_script = format!(
                r#"?[id, source_path, hash, media_type, origin, origin_ref, ts, phase, dignity] <- [[
                    "{}", "{}", "{}", "{}", "{}", "{}", "{}", "manifest", "seen"
                ]]
                :put ingestion {{ id => source_path, hash, media_type, origin, origin_ref, ts, phase, dignity }}"#,
                esc(&ingest_id), esc(&params.path), hash.to_hex(),
                esc(&params.media_type), esc(origin), esc(origin_ref), esc(&now),
            );
            db.run_script_raw(&log_script).map_err(|e| e.to_string())?;

            Ok::<String, String>(format!(
                "[hash,size,compressed_size]\n[\"{}\",{},{}]",
                hash.to_hex(), meta.size, meta.compressed_size,
            ))
        })
        .await;

        match result {
            Ok(Ok(msg)) => msg,
            Ok(Err(e)) => format!("error: {e}"),
            Err(e) => format!("error: task join failed: {e}"),
        }
    }

    #[tool(description = "Export a blob from the store to a filesystem path.")]
    async fn store_export(
        &self,
        Parameters(params): Parameters<StoreExportParams>,
    ) -> String {
        let store = self.store.clone();
        let result = tokio::task::spawn_blocking(move || {
            let hash = criome_store::ContentHash::from_hex(&params.hash)
                .ok_or_else(|| format!("invalid hash: {}", params.hash))?;
            let data = store.get(&hash).map_err(|e| e.to_string())?;
            std::fs::write(&params.dest, &data)
                .map_err(|e| format!("write failed: {e}"))?;
            Ok::<String, String>(format!(
                "[hash,size,path]\n[\"{}\",{},\"{}\"]",
                params.hash, data.len(), params.dest.replace('"', "\\\""),
            ))
        })
        .await;

        match result {
            Ok(Ok(msg)) => msg,
            Ok(Err(e)) => format!("error: {e}"),
            Err(e) => format!("error: task join failed: {e}"),
        }
    }

    #[tool(description = "Get metadata for a blob without retrieving content.")]
    async fn store_meta(
        &self,
        Parameters(params): Parameters<StoreMetaParams>,
    ) -> String {
        let db = self.db.clone();
        let result = tokio::task::spawn_blocking(move || {
            let esc = |s: &str| s.replace('\\', "\\\\").replace('"', "\\\"");
            let script = format!(
                r#"?[hash, size, compressed_size, media_type, origin, origin_ref] :=
                    *blob{{hash, size, compressed_size, media_type, origin, origin_ref}},
                    hash = "{}""#,
                esc(&params.hash),
            );
            db.run_script_cozo(&script).map_err(|e| e.to_string())
        })
        .await;

        match result {
            Ok(Ok(text)) => text,
            Ok(Err(e)) => format!("error: {e}"),
            Err(e) => format!("error: task join failed: {e}"),
        }
    }
}
