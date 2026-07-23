use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use quorum_loro_gateway::HttpUrsula;
use quorum_loro_gateway::HttpUrsulaConfig;
use quorum_loro_gateway::RoomManager;
use quorum_loro_gateway::ServerConfig;
use quorum_loro_gateway::actor::ActorConfig;
use quorum_loro_gateway::app_with_config;
use quorum_loro_gateway::frame::FrameLimits;
use quorum_loro_gateway::protocol::ProtocolLimits;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(version, about = "Loro protocol gateway backed by Ursula")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:8080")]
    listen: SocketAddr,
    #[arg(long, default_value = "http://127.0.0.1:4437")]
    ursula_url: String,
    #[arg(long = "ursula-peer-url")]
    ursula_peer_urls: Vec<String>,
    #[arg(long, default_value_t = 4)]
    ursula_max_redirects: usize,
    #[arg(long, default_value = "qloro")]
    ursula_bucket: String,
    #[arg(long, default_value_t = 30)]
    ursula_timeout_seconds: u64,
    #[arg(long, default_value_t = 5)]
    ambiguous_retries: usize,
    #[arg(long, default_value_t = 3)]
    safe_read_retries: usize,
    #[arg(long, default_value_t = 1_048_576)]
    ursula_read_chunk_bytes: usize,
    #[arg(long, default_value_t = 536_870_912)]
    max_room_history_bytes: usize,
    #[arg(long, default_value_t = 67_108_864)]
    max_frame_bytes: usize,
    #[arg(long, default_value_t = 33_554_432)]
    max_update_bytes: usize,
    #[arg(long, default_value_t = 4096)]
    max_updates_per_batch: usize,
    #[arg(long, default_value_t = 8)]
    max_fragment_batches: usize,
    #[arg(long, default_value_t = 4096)]
    max_fragments_per_batch: u64,
    #[arg(long, default_value_t = 67_108_864)]
    max_fragment_bytes_per_connection: u64,
    #[arg(long, default_value_t = 10)]
    fragment_timeout_seconds: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("quorum_loro_gateway=info")),
        )
        .init();
    let args = Args::parse();
    anyhow::ensure!(
        args.max_room_history_bytes > 0,
        "room history limit must be non-zero"
    );
    anyhow::ensure!(args.max_frame_bytes > 0, "frame limit must be non-zero");
    anyhow::ensure!(args.max_update_bytes > 0, "update limit must be non-zero");
    anyhow::ensure!(
        args.max_updates_per_batch > 0,
        "update count limit must be non-zero"
    );
    let max_update_bytes_u64 = u64::try_from(args.max_update_bytes)
        .map_err(|_| anyhow::anyhow!("update limit does not fit u64"))?;
    let store = Arc::new(HttpUrsula::new(HttpUrsulaConfig {
        base_url: args.ursula_url,
        redirect_base_urls: args.ursula_peer_urls,
        max_redirects: args.ursula_max_redirects,
        bucket: args.ursula_bucket,
        response_timeout: Duration::from_secs(args.ursula_timeout_seconds),
        read_chunk_bytes: args.ursula_read_chunk_bytes,
        max_stream_bytes: args.max_room_history_bytes,
        safe_retries: args.safe_read_retries,
        ..HttpUrsulaConfig::default()
    })?);
    let rooms = RoomManager::with_random_boot(
        store,
        ActorConfig {
            ambiguous_retries: args.ambiguous_retries,
            frame_limits: FrameLimits {
                max_frame_bytes: args.max_frame_bytes,
                max_updates: args.max_updates_per_batch,
                max_update_bytes: args.max_update_bytes,
                max_updates_bytes: args.max_update_bytes,
                max_stream_bytes: args.max_room_history_bytes,
                ..FrameLimits::default()
            },
            ..ActorConfig::default()
        },
    );
    let server_config = ServerConfig {
        protocol_limits: ProtocolLimits {
            max_updates: args.max_updates_per_batch,
            max_update_bytes: args.max_update_bytes,
            max_updates_bytes: args.max_update_bytes,
            ..ProtocolLimits::default()
        },
        max_fragment_batches: args.max_fragment_batches,
        max_fragments_per_batch: args.max_fragments_per_batch,
        max_reassembled_bytes: max_update_bytes_u64,
        max_fragment_bytes_per_connection: args.max_fragment_bytes_per_connection,
        fragment_timeout: Duration::from_secs(args.fragment_timeout_seconds),
    };
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    tracing::info!(listen = %args.listen, "quorum-loro-gateway starting");
    axum::serve(listener, app_with_config(rooms, server_config))
        .with_graceful_shutdown(shutdown())
        .await?;
    Ok(())
}

async fn shutdown() {
    let _ = tokio::signal::ctrl_c().await;
}
