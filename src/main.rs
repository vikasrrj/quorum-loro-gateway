use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use quorum_loro_gateway::HttpUrsula;
use quorum_loro_gateway::HttpUrsulaConfig;
use quorum_loro_gateway::RoomManager;
use quorum_loro_gateway::actor::ActorConfig;
use quorum_loro_gateway::app;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(version, about = "Loro protocol gateway backed by Ursula")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:8080")]
    listen: SocketAddr,
    #[arg(long, default_value = "http://127.0.0.1:4437")]
    ursula_url: String,
    #[arg(long, default_value = "qloro")]
    ursula_bucket: String,
    #[arg(long, default_value_t = 30)]
    ursula_timeout_seconds: u64,
    #[arg(long, default_value_t = 5)]
    ambiguous_retries: usize,
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
    let store = Arc::new(HttpUrsula::new(HttpUrsulaConfig {
        base_url: args.ursula_url,
        bucket: args.ursula_bucket,
        response_timeout: Duration::from_secs(args.ursula_timeout_seconds),
        ..HttpUrsulaConfig::default()
    })?);
    let rooms = RoomManager::with_random_boot(
        store,
        ActorConfig {
            ambiguous_retries: args.ambiguous_retries,
            ..ActorConfig::default()
        },
    );
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    tracing::info!(listen = %args.listen, "quorum-loro-gateway starting");
    axum::serve(listener, app(rooms))
        .with_graceful_shutdown(shutdown())
        .await?;
    Ok(())
}

async fn shutdown() {
    let _ = tokio::signal::ctrl_c().await;
}
