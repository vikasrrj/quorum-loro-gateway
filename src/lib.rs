//! Phase 1 Quorum Loro Gateway.
//!
//! The crate exposes the official Loro Synchronization Protocol v1 over
//! WebSocket and stores versioned update frames in one Ursula stream per room.

pub mod actor;
pub mod frame;
pub mod names;
pub mod protocol;
pub mod server;
pub mod ursula;

pub use actor::RoomManager;
pub use server::ServerConfig;
pub use server::app;
pub use server::app_with_config;
pub use ursula::HttpUrsula;
pub use ursula::HttpUrsulaConfig;
