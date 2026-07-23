use std::path::Path;
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::time::Duration;

use anyhow::Context;
use futures_util::SinkExt;
use futures_util::StreamExt;
use loro::ExportMode;
use loro::LoroDoc;
use loro_protocol::BatchId;
use loro_protocol::CrdtType;
use loro_protocol::ProtocolMessage;
use loro_protocol::UpdateStatusCode;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

const NODE_URLS: [&str; 3] = [
    "http://127.0.0.1:18101",
    "http://127.0.0.1:18102",
    "http://127.0.0.1:18103",
];

struct ClusterGuard {
    root: PathBuf,
}

impl ClusterGuard {
    fn start() -> anyhow::Result<Self> {
        let root = std::env::temp_dir()
            .join(format!("qlg-phase2-{}", uuid::Uuid::new_v4()))
            .join("phase2-cluster");
        let output = Command::new(script("phase2-cluster-start.sh"))
            .env("PHASE2_CLUSTER_ROOT", &root)
            .output()
            .context("start three-node Ursula cluster")?;
        if !output.status.success() {
            anyhow::bail!(
                "cluster start failed:\nstdout: {}\nstderr: {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(Self { root })
    }

    fn kill_node(&self, node_id: u8) -> anyhow::Result<()> {
        let pid =
            std::fs::read_to_string(self.root.join("pids").join(format!("node-{node_id}.pid")))?;
        let status = Command::new("kill").arg("-KILL").arg(pid.trim()).status()?;
        anyhow::ensure!(status.success(), "failed to kill Ursula node {node_id}");
        Ok(())
    }
}

impl Drop for ClusterGuard {
    fn drop(&mut self) {
        for node_id in 1..=3 {
            let pid_path = self.root.join("pids").join(format!("node-{node_id}.pid"));
            if let Ok(pid) = std::fs::read_to_string(pid_path) {
                let _ = Command::new("kill")
                    .arg("-KILL")
                    .arg(pid.trim())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status();
            }
        }
        let _ = Command::new(script("phase2-cluster-clean.sh"))
            .env("PHASE2_CLUSTER_ROOT", &self.root)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        if let Some(parent) = self.root.parent() {
            let _ = std::fs::remove_dir(parent);
        }
    }
}

struct GatewayGuard {
    child: Child,
}

impl GatewayGuard {
    fn spawn(listen: std::net::SocketAddr) -> anyhow::Result<Self> {
        let mut command = Command::new(env!("CARGO_BIN_EXE_quorum-loro-gateway"));
        command
            .arg("--listen")
            .arg(listen.to_string())
            .arg("--ursula-url")
            .arg(NODE_URLS[1])
            .arg("--ursula-peer-url")
            .arg(NODE_URLS[0])
            .arg("--ursula-peer-url")
            .arg(NODE_URLS[1])
            .arg("--ursula-peer-url")
            .arg(NODE_URLS[2])
            .arg("--ursula-timeout-seconds")
            .arg("2")
            .arg("--ambiguous-retries")
            .arg("10")
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        Ok(Self {
            child: command.spawn().context("spawn gateway")?,
        })
    }

    fn kill_and_wait(&mut self) -> anyhow::Result<()> {
        self.child.kill().context("kill gateway")?;
        let _ = self.child.wait().context("wait for killed gateway")?;
        Ok(())
    }
}

impl Drop for GatewayGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn script(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join(name)
}

fn reserve_address() -> anyhow::Result<std::net::SocketAddr> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    drop(listener);
    Ok(address)
}

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect_when_ready(url: &str) -> anyhow::Result<Socket> {
    for _ in 0..200 {
        if let Ok((socket, _)) = connect_async(url).await {
            return Ok(socket);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    anyhow::bail!("gateway did not start at {url}")
}

async fn send_protocol(socket: &mut Socket, message: &ProtocolMessage) -> anyhow::Result<()> {
    let encoded = loro_protocol::encode(message).map_err(anyhow::Error::msg)?;
    socket.send(Message::Binary(encoded.into())).await?;
    Ok(())
}

async fn next_protocol(socket: &mut Socket) -> anyhow::Result<ProtocolMessage> {
    loop {
        let message = tokio::time::timeout(Duration::from_secs(10), socket.next())
            .await
            .context("WebSocket receive timeout")?
            .context("WebSocket closed")??;
        if let Message::Binary(bytes) = message {
            return loro_protocol::decode(&bytes).map_err(anyhow::Error::msg);
        }
    }
}

async fn join(socket: &mut Socket, room_id: &str) -> anyhow::Result<()> {
    send_protocol(
        socket,
        &ProtocolMessage::JoinRequest {
            crdt: CrdtType::Loro,
            room_id: room_id.into(),
            auth: Vec::new(),
            version: Vec::new(),
        },
    )
    .await?;
    loop {
        match next_protocol(socket).await? {
            ProtocolMessage::JoinResponseOk { .. } => return Ok(()),
            ProtocolMessage::JoinError { message, .. } => {
                anyhow::bail!("gateway join failed: {message}")
            }
            _ => {}
        }
    }
}

fn update_blob() -> Vec<u8> {
    let doc = LoroDoc::new();
    doc.set_peer_id(600).expect("set Phase 2 peer ID");
    doc.get_text("text")
        .insert(0, "survives-one-voter-failure")
        .expect("insert Phase 2 text");
    doc.commit();
    doc.export(ExportMode::all_updates())
        .expect("export Phase 2 update")
}

#[tokio::test]
#[ignore = "requires /home/vik/ursula/target/release/ursula and free Phase 2 ports"]
async fn acknowledged_update_survives_one_voter_and_gateway_crash() -> anyhow::Result<()> {
    let _cluster = ClusterGuard::start()?;
    let listen = reserve_address()?;
    let url = format!("ws://{listen}/ws");
    let mut gateway = GatewayGuard::spawn(listen)?;
    let mut socket = connect_when_ready(&url).await?;
    let room_id = format!("phase2-quorum-{}", uuid::Uuid::new_v4());
    join(&mut socket, &room_id).await?;

    send_protocol(
        &mut socket,
        &ProtocolMessage::DocUpdate {
            crdt: CrdtType::Loro,
            room_id: room_id.clone(),
            updates: vec![update_blob()],
            batch_id: BatchId([60; 8]),
        },
    )
    .await?;
    loop {
        if let ProtocolMessage::Ack { status, .. } = next_protocol(&mut socket).await? {
            anyhow::ensure!(
                status == UpdateStatusCode::Ok,
                "update was not acknowledged"
            );
            break;
        }
    }

    _cluster.kill_node(3)?;
    gateway.kill_and_wait()?;
    drop(socket);

    let mut restarted_gateway = GatewayGuard::spawn(listen)?;
    let mut restarted = connect_when_ready(&url).await?;
    join(&mut restarted, &room_id).await?;
    let updates = loop {
        if let ProtocolMessage::DocUpdate { updates, .. } = next_protocol(&mut restarted).await? {
            break updates;
        }
    };
    let recovered = LoroDoc::new();
    recovered.import_batch(&updates)?;
    anyhow::ensure!(
        recovered.get_text("text").to_string() == "survives-one-voter-failure",
        "acknowledged update was not reconstructed after voter and gateway failure"
    );

    restarted_gateway.kill_and_wait()?;
    Ok(())
}
