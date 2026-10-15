//! M8 interop: Rust peeroxide (Hyperswarm) ↔ Node.js hyperswarm.
//!
//! `hyperswarm_cross_language_connect` (Rust client → Node server):
//! 1. Node.js creates a local testnet + Hyperswarm, joins a topic as server.
//! 2. Rust peeroxide joins the same topic as client, discovers the peer,
//!    and receives a SwarmConnection through the channel.
//!
//! Node client → Rust server, on the same local testnet:
//! 1. A firewalled Rust peeroxide swarm announces a topic as server.
//! 2. Node.js joins the topic as client and dials the Rust server.
//!
//! `hyperswarm_node_client_holepunches_to_rust_server`: a firewalled Node client
//! (with the same-host LAN shortcut off, as on another network) sends one
//! PEER_HANDSHAKE through two relays in parallel, then holepunches through the
//! relays listed in the server's reply.
//! `hyperswarm_open_node_client_connects_to_rust_server`: a reachable Node client
//! waits for the server to dial it, as Node servers do, instead of holepunching.

use std::io::{BufRead, Write};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use peeroxide::{discovery_key, spawn, JoinOpts, SwarmConfig};
use peeroxide_dht::hyperdht::HyperDhtConfig;
use peeroxide_dht::rpc::DhtConfig;
use serde_json::Value;

fn server_script_path() -> String {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    format!("{manifest}/../tests/node/hyperswarm-server.js")
}

struct JsSwarm {
    child: std::process::Child,
    stdin: std::process::ChildStdin,
    lines: Arc<Mutex<Vec<String>>>,
    _reader_thread: std::thread::JoinHandle<()>,
    next_id: u32,
}

impl JsSwarm {
    fn start() -> Self {
        Self::start_with(&[])
    }

    /// Start the helper with optional flags (see `tests/node/hyperswarm-server.js`).
    fn start_with(flags: &[&str]) -> Self {
        let mut child = Command::new("node")
            .arg(server_script_path())
            .args(flags)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("failed to spawn node hyperswarm-server.js");

        let stdout = child.stdout.take().expect("no stdout");
        let stdin = child.stdin.take().expect("no stdin");
        let lines: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let lines_clone = lines.clone();

        let reader_thread = std::thread::spawn(move || {
            let reader = std::io::BufReader::new(stdout);
            for line in reader.lines().map_while(Result::ok) {
                lines_clone.lock().unwrap().push(line);
            }
        });

        Self {
            child,
            stdin,
            lines,
            _reader_thread: reader_thread,
            next_id: 1,
        }
    }

    fn read_ready(&self) -> (u16, String) {
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            {
                let lines = self.lines.lock().unwrap();
                if let Some(line) = lines.first() {
                    let v: Value = serde_json::from_str(line).expect("invalid JSON from JS");
                    assert!(v["ready"].as_bool().unwrap_or(false), "JS not ready");
                    let port = v["port"].as_u64().expect("missing port") as u16;
                    let pk = v["publicKey"].as_str().expect("missing publicKey").to_string();
                    return (port, pk);
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for JS ready"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn send_cmd(&mut self, cmd: Value) -> u32 {
        let id = self.next_id;
        self.next_id += 1;
        let mut msg = cmd;
        msg["id"] = serde_json::json!(id);
        let line = serde_json::to_string(&msg).unwrap() + "\n";
        self.stdin
            .write_all(line.as_bytes())
            .expect("write to JS stdin");
        self.stdin.flush().expect("flush JS stdin");
        id
    }

    fn wait_reply(&self, id: u32) -> Value {
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            {
                let lines = self.lines.lock().unwrap();
                for line in lines.iter() {
                    if let Ok(v) = serde_json::from_str::<Value>(line) {
                        if v["id"].as_u64() == Some(id as u64) {
                            return v;
                        }
                    }
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for JS reply id={id}"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn wait_event(&self, event_name: &str) -> Value {
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            {
                let lines = self.lines.lock().unwrap();
                for line in lines.iter() {
                    if let Ok(v) = serde_json::from_str::<Value>(line) {
                        if v["event"].as_str() == Some(event_name) {
                            return v;
                        }
                    }
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for JS event '{event_name}'"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn join_server(&mut self, topic_hex: &str) {
        let id = self.send_cmd(serde_json::json!({ "cmd": "join", "topic": topic_hex }));
        let reply = self.wait_reply(id);
        assert!(
            reply["ok"].as_bool().unwrap_or(false),
            "JS join failed: {reply}"
        );
    }

    fn join_client(&mut self, topic_hex: &str) {
        let id = self.send_cmd(serde_json::json!({ "cmd": "join_client", "topic": topic_hex }));
        let reply = self.wait_reply(id);
        assert!(
            reply["ok"].as_bool().unwrap_or(false),
            "JS join_client failed: {reply}"
        );
    }

    /// Wait for a `data` event whose hex payload decodes to `expected`.
    fn wait_data(&self, expected: &str) -> Value {
        let want = hex::encode(expected.as_bytes());
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            {
                let lines = self.lines.lock().unwrap();
                for line in lines.iter() {
                    if let Ok(v) = serde_json::from_str::<Value>(line) {
                        if v["event"].as_str() == Some("data")
                            && v["payload"].as_str() == Some(want.as_str())
                        {
                            return v;
                        }
                    }
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for JS data event with payload {expected:?}"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

impl Drop for JsSwarm {
    fn drop(&mut self) {
        let _ = self
            .stdin
            .write_all(b"{\"cmd\":\"shutdown\",\"id\":0}\n");
        let _ = self.stdin.flush();
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test]
async fn hyperswarm_cross_language_connect() {
    let _ = tracing_subscriber::fmt::try_init();

    let result = tokio::time::timeout(Duration::from_secs(60), run_interop()).await;

    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("hyperswarm interop test failed: {e}"),
        Err(_) => panic!("hyperswarm interop test timed out after 60s"),
    }
}

async fn run_interop() -> Result<(), Box<dyn std::error::Error>> {
    let mut js = JsSwarm::start();
    let (bs_port, _server_pk) = js.read_ready();
    tracing::info!(bs_port, "JS testnet ready");

    let topic = discovery_key(b"peeroxide-m8-interop-test");
    let topic_hex: String = topic.iter().map(|b| format!("{b:02x}")).collect();

    js.join_server(&topic_hex);
    tracing::info!("JS joined topic as server");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut dht_config = DhtConfig::default();
    dht_config.bootstrap = vec![format!("127.0.0.1:{bs_port}")];
    dht_config.port = 0;
    dht_config.host = "127.0.0.1".to_string();
    dht_config.firewalled = true;

    let mut hyper_config = HyperDhtConfig::default();
    hyper_config.dht = dht_config;

    let mut config = SwarmConfig::default();
    config.dht = hyper_config;

    let (_task, handle, mut conn_rx) = spawn(config).await?;
    tracing::info!("Rust swarm started");

    handle
        .join(
            topic,
            {
                let mut opts = JoinOpts::default();
                opts.server = false;
                opts
            },
        )
        .await?;
    tracing::info!("Rust joined topic as client");

    let mut conn = tokio::time::timeout(Duration::from_secs(30), conn_rx.recv())
        .await
        .map_err(|_| "timed out waiting for SwarmConnection")?
        .ok_or("conn_rx closed without delivering a connection")?;

    tracing::info!(
        remote_pk = hex::encode(conn.remote_public_key()),
        is_initiator = conn.is_initiator,
        "Rust received SwarmConnection"
    );

    assert!(conn.is_initiator, "Rust should be the initiator (client)");

    let connected_event = js.wait_event("connected");
    tracing::info!("JS saw connection: {connected_event}");

    let data = tokio::time::timeout(Duration::from_secs(10), conn.peer.stream.read())
        .await
        .map_err(|_| "timed out reading from encrypted stream")?
        .map_err(|e| format!("read error: {e}"))?
        .ok_or("stream closed without data")?;

    let msg = String::from_utf8_lossy(&data);
    tracing::info!(msg = %msg, "Rust read message from JS");
    assert_eq!(msg, "hello from node", "unexpected message from JS");

    handle.destroy().await?;
    drop(js);

    Ok(())
}

#[tokio::test]
async fn hyperswarm_node_client_holepunches_to_rust_server() {
    node_client_interop(&["--remote-client"]).await;
}

#[tokio::test]
async fn hyperswarm_open_node_client_connects_to_rust_server() {
    node_client_interop(&["--open-client"]).await;
}

async fn node_client_interop(flags: &[&str]) {
    let _ = tracing_subscriber::fmt::try_init();

    let result =
        tokio::time::timeout(Duration::from_secs(90), run_node_client_interop(flags)).await;

    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("node client {flags:?} -> rust server interop test failed: {e}"),
        Err(_) => panic!("node client {flags:?} -> rust server interop test timed out after 90s"),
    }
}

async fn run_node_client_interop(flags: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
    let mut js = JsSwarm::start_with(flags);
    let (bs_port, _node_pk) = js.read_ready();
    tracing::info!(bs_port, "JS testnet ready");

    let topic = discovery_key(b"peeroxide-node-client-rust-server-test");
    let topic_hex = hex::encode(topic);

    // Firewalled like a peer behind a NAT, so the server stages a passive
    // holepunch that the Node client has to complete through the relays.
    let mut dht_config = DhtConfig::default();
    dht_config.bootstrap = vec![format!("127.0.0.1:{bs_port}")];
    dht_config.port = 0;
    dht_config.host = "127.0.0.1".to_string();
    dht_config.firewalled = true;

    let mut hyper_config = HyperDhtConfig::default();
    hyper_config.dht = dht_config;

    let mut config = SwarmConfig::default();
    config.dht = hyper_config;

    let (_task, handle, mut conn_rx) = spawn(config).await?;
    let rust_pk = hex::encode(handle.key_pair().public_key);

    handle
        .join(
            topic,
            {
                let mut opts = JoinOpts::default();
                opts.client = false;
                opts
            },
        )
        .await?;
    handle.flush().await?;
    tracing::info!("Rust announced topic as server");

    js.join_client(&topic_hex);
    tracing::info!("JS joined topic as client");

    let mut conn = tokio::time::timeout(Duration::from_secs(45), conn_rx.recv())
        .await
        .map_err(|_| "timed out waiting for the Node client's connection")?
        .ok_or("conn_rx closed without delivering a connection")?;

    assert!(!conn.is_initiator, "the Node client should be the initiator");

    let connected_event = js.wait_event("connected");
    assert_eq!(connected_event["remotePk"].as_str(), Some(rust_pk.as_str()));
    assert_eq!(connected_event["isInitiator"].as_bool(), Some(true));

    let data = tokio::time::timeout(Duration::from_secs(10), conn.peer.stream.read())
        .await
        .map_err(|_| "timed out reading from encrypted stream")?
        .map_err(|e| format!("read error: {e}"))?
        .ok_or("stream closed without data")?;
    assert_eq!(String::from_utf8_lossy(&data), "hello from node");

    conn.peer.stream.write(b"hello from rust").await?;
    js.wait_data("hello from rust");

    handle.destroy().await?;
    drop(js);

    Ok(())
}
