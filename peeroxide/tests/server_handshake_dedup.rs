//! A swarm server answers every copy of a PEER_HANDSHAKE with the same reply.
//!
//! Node hyperdht clients send one Noise handshake message through up to two
//! relays in parallel and act on the first reply that arrives (`lib/connect.js`),
//! so a server must answer each copy identically, as Node's
//! `Server._onpeerhandshake` does. Here a Rust client sends the same message
//! twice and checks that both replies are byte-identical and usable.

use std::time::Duration;

use libudx::UdxRuntime;
use peeroxide::{spawn as spawn_swarm, JoinOpts, SwarmConfig};
use peeroxide_dht::crypto::hash;
use peeroxide_dht::hyperdht::{spawn, HyperDhtConfig, KeyPair};
use peeroxide_dht::hyperdht_messages::{
    decode_handshake_from_bytes, NoisePayload, UdxInfo, FIREWALL_UNKNOWN, PEER_HANDSHAKE,
};
use peeroxide_dht::messages::Ipv4Peer;
use peeroxide_dht::noise::Keypair as NoiseKeypair;
use peeroxide_dht::noise_wrap::NoiseWrap;
use peeroxide_dht::rpc::{DhtConfig, UserRequestParams};
use peeroxide_dht::Router;

fn local_dht(bootstrap: Vec<String>, firewalled: bool) -> HyperDhtConfig {
    let mut dht = DhtConfig::default();
    dht.bootstrap = bootstrap;
    dht.port = 0;
    dht.host = "127.0.0.1".to_string();
    dht.firewalled = firewalled;
    let mut config = HyperDhtConfig::default();
    config.dht = dht;
    config
}

#[tokio::test]
async fn duplicate_peer_handshake_gets_the_same_reply() {
    let _ = tracing_subscriber::fmt::try_init();

    let result = tokio::time::timeout(Duration::from_secs(30), run()).await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("duplicate handshake test failed: {e}"),
        Err(_) => panic!("duplicate handshake test timed out after 30s"),
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let rt = UdxRuntime::new()?;

    let (_bs_join, bs, _bs_rx) = spawn(&rt, local_dht(vec![], true)).await?;
    let bootstrap = vec![format!("127.0.0.1:{}", bs.local_port().await?)];

    // The server: a swarm announcing a topic.
    let mut config = SwarmConfig::default();
    config.dht = local_dht(bootstrap.clone(), true);
    let (_task, server, _conn_rx) = spawn_swarm(config).await?;
    server
        .join([0x5a; 32], {
            let mut opts = JoinOpts::default();
            opts.client = false;
            opts
        })
        .await?;
    let server_pk = server.key_pair().public_key;
    let server_addr = Ipv4Peer {
        host: "127.0.0.1".to_string(),
        port: server.dht().local_port().await?,
    };

    // The client: one Noise message, sent twice.
    let (_cli_join, client, _cli_rx) = spawn(&rt, local_dht(bootstrap, true)).await?;
    client.bootstrapped().await?;

    let client_kp = KeyPair::generate();
    let mut nw = NoiseWrap::new_initiator(
        NoiseKeypair {
            public_key: client_kp.public_key,
            secret_key: client_kp.secret_key,
        },
        server_pk,
    );
    let noise = nw.send(&NoisePayload {
        version: 1,
        error: 0,
        firewall: FIREWALL_UNKNOWN,
        holepunch: None,
        addresses4: vec![],
        addresses6: vec![],
        udx: Some(UdxInfo {
            version: 1,
            reusable_socket: false,
            id: 1,
            seq: 0,
        }),
        secret_stream: None,
        relay_through: None,
        relay_addresses: None,
    })?;
    let request = Router::encode_client_handshake(noise, None, Some(server_addr.clone()))?;

    let mut replies = Vec::new();
    for _ in 0..2 {
        let mut params = UserRequestParams::default();
        params.command = PEER_HANDSHAKE;
        params.target = Some(hash(&server_pk));
        params.value = Some(request.clone());
        let resp = client
            .dht()
            .request(params, &server_addr.host, server_addr.port)
            .await?;
        assert_eq!(resp.error, 0, "server returned error code {}", resp.error);
        replies.push(resp.value.ok_or("handshake reply without a value")?);
    }

    let first = decode_handshake_from_bytes(&replies[0])?;
    let second = decode_handshake_from_bytes(&replies[1])?;
    assert_eq!(
        first.noise, second.noise,
        "both copies of one handshake must get the same Noise reply"
    );

    // And that reply completes the client's handshake with the server's key.
    let payload = nw.recv(&first.noise)?;
    assert_eq!(payload.error, 0);
    assert_eq!(nw.finalize()?.remote_public_key, server_pk);

    server.destroy().await?;
    client.destroy().await?;
    bs.destroy().await?;
    Ok(())
}
