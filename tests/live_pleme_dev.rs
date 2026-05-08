//! Integration test that runs against a live spire-agent socket.
//! Off by default — gated on the env var `KAKUIN_LIVE_SOCKET` pointing
//! at a local copy of the agent's workload-API socket.
//!
//! How to run against the pleme-dev cluster (Sprint M1.1 deliverable
//! is a live SPIRE on pleme-dev): exec into a pod that has the
//! `csi.spiffe.io` volume mounted, build kakuin-workload-api, run
//! `KAKUIN_LIVE_SOCKET=/run/spiffe.io/socket cargo test live_pleme_dev`.

use std::path::PathBuf;

use kakuin_workload_api::WorkloadApiClient;

fn live_socket() -> Option<PathBuf> {
    std::env::var_os("KAKUIN_LIVE_SOCKET").map(PathBuf::from)
}

#[tokio::test]
async fn fetches_svid_from_live_agent() {
    let Some(socket) = live_socket() else {
        eprintln!("skip — set KAKUIN_LIVE_SOCKET to enable");
        return;
    };

    let mut client = WorkloadApiClient::connect(&socket)
        .await
        .expect("connect to workload-API");
    let svid = client.fetch_x509_svid().await.expect("fetch_x509_svid");

    assert!(
        svid.spiffe_id.starts_with("spiffe://"),
        "got: {}",
        svid.spiffe_id
    );
    assert!(!svid.cert_chain_der.is_empty(), "no cert chain");
    assert!(!svid.private_key_der.is_empty(), "no private key");
    assert!(!svid.bundle_der.is_empty(), "no trust bundle");

    let td = svid.trust_domain().expect("trust_domain parses");
    assert!(!td.is_empty(), "trust domain non-empty");
    eprintln!("✓ kakuin-workload-api fetched SVID");
    eprintln!("  spiffe_id = {}", svid.spiffe_id);
    eprintln!("  trust_domain = {td}");
    eprintln!("  cert_chain_der = {} bytes", svid.cert_chain_der.len());
    eprintln!("  bundle_der = {} bytes", svid.bundle_der.len());
}
