# kakuin-workload-api

Rust SPIFFE Workload API client. Connects to a [SPIFFE](https://spiffe.io)
agent's gRPC endpoint over a unix domain socket, fetches the workload's
X.509 SVID + trust bundle, and exposes a subscription stream that emits
a fresh `X509Svid` every time the agent rotates the cert.

Sprint **M1.2** of [`pleme-io/theory/MESH-EXECUTION-PLAN.md`](https://github.com/pleme-io/theory/blob/main/MESH-EXECUTION-PLAN.md)
— the second concrete deliverable on the path to a typed
`(defmesh …)` primitive (per
[`MESH.md`](https://github.com/pleme-io/theory/blob/main/MESH.md)).

## Status

- ✅ X.509 SVID fetch (one-shot)
- ✅ X.509 SVID subscribe (rotation-aware stream)
- ✅ Trust-bundles fetch (independent of identity)
- ⏳ JWT-SVID fetch / validate (M5+)

## Quickstart

```rust
use kakuin_workload_api::WorkloadApiClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Reads $SPIFFE_ENDPOINT_SOCKET; default /run/spiffe.io/socket.
    let mut client = WorkloadApiClient::default().await?;

    let svid = client.fetch_x509_svid().await?;
    println!("got SVID for {}", svid.spiffe_id);

    // Or subscribe for hot-reload on rotation:
    let mut updates = client.subscribe_x509_svid().await?;
    use futures::StreamExt;
    while let Some(update) = updates.next().await {
        let update = update?;
        for s in update.svids {
            println!("rotated → {}  ({} bytes chain, valid bundle: {})",
                     s.spiffe_id, s.cert_chain_der.len(), !s.bundle_der.is_empty());
        }
    }
    Ok(())
}
```

## Verify against a live spire-agent

```bash
# Mount the workload API socket via the spiffe-csi driver in your pod:
volumes:
  - name: spiffe-csi
    csi: { driver: csi.spiffe.io, readOnly: true }
volumeMounts:
  - name: spiffe-csi
    mountPath: /run/spiffe.io
    readOnly: true

# Then in your binary:
let svid = WorkloadApiClient::default().await?.fetch_x509_svid().await?;
```

## Why "kakuin"

確認 (kakuin) — verification, confirmation. The crate is the path
through which a Rust workload *confirms* its identity to the SPIFFE
trust anchor.

## Companion to

- [`pleme-io/kakuin-rustls`](https://github.com/pleme-io/kakuin-rustls)
  — adapt SVIDs to `rustls::{Server,Client}Config` (Sprint M1.3).
- [`pleme-io/proxy`](https://github.com/pleme-io/proxy) — the mesh
  data-plane proxy that uses kakuin-workload-api to fetch its identity
  (Sprint M2.1).

## License

Dual-licensed MIT OR Apache-2.0.
