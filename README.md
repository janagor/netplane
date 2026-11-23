# Netplane

The optional `netplane-http` workspace crate serves buffered HTTP requests through a Netplane handle. It bounds body size, body read concurrency, and read time, then rejects oversized encoded responses. See [HTTP resource boundaries](docs/http-boundaries.md) and `crates/netplane-http/examples/reconfigure.rs` for a persistent listener with process-local graph publication.

The [policy routing gateway](crates/netplane-http/examples/policy_gateway/README.md) is a persistent HTTP example with deterministic stable/canary routing, bounded execution, and graceful shutdown.

Netplane is an early stage Rust library for constructing and managing dynamically reconfigurable Tower service graphs. It aims to make graph publication, request lifetime, readiness, backpressure, and cancellation behavior explicit.

The repository provides bounded Tower service endpoints, a validated single-router `GraphBuilder`, conditional publication and rollback through `ReconfigurationController`, atomic publication of validated execution versions, graceful shutdown, and development tooling. Accepted requests retain the version selected at invocation. Successful shutdown releases the active graph, even when handles remain. Version `0.0.0` marks this pre-release stage; neither library crate is published.

## Scope

Netplane builds on `tower::Service`. The core is transport independent and testable with in-memory services. The optional HTTP adapter has its own crate; deployment integrations can be added behind real dependency boundaries. Netplane is not a standalone HTTP proxy or a replacement async runtime.

The design contract is in [docs/architecture.md](docs/architecture.md), with decisions recorded in [docs/adr](docs/adr). They distinguish current behavior from future requirements.

The [performance guide](docs/performance.md) describes the core benchmark suite, its Tower comparisons, and how to reproduce the [measured baseline](docs/performance-baseline.md). The [API review](docs/api-readiness.md) and [release checklist](docs/release-readiness.md) track work remaining before any publication decision.

## Get started

The repository pins Rust 1.88 in `rust-toolchain.toml`. See [QUICKSTART.md](QUICKSTART.md) for setup and verification commands.

```bash
cargo build --workspace
cargo test --workspace
cargo run -p netplane --example live_reconfiguration
cargo run -p netplane --example controlled_reconfiguration
```

The workspace contains `crates/netplane` (the public library), `crates/netplane-http` (the optional HTTP adapter), and `crates/xtask` (development tooling). Dependencies are added only when used. `Cargo.lock` is ignored for this library workspace.

## Contribute

Read [CONTRIBUTING.md](CONTRIBUTING.md) and [AGENTS.md](AGENTS.md) before changing the architecture or public API. Report vulnerabilities through [the security policy](SECURITY.md). Netplane is licensed under [MIT](LICENSE).

## Weighted sticky routing

`WeightedRouting` is a transport-independent, immutable selector. Validate it
before building a graph and register its labels as routes:

```rust
use netplane::{GraphBuilder, WeightedRouting};
use std::convert::Infallible;
use tower::service_fn;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let policy = WeightedRouting::try_new([("canary", 25), ("stable", 75)])?;
    let selector = policy.clone();
    let mut builder = GraphBuilder::new()
        .service("canary", service_fn(|_: String| async { Ok::<_, Infallible>("canary") }))
        .service("stable", service_fn(|_: String| async { Ok::<_, Infallible>("stable") }))
        .router("policy", move |key: &String| selector.select(key.as_bytes()).to_owned());
    for target in policy.targets() {
        builder = builder.route("policy", target, target);
    }
    let graph = builder.fallback("policy", "stable").entry("policy").validate()?;
    drop(graph);
    Ok(())
}
```

Names must be nonempty and unique; integer weights must sum to 100. Zero-weight
labels remain available for route registration. Selection accepts arbitrary key
bytes; HTTP key validation stays in the HTTP adapter or application.

The policy hashes keys using 64-bit FNV-1a modulo 100 and assigns cumulative
bucket ranges in configured order. Identical keys and policies repeat their
assignment. Increasing the first target's weight expands its range; other weight
or order changes may reassign keys. Percentages allocate sticky-key buckets,
not exact per-request traffic. FNV-1a is non-cryptographic and must not define an
authorization or security boundary.

Publish a replacement graph to change weights. Accepted requests retain the
policy from the version selected at `execute()` invocation, even when selection
runs later. Controller rollback restores the retained policy. The policy owns no
workers and performs no I/O. Selection itself allocates nothing; the current
router API requires the owned label shown above. Compare both costs with
`cargo bench -p netplane --bench routing -- routing/weighted`.
