# Getting started

Install Rust with [rustup](https://rustup.rs/). The repository's `rust-toolchain.toml` pins Rust 1.88 and includes rustfmt and Clippy.

```bash
git clone https://github.com/janagor/netplane.git
cd netplane
cargo fmt --all -- --check
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

The workspace has two members: `crates/netplane`, the future public library, and `crates/xtask`, development automation. There is no runnable Netplane application yet. See the repository [QUICKSTART.md](https://github.com/janagor/netplane/blob/main/QUICKSTART.md) for further setup details and the [architecture contract](https://github.com/janagor/netplane/blob/main/docs/architecture.md) before contributing runtime code.
