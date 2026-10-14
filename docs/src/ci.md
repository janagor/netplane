# Continuous integration

The current [CI workflow](https://github.com/janagor/netplane/blob/main/.github/workflows/ci.yml) runs on pushes and pull requests to `main`, and by manual dispatch. It uses read-only repository permissions and cancels an older run when a new run starts for the same ref.

| Job | Current checks |
| --- | --- |
| Format | `cargo fmt --all -- --check` |
| Clippy | `cargo clippy --workspace --all-targets --all-features -- -D warnings` |
| Build and test | Workspace build, unit and integration tests, doc tests, package listing, and package dry run |

These jobs use Rust 1.88 on Ubuntu. Other workflow files cover commit messages, security scanning, secrets, dependency automation, and hotfix handling. The current CI workflow does not claim cross-platform coverage or run a scheduled DORA report.

## Run checks locally

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo build --workspace --all-targets
cargo test --workspace --all-features
cargo test --workspace --doc
cargo package -p netplane --list
```

`./scripts/quality-gates.sh` delegates to `xtask quality run` and offers a broader local check set. Some checks require optional tools. Its repository scan may also include local generated directories, so review its report if it fails outside the tracked workspace. Use the focused commands above to reproduce the required CI jobs.

The CI workflow will be expanded and validated in the separate infrastructure work before any cross-platform or supply-chain check is called required.
