# Continuous integration

The current [CI workflow](https://github.com/janagor/netplane/blob/main/.github/workflows/ci.yml) runs on pushes and pull requests to `main`, and by manual dispatch. It uses read-only repository permissions and cancels an older run when a new run starts for the same ref.

| Job | Current checks |
| --- | --- |
| Format and Clippy | `cargo fmt --all -- --check` and `cargo clippy --workspace --all-targets --all-features -- -D warnings` |
| Build and test | Workspace build, tests, and doc tests on Linux, macOS, and Windows with Rust 1.88, plus stable Rust on Linux; package verification on Linux |
| Dependency policy | `cargo deny check` with all features, including advisories and licenses; also runs weekly |
| Build mdBook | Test LLM generation, verify committed output, and run `mdbook build docs` |

The quality job uses Rust 1.88; the test matrix distinguishes MSRV from stable Rust. Separate workflows check commit messages and secrets. Secretlint scans tracked files in the checked out tree, including hidden files, but does not scan Git history. Dependabot proposes dependency updates weekly. Runtime behavior is covered by the workspace tests.

## Run checks locally

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo build --workspace --all-targets
cargo test --workspace --all-features
cargo test --workspace --doc
cargo package -p netplane
cargo deny check
mdbook build docs
bash tests/generate_llms_txt_test.sh
bash scripts/generate-llms-txt.sh
git diff --exit-code -- llms.txt llms-full.txt
```

`./scripts/quality-gates.sh` delegates to `xtask quality run` and offers a broader local check set. Some checks require optional tools. Its repository scan may also include local generated directories, so review its report if it fails outside the tracked workspace. Use the focused commands above to reproduce the required CI jobs.

Cross-platform jobs must run on GitHub before their results can be confirmed; local Linux checks do not establish macOS or Windows status.
