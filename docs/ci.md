# Continuous integration

Pull requests and pushes to `main` run the checks in [CI](../.github/workflows/ci.yml):

| Job | Check |
| --- | --- |
| Format and Clippy | Rust 1.88 formatting and workspace linting |
| Build and test | Workspace build, tests, and documentation tests on Linux, macOS, and Windows with Rust 1.88, plus a stable Rust lane on Linux; library packaging on Linux |
| Dependency policy | `cargo deny check` with all features, including advisories, licenses, bans, and sources; runs weekly as well as on code changes |
| Build mdBook | Verify generated LLM documentation and build the site with `mdbook build docs` |

The [secret lint workflow](../.github/workflows/secretlint.yml) also runs on pull requests and `main` pushes using Node 22. It scans tracked files in the checked out tree, including hidden files, but does not scan Git history. Dependabot checks Cargo and GitHub Actions dependencies weekly.

Run the core checks locally with:

```sh
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

`cargo-deny` and `mdbook` are optional local installations. CI installs them. Runtime behavior is also covered by the workspace tests.
