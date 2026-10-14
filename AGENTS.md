# Netplane agent coding contract

Netplane is a reusable Rust library for dynamically reconfigurable Tower service graphs. The current crate is a scaffold: do not describe graph execution, hot replacement, or lifecycle guarantees as implemented. Read [docs/architecture.md](docs/architecture.md) and the decisions in `docs/adr/` before changing the public API.

## Workspace

| Path | Responsibility |
| --- | --- |
| `crates/netplane` | Public library and future transport-independent graph runtime. |
| `crates/xtask` | Development automation; never a runtime dependency of the library. |
| `docs/architecture.md`, `docs/adr/` | Design requirements and accepted decisions. |

Add crates only when a concrete dependency boundary emerges. Keep transport and deployment adapters outside the core. Add dependencies when they have consumers, use workspace versions, and minimize dependencies exposed by the public API.

## Design rules

- Interoperate with `tower::Service` and preserve readiness semantics. Avoid a competing service abstraction without a demonstrated need.
- Keep readiness and dispatch for each request on the same service instance and graph version, including across publication.
- Validate a replacement graph before publishing it. Treat published graph versions as immutable.
- Make ownership and request lifetime explicit so in-flight requests keep required resources through completion or cancellation.
- Bound concurrency, queues, and task creation. Never hide overload with unbounded spawning.
- Keep locks out of `.await` spans. Use `tokio` only when runtime work needs it; offload blocking or long CPU work appropriately.
- Make cancellation, timeout, and shutdown cleanup observable and testable.
- Test core behavior with mock services, without sockets. Cover graph lifetime, readiness, backpressure, and concurrency as those features are implemented.
- Do not implement HTTP listeners, Kubernetes integration, or cloud deployment as part of the initial graph runtime.

## Rust and API conventions

- Rust 2024, MSRV 1.88. Do not raise the MSRV without a dependency or feature requirement.
- Forbid unsafe code in the library. Use typed errors when needed; avoid `unwrap` and `expect` in production library code.
- Document every public item, including error and cancellation behavior. Prefer a small stable surface; keep implementation details private.
- Add or update meaningful tests before changing behavior. Inspect snapshot diffs before accepting them.
- Follow workspace lint settings. A justified lint exception belongs in the crate manifest with its reason.
- Keep source files under 500 lines. Do not put secrets or personal contact data in source or docs.
- `VERSION` and workspace version remain `0.0.0` until a deliberate release decision. The library is not currently published.

## Workflow

1. Check `git status`, nearby code, the architecture documents, and the current `.github/workflows/ci.yml` before editing.
2. Identify affected behavior and tests. Add tests first when changing logic.
3. Run focused checks. Use `./scripts/quality-gates.sh` when its optional tools are installed and its repository scan can run in the local environment. At minimum, run format, Clippy, build, and tests for code changes.
4. Update documentation when architecture or public behavior changes. Run `bash scripts/generate-llms-txt.sh` after significant architecture changes.
5. Use lowercase Conventional Commit subjects if asked to commit.

Use `rg` for repository searches and exclude `.git` and `target`. Repository skills in `.agents/skills/` contain detailed procedures, but this file is the project contract.
