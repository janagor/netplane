# Migration guide

Netplane is still at version `0.0.0` and has no published crate or stable upgrade path. The proposed pre-1.0 policy is to document source-breaking changes and migration steps for each published minor version, while allowing incompatible changes only with a minor version increment. Patch releases would contain compatible fixes. This proposal requires an explicit release decision before it becomes a commitment; the first release will add version-specific migration notes here.

For current source users, the [execution contract](docs/architecture/execution-semantics.md) is authoritative. `Handle::execute` selects a graph version and consumes admission capacity when called, before its future is polled. Use `try_execute` when overload must be handled synchronously. `GraphDraft` wraps one Tower root; `GraphBuilder` supports one router with named backends. Controller rollback republishes retained executable service state.
