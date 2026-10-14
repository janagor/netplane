# Faster local builds

Netplane currently has a small workspace. Start with Cargo's ordinary development profile, then measure before changing linker or release settings.

- Use `cargo check --workspace` for fast type checking. Run the build and test commands before submitting a change.
- Use `cargo build --timings` when investigating a slow build; Cargo writes a report under `target/cargo-timings/`.
- On a memory-constrained machine, try `CARGO_BUILD_JOBS=2 cargo build --workspace` and compare elapsed time and memory use with the default.

The workspace defines an optional `fast-dev` profile in `Cargo.toml`:

```bash
cargo build --workspace --profile fast-dev
```

That profile reduces debug information and uses aborting panics. It is a local iteration option, not the profile used by CI. Keep normal test and CI commands as the source of verification.

Linker changes should be measured on the developer's platform. No external linker is required to build Netplane.
