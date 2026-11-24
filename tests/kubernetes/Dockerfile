FROM rust:1.88.0-bookworm@sha256:af306cfa71d987911a781c37b59d7d67d934f49684058f96cf72079c3626bfe0 AS builder
WORKDIR /src
COPY . .
COPY tests/kubernetes/Cargo.lock ./Cargo.lock
ENV RUSTUP_TOOLCHAIN=1.88.0
RUN cargo build --locked --profile ci -p netplane-kubernetes-e2e --bins
FROM debian:bookworm-slim@sha256:e67eeead736ff7bbe258ef088bff683635c561b1fcfefad0806c1df4ab49ce0e
COPY --from=builder /src/target/ci/backend /backend
COPY --from=builder /src/target/ci/gateway /gateway
USER 10001:10001
