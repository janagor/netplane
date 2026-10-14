# ADR 0001: Tower service model

**Status:** Accepted

**Date:** 2026-10-14

## Context

Netplane needs a service composition model with readiness and request execution semantics. Tower already provides `Service` and `Layer`, and users should be able to integrate existing Tower components.

## Decision

Use `tower::Service` as the fundamental request-processing abstraction. Future graph composition must respect readiness before dispatching requests. Prototype any additional graph-specific API against ordinary Tower services before making it public.

## Consequences

Core tests can use in-memory mock services. An adapter may be needed for graph publication and version ownership, but it must preserve Tower interoperability. This decision does not choose a graph representation, public trait set, or runtime implementation.
