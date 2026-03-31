# ── Stage 1: build ──────────────────────────────────────────────────────────
FROM rust:1.93-slim AS builder

WORKDIR /build

# Cache dependencies separately from source by building a stub binary first
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main(){}' > src/main.rs \
    && cargo build --release \
    && rm -rf src

# Build the real binary (touch forces Cargo to re-link)
COPY src ./src
RUN touch src/main.rs \
    && cargo build --release

# ── Stage 2: runtime ────────────────────────────────────────────────────────
FROM debian:bookworm-slim

# ca-certificates so rustls can verify TLS certificates
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/ch-ch-replicator /usr/local/bin/ch-ch-replicator

ENTRYPOINT ["/usr/local/bin/ch-ch-replicator"]
