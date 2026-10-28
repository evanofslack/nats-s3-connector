FROM lukemathwalker/cargo-chef:latest-rust-1.99.1-trixie AS chef
WORKDIR /app

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY types ./types
COPY server ./server
COPY client ./client
COPY cli ./cli
COPY test ./test
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY Cargo.toml Cargo.lock ./
COPY types ./types
COPY server ./server
COPY client ./client
COPY cli ./cli
COPY test ./test
RUN cargo build --release --locked -p nats3-server

FROM node:24-slim AS web-builder
WORKDIR /app/web
COPY server/web/package*.json ./
RUN npm ci
COPY server/web ./
RUN npm run build

FROM debian:trixie-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    openssl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --no-create-home --shell /usr/sbin/nologin nats3
WORKDIR /app
COPY --from=builder /app/target/release/nats3-server /usr/local/bin/
COPY --from=web-builder /app/web/dist ./server/web/dist
USER nats3
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -fsS http://localhost:8080/ping || exit 1
ENTRYPOINT ["/usr/local/bin/nats3-server"]
