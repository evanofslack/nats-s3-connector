FROM lukemathwalker/cargo-chef:latest-rust-1.89.0 AS chef
WORKDIR /app

FROM chef AS planner
COPY Cargo.toml ./
COPY types ./types
COPY server ./server
COPY client ./client
COPY cli ./cli
COPY test ./test
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY Cargo.toml ./
COPY types ./types
COPY server ./server
COPY client ./client
COPY cli ./cli
COPY test ./test
RUN cargo build --release -p nats3-server

FROM node:20-slim AS web-builder
WORKDIR /app/web
COPY server/web/package*.json ./
RUN npm ci
COPY server/web ./
RUN npm run build

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y \
    ca-certificates \
    openssl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/nats3-server /usr/local/bin
COPY --from=web-builder /app/web/dist /app/web/dist
ENTRYPOINT ["/usr/local/bin/nats3-server"]
