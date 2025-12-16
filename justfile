default:
  @just --list

test:
    cargo test --features integration

check:
    cargo test --workspace

build:
    cargo build

fmt:
    cargo +nightly fmt

precommit: fmt check test

build-server:
    cargo build -p nats3-server

run-server: build-server
    ./target/debug/nats3-server --config examples/config.toml

dev-server: build-server
    RUST_LOG=none,nats3_server=DEBUG RUST_BACKTRACE=1 ./target/debug/nats3-server --config examples/config.local.toml

build-cli:
    cargo build -p nats3-cli

install-cli:
    cargo install --path cli --force

install-test-cli:
    cargo install --path test --force

up:
    cd examples && docker compose -f docker-compose-dev.yaml up -d && docker compose logs --follow

upb:
    cd examples && docker compose -f docker-compose-dev.yaml --profile infra --profile main up -d --build --force-recreate && docker compose logs --follow

down:
    cd examples && docker compose -f docker-compose-dev.yaml --profile infra --profile main down

infra:
    cd examples && docker compose -f docker-compose-dev.yaml --profile infra up -d && docker compose logs --follow
