default:
  @just --list

test:
    cargo test

test-integration:
    cargo test --features integration

build:
    cargo build

build-server:
    cargo build -p nats3-server

run: build-server
    ./target/debug/nats3-server --config examples/config.toml

dev: build-server
    RUST_LOG=none,nats3_server=TRACE RUST_BACKTRACE=1 ./target/debug/nats3-server --config examples/config.toml

up:
    cd examples && docker compose -f docker-compose-dev.yaml up -d && docker compose logs --follow

upb:
    cd examples && docker compose -f docker-compose-dev.yaml --profile infra --profile main up -d --build --force-recreate && docker compose logs --follow

down:
    cd examples && docker compose -f docker-compose-dev.yaml --profile infra --profile main down

infra:
    cd examples && docker compose -f docker-compose-dev.yaml --profile infra up -d && docker compose logs --follow
