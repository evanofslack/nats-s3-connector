default:
  @just --list

test:
    cargo test

test-integration:
    cargo test --features integration

build:
    cargo build

run: build
    ./target/debug/nats3 --config examples/config.toml

dev: build
    RUST_LOG=none,nats3=TRACE RUST_BACKTRACE=1 ./target/debug/nats3 --config examples/config.toml

up:
    cd examples && docker compose -f docker-compose-dev.yaml up -d && docker compose logs --follow

upb:
    cd examples && docker compose -f docker-compose-dev.yaml up -d --build --force-recreate && docker compose logs --follow

down:
    cd examples && docker compose -f docker-compose-dev.yaml down

infra:
    cd examples && docker compose -f docker-compose-dev.yaml --profile infra up -d && docker compose logs --follow
