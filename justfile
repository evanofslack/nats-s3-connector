default:
  @just --list

test:
    cargo test --features integration

test1 TEST:
    cargo test --features integration {{TEST}}

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
    RUST_LOG=none,nats3_server=debug RUST_BACKTRACE=1 ./target/debug/nats3-server --config examples/config.local.toml

dev-web:
    cd server/web && npm run dev

build-web:
    cd server/web && npm run build

build-cli:
    cargo build -p nats3-cli

build-docker:
    docker build -f server/Dockerfile -t nats3 .


install-cli:
    cargo install --path cli --force

install-test-cli:
    cargo install --path test --force

install-web:
    cd server/web && npm install

install: install-cli install-test-cli install-web

up:
    cd examples && docker compose -f docker-compose-dev.yaml --profile infra --profile main up -d && docker compose logs --follow

upb:
    cd examples && docker compose -f docker-compose-dev.yaml --profile infra --profile main up -d --build --force-recreate && docker compose logs --follow

down:
    cd examples && docker compose -f docker-compose-dev.yaml --profile infra --profile main down

infra:
    cd examples && docker compose -f docker-compose-dev.yaml --profile infra up -d && docker compose logs --follow
