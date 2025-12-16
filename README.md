# nats-s3-connector

Connect [NATS Jetstream](https://docs.nats.io/nats-concepts/jetstream) to S3 for
long term storage and replay.

## Description

This application facilitates storing and loading NATS messages
to and from S3 object storage.

Create `store` jobs to handle serializing messages, compressing into blocks
and writing to S3. Create `load` jobs to download messages
from S3 and submit back into NATS.

## Running

See the [examples](https://github.com/evanofslack/nats-s3-connector/tree/main/examples) directory to get started.

The app can be run from a [pre-built docker container](https://hub.docker.com/r/evanofslack/nats-s3-connector/tags)

```yaml
services:
  nats3:
    image: evanofslack/nats-s3-connector:latest
    ports:
      - 8080:8080
    volumes:
      - ./config.toml:/etc/nats3/config.toml
```

Alternatively, build the executable from source

```bash
git clone https://github.com/evanofslack/nats-s3-connector
cd nats-s3-connector
cargo build
```

### Store

Store jobs consume messages from NATS and upload them to S3 as chunks.
Create store jobs by sending HTTP requests to server endpoint `/store/job`:

```bash
curl --header "Content-Type: application/json" \
  --request POST \
  --data '{
            "name": "job-1",
            "stream": "jobs",
            "subject": "subject-1",
            "bucket": "bucket-1",
          }' \
  http://localhost:8080/store/job
```

Or create store job with the `nats3` cli:

```bash
nats3 store create \
  --name job-1 \
  --stream jobs \
  --subject subjects-1 \
  --bucket bucket-1 \
```

### Load

Messages stored in S3 can be loaded and submitted back into NATS.
Create load jobs by sending HTTP requests to server endpoint `/load/job`:

```bash
curl --header "Content-Type: application/json" \
  --request POST \
  --data '{
            "bucket": "bucket-1",
            "read_stream": "jobs",
            "read_subject": "subject-1",
            "write_subject": "destination",
          }' \
  http://localhost:8080/load/job
```

Or create load jobs with the `nats3` cli:

```bash
nats3 load create \
  --bucket bucket-1 \
  --read-stream jobs \
  --read-subject subject-1 \
  --write-subject destination \
```

This will start loading messages from S3 and publishing them to specified stream.

### Metrics

There is an prometheus compatible metrics endpoint at `/metrics`. It provides
counters for messages stored and loaded, as well as gauges tracking in-progress
jobs. All metrics are prefixed with `nats3`.
