# nats-s3-connector

Connect [NATS Jetstream](https://docs.nats.io/nats-concepts/jetstream) to S3 for long term storage and replay.

## Description

This application facilitates storing and loading NATS messages
to and from S3 object storage.

Define `store` jobs to handle serializing messages, compressing into blocks
and writing to S3. Send HTTP requests to start `load` jobs to download messages
from S3 and submit back into NATS.

## Running

See the [examples](https://github.com/evanofslack/nats-s3-connector/tree/main/examples) directory to get started.

The app can be run from a [pre-built docker container](https://hub.docker.com/r/evanofslack/nats-s3-connector/tags)

```yaml
version: "3.7"
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

Jobs that store NATS messages in S3 are defined through the config file.
Config values can be defined through toml or yaml formats.

```toml
[[store]]
name ="job-1"
stream = "test"
subject = "subjects-1"
bucket = "bucket-1"

[[store]]
name ="job-2"
stream = "test"
subject = "subjects-2"
bucket = "bucket-2"
```

The config can take any number of `store` definitions. It will start
threads to monitor each job.

### Load

Messages stored in S3 can be loaded and submitted back into NATS.
These load jobs are started by sending a PUT request to the HTTP server
on the endpoint `/load`:

```bash
curl --header "Content-Type: application/json" \
  --request POST \
  --data '{
            "bucket":"bucket-1",
            "read_stream":"test",
            "read_subject":"subjects-1",
            "write_stream":"test",
            "write_subject":"dest-1"
        }' \
  http://localhost:8080/load
```

This will start loading messages from S3 and publishing them to specified stream.
