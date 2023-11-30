# nats-s3-connect

Connect NATS Jetstream to S3 for long term storage and replay.

## Description

This application facilitates writing and reading NATS messages
to and from S3 object storage.

It handles serializing messages, compressing into blocks and writing to S3.
It enables messages stored in S3 to be deserialized and submitted
back to NATS.
