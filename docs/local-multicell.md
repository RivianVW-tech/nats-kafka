# Local Multicell Validation

This runbook validates the [multiple NATS clusters](config.md#natsclusters) feature end to end on a laptop.
It bridges JetStream events from two independent NATS servers (cells) into a single Kafka topic.

The environment is defined in `resources/local_multicell.yml` and consists of:

| Service | Address | Notes |
| --- | --- | --- |
| Zookeeper | localhost:2181 | |
| Kafka | localhost:9092 | plaintext listener, topic auto-create enabled |
| NATS cell-a | localhost:4222 | JetStream enabled, monitoring on localhost:8222 |
| NATS cell-b | localhost:4223 | JetStream enabled, monitoring on localhost:8223 |

The matching bridge config is `conf/nats-kafka-multicell-local.conf`.
It runs two `JetStreamToKafka` connectors, one against each cell, both writing to the `brand.telemetry` topic.

## Prerequisites

* Docker with the compose plugin.
* The [`nats` CLI](https://github.com/nats-io/natscli): `brew install nats-io/nats-tools/nats`.

## Steps

1. Start the environment and build the bridge:

   ```bash
   make setup-local-multicell
   make build
   ```

2. Create the `TELEMETRY` stream in each cell:

   ```bash
   nats -s nats://localhost:4222 stream add TELEMETRY --subjects "telemetry.>" \
     --storage file --retention limits --discard old --defaults
   nats -s nats://localhost:4223 stream add TELEMETRY --subjects "telemetry.>" \
     --storage file --retention limits --discard old --defaults
   ```

   File storage matters for step 7: a memory-storage stream is lost when its container restarts.
   If the stream disappears, the affected connector logs `stream not found` and retries every `reconnectinterval` until the stream is recreated, then restarts on its own.

   If the `nats` CLI is not installed, the same commands can be run through docker:

   ```bash
   docker run --rm --network nats_kafka_multicell_default natsio/nats-box:latest \
     nats -s nats://nats-cell-a:4222 stream add TELEMETRY --subjects "telemetry.>" \
     --storage file --retention limits --discard old --defaults
   ```

   Inside that network use `nats-cell-a:4222` and `nats-cell-b:4222` instead of the localhost ports.

3. Run the bridge:

   ```bash
   ./nats-kafka -c conf/nats-kafka-multicell-local.conf
   ```

   The startup log should show both connectors, the cell-b one with a `[cell-b]` prefix in its name.

4. In a second terminal, start a Kafka consumer:

   ```bash
   docker exec -it nats_kafka_multicell-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
     --bootstrap-server localhost:9092 --topic brand.telemetry --from-beginning
   ```

   Alternatively use `kcat -b localhost:9092 -t brand.telemetry -C` from the host.

5. In a third terminal, publish one message into each cell:

   ```bash
   nats -s nats://localhost:4222 pub telemetry.test.carA "hello-from-cell-a"
   nats -s nats://localhost:4223 pub telemetry.test.carB "hello-from-cell-b"
   ```

   Both messages must appear on the Kafka consumer.

6. Check monitoring:

   ```bash
   curl -s localhost:9222/varz | jq '.nats'
   ```

   The `nats` array must list two connections, `default` and `cell-b`, both `connected: true`.
   The `connectors` array reports per-connector message counts and the `nats_connection` of each connector.

7. Validate failure isolation:

   ```bash
   docker stop nats_kafka_multicell-nats-cell-b-1
   nats -s nats://localhost:4222 pub telemetry.test.carA "cell-a-still-works"
   ```

   The bridge must stay up and the cell-a message must still reach Kafka.
   `/varz` shows `cell-b` as `connected: false`.
   Then restart the cell and confirm it recovers:

   ```bash
   docker start nats_kafka_multicell-nats-cell-b-1
   # wait a few seconds for the reconnect timer, then:
   nats -s nats://localhost:4223 pub telemetry.test.carB "cell-b-recovered"
   ```

8. Tear everything down:

   ```bash
   make teardown-local-multicell
   ```

## Relationship to the automated tests

The multicell integration tests (`server/core/multicell_test.go`) cover the same flows using embedded in-process NATS servers, so they only need the docker Kafka from `make setup-docker-test`:

```bash
make setup-docker-test
go test -count=1 -race -run TestMultiCell ./server/core/...
make teardown-docker-test
```

This runbook exists for manual validation of the real binary, config file format, and runtime behavior against out-of-process servers.
