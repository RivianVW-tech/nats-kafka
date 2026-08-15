# HOWTO: Validate Multicell Support Locally

This guide validates the [multiple NATS clusters](docs/config.md#natsclusters) (multicell) feature end to end on a laptop.
It bridges JetStream events from two independent NATS servers (cells) into a single Kafka topic, using only docker compose and the bridge binary.

## What is being validated

The bridge supports one default NATS cluster plus additional named clusters in a single deployment.
The legacy `nats {}` config block defines the default cluster.
Additional named clusters come from the `natsclusters` list.
Each connector selects a cluster with `natsconnection`; an empty value means the default cluster.

The feature lives on branch `ak-RCL-multicell`.
The validation environment in this guide lives on branch `worktree-local-multicell-validation`.

## Branch setup

Combine the feature branch and the validation environment on a throwaway branch:

```bash
git checkout -b local-multicell-test ak-RCL-multicell
git merge worktree-local-multicell-validation
```

## Environment overview

The environment is defined in `resources/local_multicell.yml` and started with `make setup-local-multicell`.

| Service | Address | Notes |
| --- | --- | --- |
| Zookeeper | internal only | reachable by Kafka as zookeeper:2181, no host port |
| Kafka | localhost:9192 | plaintext listener, topic auto-create enabled |
| NATS cell-a | localhost:4222 | JetStream enabled, monitoring on localhost:8222 |
| NATS cell-b | localhost:4223 | JetStream enabled, monitoring on localhost:8223 |
| Bridge monitoring | localhost:9222 | served by the bridge process on the host, not by docker |

Kafka intentionally uses host port 9192 instead of 9092.
This keeps the environment clear of the integration-test servers from `resources/test_servers.yml` and of local port forwards.

The matching bridge config is `conf/nats-kafka-multicell-local.conf`.
It runs two `JetStreamToKafka` connectors, one against each cell, both writing to the `brand.telemetry` topic.

## Prerequisites

* Docker with the compose plugin.
* The [`nats` CLI](https://github.com/nats-io/natscli): `brew install nats-io/nats-tools/nats`.

## Steps

### 1. Start the environment and build the bridge

```bash
make setup-local-multicell
make build
```

Note that `make setup-local-multicell` only starts the docker infrastructure.
The bridge itself runs as a host process in step 3.

### 2. Create the `TELEMETRY` stream in each cell

This step is mandatory before starting the bridge.
A `JetStreamToKafka` connector fails at startup with `nats: stream not found` if its stream does not exist yet, and a connector failure at startup stops the whole bridge.

```bash
nats -s nats://localhost:4222 stream add TELEMETRY --subjects "telemetry.>" \
  --storage file --retention limits --discard old --defaults
nats -s nats://localhost:4223 stream add TELEMETRY --subjects "telemetry.>" \
  --storage file --retention limits --discard old --defaults
```

File storage matters for step 7: a memory-storage stream is lost when its container restarts.
If a stream disappears while the bridge is already running, the affected connector logs `stream not found` and retries every `reconnectinterval` until the stream is recreated, then restarts on its own.

If the `nats` CLI is not installed, the same commands can be run through docker:

```bash
docker run --rm --network nats_kafka_multicell_default natsio/nats-box:latest \
  nats -s nats://nats-cell-a:4222 stream add TELEMETRY --subjects "telemetry.>" \
  --storage file --retention limits --discard old --defaults
docker run --rm --network nats_kafka_multicell_default natsio/nats-box:latest \
  nats -s nats://nats-cell-b:4222 stream add TELEMETRY --subjects "telemetry.>" \
  --storage file --retention limits --discard old --defaults
```

Inside that network use `nats-cell-a:4222` and `nats-cell-b:4222` instead of the localhost ports.

### 3. Run the bridge

```bash
./nats-kafka -c conf/nats-kafka-multicell-local.conf
```

The startup log should show connections to clusters `default` and `cell-b`, and both connectors, the cell-b one with a `[cell-b]` prefix in its name.

### 4. Start a Kafka consumer

In a second terminal:

```bash
docker exec -it nats_kafka_multicell-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9192 --topic brand.telemetry --from-beginning
```

Alternatively use `kcat -b localhost:9192 -t brand.telemetry -C` from the host.

### 5. Publish one message into each cell

In a third terminal:

```bash
nats -s nats://localhost:4222 pub telemetry.test.carA "hello-from-cell-a"
nats -s nats://localhost:4223 pub telemetry.test.carB "hello-from-cell-b"
```

Both messages must appear on the Kafka consumer.

### 6. Check monitoring

```bash
curl -s localhost:9222/varz | jq '.nats'
```

The `nats` array must list two connections, `default` and `cell-b`, both `connected: true`.
The `connectors` array reports per-connector message counts and the `nats_connection` of each connector.

### 7. Validate failure isolation

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

### 8. Tear everything down

```bash
make teardown-local-multicell
```

## Troubleshooting

### `Bind for 0.0.0.0:<port> failed: port is already allocated`

Another process or container already publishes that host port.
Check with `lsof -nP -iTCP:<port> -sTCP:LISTEN` and `docker ps`.
Common culprits are ssh port forwards and the integration-test servers from `make setup-docker-test`, which publish 2181 and 9092-9094.
This environment avoids both by publishing no Zookeeper host port and by using 9192 for Kafka.

### `http://localhost:9222/varz` is unreachable

The monitoring endpoint is served by the bridge process, not by a container.
If nothing listens on 9222, the bridge is not running.
Start it as in step 3 and check its startup log for errors.

### `error starting bridge, nats: stream not found` at startup

The `TELEMETRY` stream is missing in one of the cells.
Run step 2, then start the bridge again.
A missing stream is fatal at startup but tolerated at runtime, as described in step 2.

### Platform warning for the Zookeeper image

`wurstmeister/zookeeper` is an amd64-only image.
On Apple Silicon it runs under emulation and docker prints a platform warning.
This is harmless; the same image is used by the repo's integration-test servers.

## Relationship to the automated tests

The multicell integration tests (`server/core/multicell_test.go`) cover the same flows using embedded in-process NATS servers, so they only need the docker Kafka from `make setup-docker-test`:

```bash
make setup-docker-test
go test -count=1 -race -run TestMultiCell ./server/core/...
make teardown-docker-test
```

This guide exists for manual validation of the real binary, config file format, and runtime behavior against out-of-process servers.
