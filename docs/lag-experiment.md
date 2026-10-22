# Consumer-lag experiment (Kafka + DynamoDB Local)

> **Status:** the Kafka/DynamoDB code behind `-tags infra` was written but could not be compiled or run where
> it was authored (no network for Go modules or Docker images). Run these steps yourself, and record **measured**
> numbers in the table at the bottom. Everything labelled "expected" is a prediction, not a result.

## 0. One-time setup (PowerShell)

```powershell
docker compose up -d                       # Kafka (KRaft) + DynamoDB Local, both free
go get github.com/segmentio/kafka-go github.com/aws/aws-sdk-go-v2/aws github.com/aws/aws-sdk-go-v2/config github.com/aws/aws-sdk-go-v2/credentials github.com/aws/aws-sdk-go-v2/service/dynamodb
go mod tidy
go vet -tags infra ./...                   # fix any compile error before continuing
```

Start the service against real infra (leave this terminal running):

```powershell
$env:KAFKA_BROKERS="localhost:9092"; $env:DYNAMODB_ENDPOINT="http://localhost:8000"
$env:KAFKA_CONSUMERS="1"; $env:CONSUMER_DELAY_MS="10"     # 1 consumer, 10 ms/event => ~100 events/s capacity
go run -tags infra ./cmd/server
```

## 1. Experiment A: producer faster than the consumer

```powershell
go run -tags infra ./cmd/produce -rate 300 -seconds 20     # second terminal
```

Watch lag two ways (the second is authoritative):

```powershell
curl.exe -s localhost:8080/metrics | Select-String "consumer_lag|outcomes"
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group adshield-detector
```

Expected: lag grows by roughly (300 − 100) = 200 events/s while producing, so about 4,000 at the end, then
drains slowly. The `LAG` column in `kafka-consumer-groups.sh` should agree with `adshield_consumer_lag`.

## 2. Experiment B: scale the consumer group

Stop the server (Ctrl+C), then restart with `KAFKA_CONSUMERS=8` and the same delay. Re-run the producer.
Expected: capacity rises towards 8 × 100 = 800 events/s, lag stays near zero. A group cannot use more
consumers than partitions (the topic has 8), so 12 consumers would not be faster than 8.

## 3. Experiment C: crash mid-stream (the idempotency proof)

Run the producer, then Ctrl+C the server after ~10 s and start it again.
Expected: the consumer resumes from the last committed offset. Events processed but not yet committed are
redelivered, so `adshield_processing_outcomes_total{outcome="duplicate"}` rises while the number of
`processed` events and the payout stay correct, because the DynamoDB conditional write rejects the replay.
Inspect the claims: `aws dynamodb scan --table-name adshield_idempotency --endpoint-url http://localhost:8000 --region eu-central-1`
(any dummy credentials work).

## 4. Questions to be able to answer in an interview

- Why key by `device_id`? (Per-device ordering; detection rules read a device's history in order.)
- Why commit offsets after processing? (At-least-once; idempotency makes duplicates harmless.)
- What does a hot partition look like here, and how would you see it? (One partition's lag grows alone.)
- What happens to a poison message? (Retries in place with backoff, then `adshield.events.dlq`.)
- Why release the DynamoDB claim when processing fails? (Otherwise the retry looks like a duplicate and the event is lost.)

## 5. Record your results

| Run | Consumers | Delay/event | Produce rate | Peak lag (measured) | Time to drain (measured) |
|---|---|---|---|---|---|
| A | 1 | 10 ms | 300/s | | |
| B | 8 | 10 ms | 300/s | | |
| C (crash) | 4 | 0 ms | 300/s | duplicates seen: | payout correct? |
