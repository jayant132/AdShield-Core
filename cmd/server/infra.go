//go:build infra

package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jayant132/adshield-core/internal/dynamoidem"
	"github.com/jayant132/adshield-core/internal/kafkaq"
	"github.com/jayant132/adshield-core/internal/pipeline"
	"github.com/jayant132/adshield-core/internal/telemetry"
)

// initInfra switches on real infrastructure from environment variables:
//
//	DYNAMODB_ENDPOINT=http://localhost:8000   idempotency in DynamoDB Local
//	KAFKA_BROKERS=localhost:9092              events through Kafka
//	KAFKA_CONSUMERS=4  CONSUMER_DELAY_MS=0    consumer-group size / artificial slowness
func initInfra(ctx context.Context, proc *pipeline.Processor, m *telemetry.Metrics) (publisher, bool) {
	if ep := os.Getenv("DYNAMODB_ENDPOINT"); ep != "" {
		ds, err := dynamoidem.New(ctx, ep, env("DYNAMODB_TABLE", "adshield_idempotency"))
		if err != nil {
			log.Fatalf("dynamodb: %v", err)
		}
		proc.Idem = ds
		log.Printf("idempotency: DynamoDB at %s", ep)
	}
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		return nil, false
	}
	bs := strings.Split(brokers, ",")
	n, _ := strconv.Atoi(env("KAFKA_CONSUMERS", "4"))
	delay, _ := strconv.Atoi(env("CONSUMER_DELAY_MS", "0"))
	go kafkaq.RunConsumers(ctx, kafkaq.Config{
		Brokers: bs, GroupID: env("KAFKA_GROUP", "adshield-detector"), Consumers: n,
		MaxAttempts: 5, Backoff: 100 * time.Millisecond, Delay: time.Duration(delay) * time.Millisecond,
	}, proc.Handle, m)
	log.Printf("queue: Kafka %v, %d consumers, %dms artificial delay/event", bs, n, delay)
	return kafkaq.NewProducer(bs, false), true
}
