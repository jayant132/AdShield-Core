//go:build infra

// Package kafkaq is the real-Kafka counterpart of internal/queue.
//
//   - Producer keys every message by device_id, so one device's events always
//     land on one partition and are consumed in order.
//   - Consumers are a consumer group; offsets are committed only AFTER the
//     event is processed (at-least-once). The idempotent processor makes
//     redelivery after a crash harmless.
//   - A failing event is retried in place with backoff (which preserves
//     per-partition ordering); after MaxAttempts it goes to a DLQ topic.
package kafkaq

import (
	"context"
	"encoding/json"
	"log"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/jayant132/adshield-core/internal/events"
	"github.com/jayant132/adshield-core/internal/telemetry"
)

const (
	Topic    = "adshield.events"
	DLQTopic = "adshield.events.dlq"
)

type Producer struct{ w *kafka.Writer }

func NewProducer(brokers []string, async bool) *Producer {
	return &Producer{w: &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  Topic,
		Balancer:               &kafka.Hash{}, // hash(key) -> partition
		RequiredAcks:           kafka.RequireAll,
		BatchTimeout:           5 * time.Millisecond, // default is 1s, far too slow for an API
		AllowAutoTopicCreation: true,
		Async:                  async,
	}}
}

// Publish implements the server's publisher interface.
func (p *Producer) Publish(ctx context.Context, e events.Event) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return p.w.WriteMessages(ctx, kafka.Message{Key: []byte(e.DeviceID), Value: b})
}

func (p *Producer) Close() error { return p.w.Close() }

type Handler func(ctx context.Context, e events.Event) error

type Config struct {
	Brokers     []string
	GroupID     string
	Consumers   int           // readers in this process; partitions are split among them
	MaxAttempts int           // per event before it is dead-lettered
	Backoff     time.Duration // doubled on every attempt
	Delay       time.Duration // artificial per-event delay to simulate a slow downstream (lag experiments)
}

// RunConsumers blocks until ctx is cancelled.
func RunConsumers(ctx context.Context, cfg Config, h Handler, m *telemetry.Metrics) {
	dlq := &kafka.Writer{Addr: kafka.TCP(cfg.Brokers...), Topic: DLQTopic, AllowAutoTopicCreation: true,
		RequiredAcks: kafka.RequireAll, BatchTimeout: 5 * time.Millisecond}
	defer dlq.Close()

	lags := make([]atomic.Int64, cfg.Consumers)
	for i := 0; i < cfg.Consumers; i++ {
		i := i
		go func() {
			r := kafka.NewReader(kafka.ReaderConfig{
				Brokers: cfg.Brokers, GroupID: cfg.GroupID, Topic: Topic,
				MinBytes: 1, MaxBytes: 1 << 20, MaxWait: 200 * time.Millisecond,
				StartOffset: kafka.FirstOffset, // new groups read from the beginning
			})
			defer r.Close()
			for {
				msg, err := r.FetchMessage(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					log.Printf("kafka fetch: %v", err)
					time.Sleep(time.Second)
					continue
				}
				lags[i].Store(r.Stats().Lag)
				handle(ctx, cfg, h, m, dlq, msg)
				if err := r.CommitMessages(ctx, msg); err != nil && ctx.Err() == nil {
					log.Printf("kafka commit: %v", err) // will be redelivered; idempotency absorbs it
				}
			}
		}()
	}
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			var sum int64
			for i := range lags {
				sum += lags[i].Load()
			}
			m.ConsumerLag.Set(sum)
		}
	}
}

func handle(ctx context.Context, cfg Config, h Handler, m *telemetry.Metrics, dlq *kafka.Writer, msg kafka.Message) {
	var e events.Event
	if err := json.Unmarshal(msg.Value, &e); err != nil || e.Validate() != nil {
		m.Outcomes.Inc("invalid")
		_ = dlq.WriteMessages(ctx, kafka.Message{Key: msg.Key, Value: msg.Value,
			Headers: []kafka.Header{{Key: "error", Value: []byte("poison message")}}})
		return
	}
	if cfg.Delay > 0 {
		time.Sleep(cfg.Delay)
	}
	var err error
	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		if err = h(ctx, e); err == nil {
			return
		}
		if attempt < cfg.MaxAttempts {
			m.Outcomes.Inc("retried")
			select {
			case <-time.After(cfg.Backoff << (attempt - 1)):
			case <-ctx.Done():
				return // not committed -> redelivered after restart
			}
		}
	}
	m.Outcomes.Inc("dead_lettered")
	_ = dlq.WriteMessages(ctx, kafka.Message{Key: msg.Key, Value: msg.Value,
		Headers: []kafka.Header{{Key: "error", Value: []byte(err.Error())}}})
}
