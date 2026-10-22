// Package queue is an in-process stand-in for Kafka/SQS with the same failure
// semantics that matter for fraud pipelines: at-least-once delivery, bounded
// retries with backoff, and a dead-letter queue (DLQ).
//
// The Broker interface is what a real Kafka/SQS adapter would implement.
package queue

import (
	"context"
	"hash/fnv"
	"sync"
	"time"

	"github.com/jayant132/adshield-core/internal/events"
)

type Message struct {
	Event    events.Event
	Attempts int
}

// Handler returns an error for retryable failures. A nil error acks.
type Handler func(ctx context.Context, e events.Event) error

type Broker struct {
	parts       []chan Message // one channel per partition, keyed by device_id
	mu          sync.Mutex
	dlq         []Message
	maxAttempts int
	backoff     time.Duration
	wg          sync.WaitGroup
	inflight    sync.WaitGroup
	onRetry     func()
	onDLQ       func()
}

// New creates a broker with the given number of partitions. Events are routed
// by device_id so one device's events are consumed in order by one goroutine,
// exactly like a Kafka topic keyed by device_id.
func New(partitions, buffer, maxAttempts int, backoff time.Duration) *Broker {
	b := &Broker{maxAttempts: maxAttempts, backoff: backoff}
	for i := 0; i < partitions; i++ {
		b.parts = append(b.parts, make(chan Message, buffer))
	}
	return b
}

func (b *Broker) partition(device string) int {
	h := fnv.New32a()
	h.Write([]byte(device))
	return int(h.Sum32()) % len(b.parts)
}

// Hooks lets the caller count retries and dead-letters.
func (b *Broker) Hooks(onRetry, onDLQ func()) { b.onRetry, b.onDLQ = onRetry, onDLQ }

// Publish enqueues an event. It blocks when the buffer is full (back-pressure).
func (b *Broker) Publish(ctx context.Context, e events.Event) error {
	b.inflight.Add(1)
	select {
	case b.parts[b.partition(e.DeviceID)] <- Message{Event: e}:
		return nil
	case <-ctx.Done():
		b.inflight.Done()
		return ctx.Err()
	}
}

func (b *Broker) Depth() int {
	n := 0
	for _, p := range b.parts {
		n += len(p)
	}
	return n
}

func (b *Broker) DLQ() []Message {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]Message(nil), b.dlq...)
}

// Consume starts one consumer goroutine per partition (a consumer group).
func (b *Broker) Consume(ctx context.Context, h Handler) {
	for i := range b.parts {
		ch := b.parts[i]
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case m := <-ch:
					b.handle(ctx, m, h)
				}
			}
		}()
	}
}

func (b *Broker) handle(ctx context.Context, m Message, h Handler) {
	m.Attempts++
	if err := h(ctx, m.Event); err == nil {
		b.inflight.Done()
		return
	}
	if m.Attempts >= b.maxAttempts {
		b.mu.Lock()
		b.dlq = append(b.dlq, m)
		b.mu.Unlock()
		if b.onDLQ != nil {
			b.onDLQ()
		}
		b.inflight.Done()
		return
	}
	if b.onRetry != nil {
		b.onRetry()
	}
	// Exponential backoff, then redeliver without blocking the consumer.
	delay := b.backoff << (m.Attempts - 1)
	go func() {
		select {
		case <-time.After(delay):
			select {
			case b.parts[b.partition(m.Event.DeviceID)] <- m:
			case <-ctx.Done():
				b.inflight.Done()
			}
		case <-ctx.Done():
			b.inflight.Done()
		}
	}()
}

// Drain blocks until every published message is acked or dead-lettered.
func (b *Broker) Drain() { b.inflight.Wait() }
