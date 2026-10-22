// Package pipeline wires store + detection + metrics into one idempotent
// event processor that a queue consumer calls.
package pipeline

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jayant132/adshield-core/internal/detect"
	"github.com/jayant132/adshield-core/internal/events"
	"github.com/jayant132/adshield-core/internal/store"
	"github.com/jayant132/adshield-core/internal/telemetry"
)

var ErrTransient = errors.New("transient downstream failure")

// Idempotency is a conditional-write claim on an event_id. The in-memory Store
// implements it; so does the DynamoDB implementation (attribute_not_exists).
// Claim returns false when the event was already claimed (a duplicate).
type Idempotency interface {
	Claim(ctx context.Context, id string) (bool, error)
	Release(ctx context.Context, id string) error
}

type Processor struct {
	Idem   Idempotency // defaults to Store
	Store  *store.Store
	Engine *detect.Engine
	M      *telemetry.Metrics

	// FailHook lets tests/simulations inject downstream failures *after* the
	// idempotency key is written, which is the dangerous spot.
	FailHook func(e events.Event, attempt int) error

	mu       sync.Mutex
	alerts   []detect.Alert
	seen     map[string]struct{} // one alert per device per rule
	attempts map[string]int
}

func New(s *store.Store, eng *detect.Engine, m *telemetry.Metrics) *Processor {
	return &Processor{Idem: s, Store: s, Engine: eng, M: m, attempts: map[string]int{}, seen: map[string]struct{}{}}
}

// Handle is safe to call any number of times for the same event: the payout
// and history are applied exactly once.
func (p *Processor) Handle(ctx context.Context, e events.Event) error {
	start := time.Now()
	defer func() { p.M.ProcessSecs.Observe(time.Since(start).Seconds()) }()

	claimed, err := p.Idem.Claim(ctx, e.EventID)
	if err != nil {
		p.M.Outcomes.Inc("failed_attempt")
		return err // idempotency store unavailable: retry, never guess
	}
	if !claimed {
		p.M.Outcomes.Inc("duplicate")
		return nil
	}

	p.mu.Lock()
	p.attempts[e.EventID]++
	n := p.attempts[e.EventID]
	p.mu.Unlock()

	if p.FailHook != nil {
		if err := p.FailHook(e, n); err != nil {
			// Release the idempotency key, otherwise the retry would be
			// ignored as a "duplicate" and the event silently lost.
			_ = p.Idem.Release(ctx, e.EventID)
			p.M.Outcomes.Inc("failed_attempt")
			return err
		}
	}

	p.Store.Append(e)
	for _, a := range p.Engine.Evaluate(e) {
		key := a.DeviceID + "|" + a.Rule
		p.mu.Lock()
		_, dup := p.seen[key]
		if !dup {
			p.seen[key] = struct{}{}
			p.alerts = append(p.alerts, a)
		}
		p.mu.Unlock()
		if !dup {
			p.M.Alerts.Inc(a.Rule)
		}
	}
	p.M.Outcomes.Inc("processed")
	return nil
}

func (p *Processor) Alerts() []detect.Alert {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]detect.Alert(nil), p.alerts...)
}

func (p *Processor) DeviceAlerts(device string) []detect.Alert {
	var out []detect.Alert
	for _, a := range p.Alerts() {
		if a.DeviceID == device {
			out = append(out, a)
		}
	}
	return out
}
