package pipeline_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jayant132/adshield-core/internal/detect"
	"github.com/jayant132/adshield-core/internal/events"
	"github.com/jayant132/adshield-core/internal/pipeline"
	"github.com/jayant132/adshield-core/internal/queue"
	"github.com/jayant132/adshield-core/internal/store"
	"github.com/jayant132/adshield-core/internal/telemetry"
)

var t0 = time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

func newProc() (*pipeline.Processor, *store.Store) {
	st := store.New()
	return pipeline.New(st, detect.NewEngine(detect.DefaultConfig(), st), telemetry.NewMetrics()), st
}

func reward(id, dev string, at time.Time) events.Event {
	return events.Event{EventID: id, Type: events.Reward, DeviceID: dev, CampaignID: "c1", Timestamp: at, RewardCents: 100}
}

func TestDuplicateRewardPaidOnce(t *testing.T) {
	p, st := newProc()
	e := reward("r1", "d1", t0)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ { // 50 concurrent redeliveries of the same event
		wg.Add(1)
		go func() { defer wg.Done(); _ = p.Handle(context.Background(), e) }()
	}
	wg.Wait()
	if got := st.TotalPayoutCents(); got != 100 {
		t.Fatalf("payout = %d, want 100", got)
	}
}

func TestRetryAfterTransientFailureIsNotLost(t *testing.T) {
	p, st := newProc()
	p.FailHook = func(e events.Event, attempt int) error {
		if attempt < 3 {
			return pipeline.ErrTransient
		}
		return nil
	}
	b := queue.New(2, 16, 5, time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b.Consume(ctx, p.Handle)
	_ = b.Publish(ctx, reward("r1", "d1", t0))
	b.Drain()
	if st.TotalPayoutCents() != 100 || len(b.DLQ()) != 0 {
		t.Fatalf("payout=%d dlq=%d, want 100 and 0", st.TotalPayoutCents(), len(b.DLQ()))
	}
}

func TestPoisonEventGoesToDLQ(t *testing.T) {
	p, st := newProc()
	p.FailHook = func(events.Event, int) error { return pipeline.ErrTransient }
	b := queue.New(1, 16, 3, time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b.Consume(ctx, p.Handle)
	_ = b.Publish(ctx, reward("r1", "d1", t0))
	b.Drain()
	if len(b.DLQ()) != 1 || b.DLQ()[0].Attempts != 3 {
		t.Fatalf("expected 1 DLQ message after 3 attempts, got %+v", b.DLQ())
	}
	if st.TotalPayoutCents() != 0 {
		t.Fatal("a dead-lettered reward must not be paid")
	}
}

func TestRewardVelocityAlertCarriesEvidence(t *testing.T) {
	p, _ := newProc()
	for i := 0; i < 8; i++ {
		_ = p.Handle(context.Background(), reward(fmt.Sprintf("r%d", i), "d1", t0.Add(time.Duration(i)*10*time.Second)))
	}
	alerts := p.DeviceAlerts("d1")
	if len(alerts) != 1 || alerts[0].Rule != "reward_velocity" || len(alerts[0].EventIDs) <= 5 {
		t.Fatalf("unexpected alerts: %+v", alerts)
	}
}

func TestSharedIPAloneIsNotAFarm(t *testing.T) {
	p, _ := newProc()
	for i := 0; i < 10; i++ { // 10 devices on one NAT IP, rewards hours apart
		e := reward(fmt.Sprintf("r%d", i), fmt.Sprintf("d%d", i), t0.Add(time.Duration(i)*time.Hour))
		e.IP = "85.1.1.1"
		_ = p.Handle(context.Background(), e)
	}
	if n := len(p.Alerts()); n != 0 {
		t.Fatalf("shared IP produced %d alerts, want 0", n)
	}
}

func TestSynchronizedDevicesOnOneIPAreAFarm(t *testing.T) {
	p, _ := newProc()
	for i := 0; i < 6; i++ {
		e := reward(fmt.Sprintf("r%d", i), fmt.Sprintf("d%d", i), t0.Add(time.Duration(i)*5*time.Second))
		e.IP = "100.64.1.1"
		_ = p.Handle(context.Background(), e)
	}
	for i := 0; i < 6; i++ { // retroactive: all six devices are implicated
		if len(p.DeviceAlerts(fmt.Sprintf("d%d", i))) == 0 {
			t.Fatalf("device d%d not implicated", i)
		}
	}
}

func TestInstallTooFastAfterClick(t *testing.T) {
	p, _ := newProc()
	_ = p.Handle(context.Background(), events.Event{EventID: "c", Type: events.Click, DeviceID: "d", CampaignID: "c1", Timestamp: t0})
	_ = p.Handle(context.Background(), events.Event{EventID: "i", Type: events.Install, DeviceID: "d", CampaignID: "c1", Timestamp: t0.Add(2 * time.Second)})
	if a := p.DeviceAlerts("d"); len(a) != 1 || a[0].Rule != "click_to_install_too_fast" {
		t.Fatalf("unexpected alerts: %+v", a)
	}
}

func TestValidation(t *testing.T) {
	bad := []events.Event{
		{},
		{EventID: "x", Type: "bogus", DeviceID: "d", CampaignID: "c", Timestamp: t0},
		{EventID: "x", Type: events.Reward, DeviceID: "d", CampaignID: "c", Timestamp: t0}, // no amount
		{EventID: "x", Type: events.Click, DeviceID: "d", CampaignID: "c", Timestamp: t0, RewardCents: 5},
	}
	for i, e := range bad {
		if e.Validate() == nil {
			t.Errorf("case %d should be invalid", i)
		}
	}
}
