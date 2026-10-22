// Package store holds the idempotency table and per-device event history.
// Behaviour mirrors DynamoDB: MarkProcessed is a conditional write
// (attribute_not_exists(event_id)) and device history is keyed by device_id
// (partition key) sorted by timestamp (sort key).
package store

import (
	"context"
	"sort"
	"sync"

	"github.com/jayant132/adshield-core/internal/events"
)

type Store struct {
	mu        sync.RWMutex
	processed map[string]struct{}
	byDevice  map[string][]events.Event
	byIP      map[string]map[string]struct{} // ip -> set of device ids
	payoutsC  int64
}

func New() *Store {
	return &Store{
		processed: make(map[string]struct{}),
		byDevice:  make(map[string][]events.Event),
		byIP:      make(map[string]map[string]struct{}),
	}
}

// MarkProcessed atomically records event_id. It returns false if the event was
// already processed (a duplicate delivery).
func (s *Store) MarkProcessed(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.processed[id]; ok {
		return false
	}
	s.processed[id] = struct{}{}
	return true
}

// Unmark releases an idempotency key so a failed event can be retried.
func (s *Store) Unmark(id string) {
	s.mu.Lock()
	delete(s.processed, id)
	s.mu.Unlock()
}

func (s *Store) Append(e events.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := append(s.byDevice[e.DeviceID], e)
	sort.SliceStable(h, func(i, j int) bool { return h[i].Timestamp.Before(h[j].Timestamp) })
	s.byDevice[e.DeviceID] = h
	if e.IP != "" {
		if s.byIP[e.IP] == nil {
			s.byIP[e.IP] = make(map[string]struct{})
		}
		s.byIP[e.IP][e.DeviceID] = struct{}{}
	}
	if e.Type == events.Reward {
		s.payoutsC += e.RewardCents
	}
}

// History returns a copy of a device's events in timestamp order.
func (s *Store) History(device string) []events.Event {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]events.Event(nil), s.byDevice[device]...)
}

// DevicesOnIP returns the device ids seen on an IP.
func (s *Store) DevicesOnIP(ip string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.byIP[ip]))
	for d := range s.byIP[ip] {
		out = append(out, d)
	}
	sort.Strings(out)
	return out
}

// TotalPayoutCents is the sum of all reward events accepted exactly once.
func (s *Store) TotalPayoutCents() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.payoutsC
}

func (s *Store) ProcessedCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.processed)
}

// Claim and Release let *Store satisfy pipeline.Idempotency (the same
// interface the DynamoDB-backed implementation provides).
func (s *Store) Claim(_ context.Context, id string) (bool, error) { return s.MarkProcessed(id), nil }
func (s *Store) Release(_ context.Context, id string) error       { s.Unmark(id); return nil }
