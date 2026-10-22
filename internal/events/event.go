// Package events defines the attribution lifecycle events:
// click -> install -> session -> reward.
package events

import (
	"errors"
	"fmt"
	"time"
)

type Type string

const (
	Click   Type = "click"
	Install Type = "install"
	Session Type = "session"
	Reward  Type = "reward"
)

// Event is one telemetry record. EventID is the idempotency key: the same
// EventID delivered twice (Kafka/SQS redelivery) must only be processed once.
type Event struct {
	EventID     string    `json:"event_id"`
	Type        Type      `json:"event_type"`
	DeviceID    string    `json:"device_id"`
	CampaignID  string    `json:"campaign_id"`
	PublisherID string    `json:"publisher_id,omitempty"`
	IP          string    `json:"ip,omitempty"`
	OS          string    `json:"os,omitempty"`
	Timestamp   time.Time `json:"timestamp"`
	RewardCents int64     `json:"reward_cents,omitempty"`

	// Device integrity signals reported by the SDK (Magisk, Xposed, jailbreak, emulator).
	Rooted   bool `json:"rooted,omitempty"`
	Hooked   bool `json:"hooked,omitempty"`
	Emulator bool `json:"emulator,omitempty"`
}

var ErrInvalid = errors.New("invalid event")

func (e *Event) Validate() error {
	switch e.Type {
	case Click, Install, Session, Reward:
	default:
		return fmt.Errorf("%w: unknown event_type %q", ErrInvalid, e.Type)
	}
	if e.EventID == "" || e.DeviceID == "" || e.CampaignID == "" {
		return fmt.Errorf("%w: event_id, device_id and campaign_id are required", ErrInvalid)
	}
	if e.Timestamp.IsZero() {
		return fmt.Errorf("%w: timestamp is required", ErrInvalid)
	}
	if e.Type == Reward && e.RewardCents <= 0 {
		return fmt.Errorf("%w: reward events need reward_cents > 0", ErrInvalid)
	}
	if e.Type != Reward && e.RewardCents != 0 {
		return fmt.Errorf("%w: only reward events may carry reward_cents", ErrInvalid)
	}
	return nil
}
