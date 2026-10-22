// Package detect contains explainable, rule-based fraud detection.
// Every alert carries a rule name, score, human-readable reason and the
// event IDs that triggered it, so an investigator can verify it in SQL.
package detect

import (
	"fmt"
	"sort"
	"time"

	"github.com/jayant132/adshield-core/internal/events"
)

type Alert struct {
	DeviceID string    `json:"device_id"`
	Rule     string    `json:"rule"`
	Score    int       `json:"score"`
	Reason   string    `json:"reason"`
	EventIDs []string  `json:"event_ids"`
	At       time.Time `json:"at"`
}

// Config thresholds. Tune them against labelled data (see cmd/evaluate).
type Config struct {
	RewardWindow       time.Duration
	RewardMaxInWindow  int           // more than this many rewards in window => farming
	MinClickToInstall  time.Duration // faster than this => injected install
	ClickSpamThreshold int           // clicks on one campaign with no install
	FarmMinDevices     int           // distinct devices on a shared IP
	FarmSyncWindow     time.Duration // reward timestamps within this window count as synchronized
}

func DefaultConfig() Config {
	return Config{
		RewardWindow:       5 * time.Minute,
		RewardMaxInWindow:  5,
		MinClickToInstall:  10 * time.Second,
		ClickSpamThreshold: 20,
		FarmMinDevices:     5,
		FarmSyncWindow:     2 * time.Minute,
	}
}

// DeviceHistory gives rules read access to history without importing store.
type DeviceHistory interface {
	History(device string) []events.Event
	DevicesOnIP(ip string) []string
}

type Engine struct {
	cfg Config
	h   DeviceHistory
}

func NewEngine(cfg Config, h DeviceHistory) *Engine { return &Engine{cfg: cfg, h: h} }

// Evaluate runs all rules for the device that produced e (e must already be
// appended to history) and returns every alert that fires.
func (g *Engine) Evaluate(e events.Event) []Alert {
	hist := g.h.History(e.DeviceID)
	var out []Alert
	out = append(out, g.rewardVelocity(e, hist)...)
	out = append(out, g.clickToInstall(e, hist)...)
	if e.Type == events.Click {
		// A click can arrive after its install (retry / redelivery reordering):
		// re-check installs already seen on this campaign.
		for _, x := range hist {
			if x.Type == events.Install && x.CampaignID == e.CampaignID && !x.Timestamp.Before(e.Timestamp) {
				out = append(out, g.clickToInstall(x, hist)...)
			}
		}
	}
	out = append(out, g.clickSpam(e, hist)...)
	out = append(out, g.integrity(e)...)
	out = append(out, g.deviceFarm(e)...)
	return out
}

// Rule 1: reward farming = too many rewards inside a sliding window.
func (g *Engine) rewardVelocity(e events.Event, hist []events.Event) []Alert {
	if e.Type != events.Reward {
		return nil
	}
	var ids []string
	for _, x := range hist {
		if x.Type == events.Reward && !x.Timestamp.After(e.Timestamp) && e.Timestamp.Sub(x.Timestamp) <= g.cfg.RewardWindow {
			ids = append(ids, x.EventID)
		}
	}
	if len(ids) <= g.cfg.RewardMaxInWindow {
		return nil
	}
	return []Alert{{
		DeviceID: e.DeviceID, Rule: "reward_velocity", Score: 85, At: e.Timestamp, EventIDs: ids,
		Reason: fmt.Sprintf("%d rewards within %s (limit %d)", len(ids), g.cfg.RewardWindow, g.cfg.RewardMaxInWindow),
	}}
}

// Rule 2: click injection = install attributed implausibly fast after the click.
func (g *Engine) clickToInstall(e events.Event, hist []events.Event) []Alert {
	if e.Type != events.Install {
		return nil
	}
	var last *events.Event
	for i := range hist {
		x := hist[i]
		if x.Type == events.Click && x.CampaignID == e.CampaignID && !x.Timestamp.After(e.Timestamp) {
			last = &hist[i]
		}
	}
	if last == nil {
		return []Alert{{
			DeviceID: e.DeviceID, Rule: "install_without_click", Score: 60, At: e.Timestamp,
			EventIDs: []string{e.EventID},
			Reason:   "install has no preceding click on this campaign (possible SDK spoofing / attribution gap)",
		}}
	}
	d := e.Timestamp.Sub(last.Timestamp)
	if d < g.cfg.MinClickToInstall {
		return []Alert{{
			DeviceID: e.DeviceID, Rule: "click_to_install_too_fast", Score: 80, At: e.Timestamp,
			EventIDs: []string{last.EventID, e.EventID},
			Reason:   fmt.Sprintf("install %s after click (minimum plausible %s)", d.Round(time.Millisecond), g.cfg.MinClickToInstall),
		}}
	}
	return nil
}

// Rule 3: click spam = lots of clicks on a campaign that never convert.
func (g *Engine) clickSpam(e events.Event, hist []events.Event) []Alert {
	if e.Type != events.Click {
		return nil
	}
	var ids []string
	installed := false
	for _, x := range hist {
		if x.CampaignID != e.CampaignID {
			continue
		}
		if x.Type == events.Click {
			ids = append(ids, x.EventID)
		}
		if x.Type == events.Install {
			installed = true
		}
	}
	if installed || len(ids) < g.cfg.ClickSpamThreshold || len(ids) != g.cfg.ClickSpamThreshold {
		return nil // fire once, when the threshold is first crossed
	}
	return []Alert{{
		DeviceID: e.DeviceID, Rule: "click_spam", Score: 70, At: e.Timestamp, EventIDs: ids,
		Reason: fmt.Sprintf("%d clicks on campaign %s with zero installs", len(ids), e.CampaignID),
	}}
}

// Rule 4: SDK-reported device tampering. Scored moderate on its own because
// rooted devices are not necessarily fraudulent; it is a strong *combiner*.
func (g *Engine) integrity(e events.Event) []Alert {
	if e.Type != events.Reward || !(e.Hooked || e.Emulator || e.Rooted) {
		return nil
	}
	sig := ""
	switch {
	case e.Hooked:
		sig = "hooking framework (Xposed/Frida)"
	case e.Emulator:
		sig = "emulator"
	default:
		sig = "rooted / jailbroken"
	}
	score := 50
	if e.Hooked || e.Emulator {
		score = 75
	}
	return []Alert{{
		DeviceID: e.DeviceID, Rule: "device_integrity", Score: score, At: e.Timestamp,
		EventIDs: []string{e.EventID}, Reason: "reward earned on device with " + sig,
	}}
}

// Rule 5: device farm = many distinct devices on one IP earning rewards in
// lock-step. A shared IP alone is NOT enough (carrier NAT, offices, campuses).
func (g *Engine) deviceFarm(e events.Event) []Alert {
	if e.Type != events.Reward || e.IP == "" {
		return nil
	}
	devs := g.h.DevicesOnIP(e.IP)
	if len(devs) < g.cfg.FarmMinDevices {
		return nil
	}
	synced := map[string]string{}
	for _, d := range devs {
		for _, x := range g.h.History(d) {
			if x.Type == events.Reward && abs(x.Timestamp.Sub(e.Timestamp)) <= g.cfg.FarmSyncWindow {
				synced[d] = x.EventID
				break
			}
		}
	}
	if len(synced) < g.cfg.FarmMinDevices {
		return nil
	}
	// Retroactive: the moment the cluster crosses the threshold, every device in
	// it is implicated, not just the one whose event tipped it over.
	peers := make([]string, 0, len(synced))
	for d := range synced {
		peers = append(peers, d)
	}
	sort.Strings(peers)
	ids := make([]string, 0, len(peers))
	for _, d := range peers {
		ids = append(ids, synced[d])
	}
	var out []Alert
	for _, d := range peers {
		out = append(out, Alert{
			DeviceID: d, Rule: "device_farm", Score: 90, At: e.Timestamp, EventIDs: ids,
			Reason: fmt.Sprintf("%d devices on IP %s earned rewards within %s of each other", len(synced), e.IP, g.cfg.FarmSyncWindow),
		})
	}
	return out
}

func abs(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

// RiskScore combines alerts: the max rule score plus a bonus for every
// additional distinct rule, capped at 100.
func RiskScore(alerts []Alert) int {
	if len(alerts) == 0 {
		return 0
	}
	max, rules := 0, map[string]struct{}{}
	for _, a := range alerts {
		if a.Score > max {
			max = a.Score
		}
		rules[a.Rule] = struct{}{}
	}
	s := max + 5*(len(rules)-1)
	if s > 100 {
		s = 100
	}
	return s
}
