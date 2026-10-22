// Package simulator generates labelled synthetic mobile attribution traffic:
// legitimate users, five fraud archetypes and one deliberately ambiguous
// group (a shared-IP office) that must NOT be flagged as a farm.
package simulator

import (
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/jayant132/adshield-core/internal/events"
)

type Label string

const (
	Legit  Label = "legit"
	Fraud  Label = "fraud"
	Shared Label = "legit_shared_ip" // ambiguous but legitimate
)

type Dataset struct {
	Events []events.Event
	Labels map[string]Label  // device_id -> ground truth
	Kinds  map[string]string // device_id -> archetype
}

// Generate builds a deterministic dataset from seed.
func Generate(seed int64, legit int) Dataset {
	r := rand.New(rand.NewSource(seed))
	base := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	d := Dataset{Labels: map[string]Label{}, Kinds: map[string]string{}}
	n := 0
	id := func() string { n++; return fmt.Sprintf("evt-%06d", n) }
	add := func(e events.Event) { e.EventID = id(); d.Events = append(d.Events, e) }
	mark := func(dev string, l Label, kind string) { d.Labels[dev] = l; d.Kinds[dev] = kind }

	// 1. Legitimate users: click, install after 30s-20m, a few rewards spread over hours.
	for i := 0; i < legit; i++ {
		dev := fmt.Sprintf("dev-legit-%04d", i)
		mark(dev, Legit, "legit")
		camp := fmt.Sprintf("camp-%d", r.Intn(5))
		ip := fmt.Sprintf("10.%d.%d.%d", r.Intn(250), r.Intn(250), r.Intn(250))
		t := base.Add(time.Duration(r.Intn(3600)) * time.Second)
		add(events.Event{Type: events.Click, DeviceID: dev, CampaignID: camp, PublisherID: "pub-1", IP: ip, OS: "android", Timestamp: t})
		t = t.Add(time.Duration(30+r.Intn(1200)) * time.Second)
		add(events.Event{Type: events.Install, DeviceID: dev, CampaignID: camp, PublisherID: "pub-1", IP: ip, OS: "android", Timestamp: t})
		for k := 0; k < 1+r.Intn(3); k++ {
			t = t.Add(time.Duration(10+r.Intn(50)) * time.Minute)
			add(events.Event{Type: events.Reward, DeviceID: dev, CampaignID: camp, IP: ip, OS: "android", Timestamp: t, RewardCents: int64(10 + r.Intn(40))})
		}
	}

	// 2. Reward farmers: normal install, then 12 rewards in 3 minutes.
	for i := 0; i < 8; i++ {
		dev := fmt.Sprintf("dev-farmer-%02d", i)
		mark(dev, Fraud, "reward_farming")
		ip := fmt.Sprintf("172.16.%d.9", i)
		t := base.Add(time.Duration(r.Intn(1800)) * time.Second)
		add(events.Event{Type: events.Click, DeviceID: dev, CampaignID: "camp-1", IP: ip, Timestamp: t})
		t = t.Add(4 * time.Minute)
		add(events.Event{Type: events.Install, DeviceID: dev, CampaignID: "camp-1", IP: ip, Timestamp: t})
		for k := 0; k < 12; k++ {
			t = t.Add(15 * time.Second)
			add(events.Event{Type: events.Reward, DeviceID: dev, CampaignID: "camp-1", IP: ip, Timestamp: t, RewardCents: 25})
		}
	}

	// 3. Click injection: install 1-3 seconds after the click.
	for i := 0; i < 8; i++ {
		dev := fmt.Sprintf("dev-inject-%02d", i)
		mark(dev, Fraud, "click_injection")
		ip := fmt.Sprintf("192.168.%d.5", 100+i)
		t := base.Add(time.Duration(r.Intn(1800)) * time.Second)
		add(events.Event{Type: events.Click, DeviceID: dev, CampaignID: "camp-2", IP: ip, Timestamp: t})
		add(events.Event{Type: events.Install, DeviceID: dev, CampaignID: "camp-2", IP: ip, Timestamp: t.Add(time.Duration(1+r.Intn(3)) * time.Second)})
	}

	// 4. Click spam: 25 clicks, never installs.
	for i := 0; i < 6; i++ {
		dev := fmt.Sprintf("dev-spam-%02d", i)
		mark(dev, Fraud, "click_spam")
		ip := fmt.Sprintf("198.51.%d.7", i)
		t := base
		for k := 0; k < 25; k++ {
			t = t.Add(time.Duration(5+r.Intn(20)) * time.Second)
			add(events.Event{Type: events.Click, DeviceID: dev, CampaignID: "camp-3", IP: ip, Timestamp: t})
		}
	}

	// 5. Hooked/emulator devices (Xposed/Frida, emulator farms) earning rewards.
	for i := 0; i < 6; i++ {
		dev := fmt.Sprintf("dev-hooked-%02d", i)
		mark(dev, Fraud, "device_tampering")
		ip := fmt.Sprintf("203.0.113.%d", 10+i)
		t := base.Add(time.Duration(r.Intn(1800)) * time.Second)
		add(events.Event{Type: events.Click, DeviceID: dev, CampaignID: "camp-4", IP: ip, Timestamp: t})
		t = t.Add(3 * time.Minute)
		add(events.Event{Type: events.Install, DeviceID: dev, CampaignID: "camp-4", IP: ip, Timestamp: t})
		t = t.Add(20 * time.Minute)
		add(events.Event{Type: events.Reward, DeviceID: dev, CampaignID: "camp-4", IP: ip, Timestamp: t, RewardCents: 30, Hooked: i%2 == 0, Emulator: i%2 == 1})
	}

	// 6. Device farm: 12 devices on one IP, rewards in lock-step, each slow enough
	// to evade per-device velocity rules.
	for i := 0; i < 12; i++ {
		dev := fmt.Sprintf("dev-farm-%02d", i)
		mark(dev, Fraud, "device_farm")
		ip := "100.64.7.7"
		t := base.Add(2 * time.Hour)
		add(events.Event{Type: events.Click, DeviceID: dev, CampaignID: "camp-1", IP: ip, Timestamp: t})
		t = t.Add(5 * time.Minute)
		add(events.Event{Type: events.Install, DeviceID: dev, CampaignID: "camp-1", IP: ip, Timestamp: t})
		for k := 0; k < 3; k++ {
			t = t.Add(time.Duration(20+i) * time.Second)
			add(events.Event{Type: events.Reward, DeviceID: dev, CampaignID: "camp-1", IP: ip, Timestamp: t, RewardCents: 20})
			t = t.Add(10 * time.Minute)
		}
	}

	// 7. Ambiguous but legitimate: 8 office devices behind one NAT IP, rewards spread
	// across the working day (not synchronized). Shared IP alone must not alert.
	for i := 0; i < 8; i++ {
		dev := fmt.Sprintf("dev-office-%02d", i)
		mark(dev, Shared, "shared_ip_office")
		ip := "85.214.1.1"
		t := base.Add(time.Duration(i) * 25 * time.Minute)
		add(events.Event{Type: events.Click, DeviceID: dev, CampaignID: "camp-0", IP: ip, Timestamp: t})
		t = t.Add(time.Duration(2+r.Intn(10)) * time.Minute)
		add(events.Event{Type: events.Install, DeviceID: dev, CampaignID: "camp-0", IP: ip, Timestamp: t})
		for k := 0; k < 2; k++ {
			t = t.Add(time.Duration(40+r.Intn(60)) * time.Minute)
			add(events.Event{Type: events.Reward, DeviceID: dev, CampaignID: "camp-0", IP: ip, Timestamp: t, RewardCents: 20})
		}
	}

	// Deliver in global time order, like a real stream.
	sortByTime(d.Events)
	return d
}

func sortByTime(es []events.Event) {
	sort.SliceStable(es, func(i, j int) bool { return es[i].Timestamp.Before(es[j].Timestamp) })
}
