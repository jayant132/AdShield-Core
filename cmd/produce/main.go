//go:build infra

// Command produce publishes labelled synthetic traffic to Kafka at a fixed
// rate, for lag experiments. Each loop uses a different seed and prefixes
// device/event IDs so loops never collide.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jayant132/adshield-core/internal/kafkaq"
	"github.com/jayant132/adshield-core/internal/simulator"
)

func main() {
	brokers := flag.String("brokers", "localhost:9092", "comma-separated Kafka brokers")
	rate := flag.Int("rate", 300, "events per second")
	secs := flag.Int("seconds", 20, "how long to produce")
	flag.Parse()

	p := kafkaq.NewProducer(strings.Split(*brokers, ","), true)
	defer p.Close()
	ctx := context.Background()

	tick := time.NewTicker(time.Second / time.Duration(*rate))
	defer tick.Stop()
	deadline := time.Now().Add(time.Duration(*secs) * time.Second)
	sent, loop := 0, 0
	for time.Now().Before(deadline) {
		ds := simulator.Generate(int64(1000+loop), 200)
		for _, e := range ds.Events {
			if !time.Now().Before(deadline) {
				break
			}
			<-tick.C
			e.EventID = fmt.Sprintf("l%d-%s", loop, e.EventID)
			e.DeviceID = fmt.Sprintf("l%d-%s", loop, e.DeviceID)
			if err := p.Publish(ctx, e); err != nil {
				log.Fatal(err)
			}
			sent++
		}
		loop++
	}
	log.Printf("produced %d events in %ds (~%d/s)", sent, *secs, sent / *secs)
}
