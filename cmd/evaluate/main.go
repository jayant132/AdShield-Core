// Command evaluate replays a labelled synthetic dataset through the full
// pipeline (partitioned queue -> idempotent processor -> rules), injecting
// duplicate deliveries and transient failures, then reports detection quality
// and verifies payout correctness.
package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"hash/fnv"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/jayant132/adshield-core/internal/detect"
	"github.com/jayant132/adshield-core/internal/events"
	"github.com/jayant132/adshield-core/internal/pipeline"
	"github.com/jayant132/adshield-core/internal/queue"
	"github.com/jayant132/adshield-core/internal/simulator"
	"github.com/jayant132/adshield-core/internal/store"
	"github.com/jayant132/adshield-core/internal/telemetry"
)

func main() {
	legit := flag.Int("legit", 500, "number of legitimate devices")
	seed := flag.Int64("seed", 42, "random seed")
	dupEvery := flag.Int("dup-every", 7, "redeliver every Nth event a second time (0 = off)")
	failEvery := flag.Int("fail-every", 11, "fail the first attempt of every Nth event (0 = off)")
	threshold := flag.Int("threshold", 70, "device risk score at or above which a device is flagged")
	export := flag.String("export", "", "write the generated events to this CSV (for loading into Postgres/Trino)")
	flag.Parse()

	ds := simulator.Generate(*seed, *legit)

	if *export != "" {
		if err := writeCSV(*export, ds); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
	}

	m := telemetry.NewMetrics()
	st := store.New()
	eng := detect.NewEngine(detect.DefaultConfig(), st)
	proc := pipeline.New(st, eng, m)
	if *failEvery > 0 {
		proc.FailHook = func(e events.Event, attempt int) error {
			if attempt == 1 && hash(e.EventID)%uint32(*failEvery) == 0 {
				return pipeline.ErrTransient
			}
			return nil
		}
	}

	b := queue.New(8, 4096, 5, time.Millisecond)
	b.Hooks(func() { m.Outcomes.Inc("retried") }, func() { m.Outcomes.Inc("dead_lettered") })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b.Consume(ctx, proc.Handle)

	var uniqueRewardCents int64
	sent := 0
	for i, e := range ds.Events {
		if e.Type == events.Reward {
			uniqueRewardCents += e.RewardCents
		}
		_ = b.Publish(ctx, e)
		sent++
		if *dupEvery > 0 && i%*dupEvery == 0 {
			_ = b.Publish(ctx, e) // at-least-once redelivery of the same event_id
			sent++
		}
	}
	b.Drain()

	// ---- Detection quality (device level) ----
	byDev := map[string][]detect.Alert{}
	for _, a := range proc.Alerts() {
		byDev[a.DeviceID] = append(byDev[a.DeviceID], a)
	}
	var tp, fp, fn, tn int
	type kind struct{ tot, hit int }
	kinds := map[string]*kind{}
	var falsePos []string
	for dev, label := range ds.Labels {
		flagged := detect.RiskScore(byDev[dev]) >= *threshold
		isFraud := label == simulator.Fraud
		switch {
		case isFraud && flagged:
			tp++
		case isFraud && !flagged:
			fn++
		case !isFraud && flagged:
			fp++
			falsePos = append(falsePos, dev)
		default:
			tn++
		}
		k := kinds[ds.Kinds[dev]]
		if k == nil {
			k = &kind{}
			kinds[ds.Kinds[dev]] = k
		}
		k.tot++
		if flagged {
			k.hit++
		}
	}
	prec := ratio(tp, tp+fp)
	rec := ratio(tp, tp+fn)
	fpr := ratio(fp, fp+tn)

	fmt.Printf("Events generated: %d  delivered (incl. duplicates): %d\n", len(ds.Events), sent)
	fmt.Printf("Outcomes: processed=%d duplicate=%d failed_attempt=%d retried=%d dead_lettered=%d\n",
		m.Outcomes.Get("processed"), m.Outcomes.Get("duplicate"), m.Outcomes.Get("failed_attempt"),
		m.Outcomes.Get("retried"), m.Outcomes.Get("dead_lettered"))
	fmt.Printf("Payout check: expected=%d cents, actual=%d cents -> %s\n",
		uniqueRewardCents, st.TotalPayoutCents(), okStr(uniqueRewardCents == st.TotalPayoutCents()))
	fmt.Printf("Events processed exactly once: %d of %d -> %s\n",
		st.ProcessedCount(), len(ds.Events), okStr(st.ProcessedCount() == len(ds.Events)))

	fmt.Printf("\nDevice-level detection (threshold %d)\n", *threshold)
	fmt.Printf("  precision=%.3f recall=%.3f false-positive-rate=%.3f  (TP=%d FP=%d FN=%d TN=%d)\n", prec, rec, fpr, tp, fp, fn, tn)
	names := make([]string, 0, len(kinds))
	for k := range kinds {
		names = append(names, k)
	}
	sort.Strings(names)
	fmt.Println("\n  archetype             flagged / total")
	for _, n := range names {
		fmt.Printf("  %-20s  %4d / %-4d\n", n, kinds[n].hit, kinds[n].tot)
	}
	if len(falsePos) > 0 {
		sort.Strings(falsePos)
		fmt.Println("\n  false positives:", falsePos)
	}
	if uniqueRewardCents != st.TotalPayoutCents() || m.Outcomes.Get("dead_lettered") > 0 {
		os.Exit(1)
	}
}

func hash(s string) uint32 { h := fnv.New32a(); h.Write([]byte(s)); return h.Sum32() }
func ratio(a, b int) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b)
}
func okStr(ok bool) string {
	if ok {
		return "OK"
	}
	return "MISMATCH"
}

func writeCSV(path string, ds simulator.Dataset) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	_ = w.Write([]string{"event_id", "event_type", "device_id", "campaign_id", "publisher_id", "ip", "os", "event_ts", "reward_cents", "rooted", "hooked", "emulator", "ground_truth", "archetype"})
	for _, e := range ds.Events {
		_ = w.Write([]string{e.EventID, string(e.Type), e.DeviceID, e.CampaignID, e.PublisherID, e.IP, e.OS,
			e.Timestamp.Format(time.RFC3339), strconv.FormatInt(e.RewardCents, 10),
			strconv.FormatBool(e.Rooted), strconv.FormatBool(e.Hooked), strconv.FormatBool(e.Emulator),
			string(ds.Labels[e.DeviceID]), ds.Kinds[e.DeviceID]})
	}
	return nil
}
