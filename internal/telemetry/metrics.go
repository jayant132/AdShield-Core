// Package telemetry is a tiny dependency-free metrics registry that serves the
// Prometheus text exposition format. The API mirrors client_golang closely
// enough that swapping it in later is a one-file change.
package telemetry

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

type CounterVec struct {
	name, help string
	mu         sync.Mutex
	vals       map[string]*atomic.Int64
}

func (c *CounterVec) Inc(label string) { c.Add(label, 1) }
func (c *CounterVec) Add(label string, n int64) {
	c.mu.Lock()
	v, ok := c.vals[label]
	if !ok {
		v = new(atomic.Int64)
		c.vals[label] = v
	}
	c.mu.Unlock()
	v.Add(n)
}
func (c *CounterVec) Get(label string) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if v, ok := c.vals[label]; ok {
		return v.Load()
	}
	return 0
}

type Gauge struct {
	name, help string
	v          atomic.Int64
}

func (g *Gauge) Set(n int64) { g.v.Store(n) }
func (g *Gauge) Add(n int64) { g.v.Add(n) }

type Histogram struct {
	name, help string
	bounds     []float64
	mu         sync.Mutex
	counts     []int64
	sum        float64
	n          int64
}

func (h *Histogram) Observe(v float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i, b := range h.bounds {
		if v <= b {
			h.counts[i]++
		}
	}
	h.sum += v
	h.n++
}

type Registry struct {
	mu       sync.Mutex
	counters []*CounterVec
	gauges   []*Gauge
	hists    []*Histogram
	label    map[*CounterVec]string
}

func NewRegistry() *Registry { return &Registry{label: map[*CounterVec]string{}} }

func (r *Registry) Counter(name, help, labelName string) *CounterVec {
	c := &CounterVec{name: name, help: help, vals: map[string]*atomic.Int64{}}
	r.mu.Lock()
	r.counters = append(r.counters, c)
	r.label[c] = labelName
	r.mu.Unlock()
	return c
}

func (r *Registry) Gauge(name, help string) *Gauge {
	g := &Gauge{name: name, help: help}
	r.mu.Lock()
	r.gauges = append(r.gauges, g)
	r.mu.Unlock()
	return g
}

func (r *Registry) Histogram(name, help string, bounds []float64) *Histogram {
	h := &Histogram{name: name, help: help, bounds: bounds, counts: make([]int64, len(bounds))}
	r.mu.Lock()
	r.hists = append(r.hists, h)
	r.mu.Unlock()
	return h
}

// Handler serves /metrics.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		r.Write(w)
	})
}

func (r *Registry) Write(w io.Writer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range r.counters {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n", c.name, c.help, c.name)
		c.mu.Lock()
		keys := make([]string, 0, len(c.vals))
		for k := range c.vals {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(w, "%s{%s=%q} %d\n", c.name, r.label[c], k, c.vals[k].Load())
		}
		c.mu.Unlock()
	}
	for _, g := range r.gauges {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n%s %d\n", g.name, g.help, g.name, g.name, g.v.Load())
	}
	for _, h := range r.hists {
		h.mu.Lock()
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s histogram\n", h.name, h.help, h.name)
		for i, b := range h.bounds {
			fmt.Fprintf(w, "%s_bucket{le=\"%s\"} %d\n", h.name, trim(b), h.counts[i])
		}
		fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n%s_sum %g\n%s_count %d\n", h.name, h.n, h.name, h.sum, h.name, h.n)
		h.mu.Unlock()
	}
}

func trim(f float64) string {
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%f", f), "0"), ".")
}

// Metrics bundles the service's instruments.
type Metrics struct {
	Reg         *Registry
	Events      *CounterVec // by event_type
	Outcomes    *CounterVec // processed | duplicate | invalid | retried | dead_lettered
	Alerts      *CounterVec // by rule
	QueueDepth  *Gauge
	DLQDepth    *Gauge
	ConsumerLag *Gauge // Kafka consumer-group lag (messages behind the log end)
	ProcessSecs *Histogram
}

func NewMetrics() *Metrics {
	r := NewRegistry()
	return &Metrics{
		Reg:         r,
		Events:      r.Counter("adshield_events_total", "Events accepted by the ingestion API.", "event_type"),
		Outcomes:    r.Counter("adshield_processing_outcomes_total", "Consumer outcomes per delivery.", "outcome"),
		Alerts:      r.Counter("adshield_fraud_alerts_total", "Fraud alerts by rule.", "rule"),
		QueueDepth:  r.Gauge("adshield_queue_depth", "Messages waiting in the event queue."),
		DLQDepth:    r.Gauge("adshield_dlq_depth", "Messages in the dead-letter queue."),
		ConsumerLag: r.Gauge("adshield_consumer_lag", "Kafka consumer lag in messages (sum over partitions)."),
		ProcessSecs: r.Histogram("adshield_process_seconds", "Per-event processing latency.", []float64{.0001, .0005, .001, .005, .01, .05, .1, .5, 1}),
	}
}
