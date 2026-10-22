// Command server runs the AdShield Core ingestion + investigation API.
//
//	POST /v1/events               validate and enqueue an event (202)
//	GET  /v1/devices/{id}/risk    risk score, alerts and recent history
//	GET  /v1/alerts               all alerts (optional ?rule=)
//	GET  /v1/dlq                  dead-lettered events
//	GET  /healthz  /metrics       health and Prometheus metrics
//	GET  /debug/pprof/            Go profiling (pprof)
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	_ "net/http/pprof" // registers /debug/pprof/* on DefaultServeMux
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jayant132/adshield-core/internal/detect"
	"github.com/jayant132/adshield-core/internal/events"
	"github.com/jayant132/adshield-core/internal/pipeline"
	"github.com/jayant132/adshield-core/internal/queue"
	"github.com/jayant132/adshield-core/internal/store"
	"github.com/jayant132/adshield-core/internal/telemetry"
)

type publisher interface {
	Publish(ctx context.Context, e events.Event) error
}

type server struct {
	pub  publisher // in-memory broker by default, Kafka with -tags infra
	b    *queue.Broker
	proc *pipeline.Processor
	st   *store.Store
	m    *telemetry.Metrics
}

func main() {
	addr := env("ADDR", ":8080")
	m := telemetry.NewMetrics()
	st := store.New()
	proc := pipeline.New(st, detect.NewEngine(detect.DefaultConfig(), st), m)
	b := queue.New(8, 1024, 5, 50*time.Millisecond)
	b.Hooks(func() { m.Outcomes.Inc("retried") }, func() { m.Outcomes.Inc("dead_lettered") })

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var pub publisher = b
	if p, ok := initInfra(ctx, proc, m); ok { // Kafka / DynamoDB Local via -tags infra
		pub = p
	} else {
		b.Consume(ctx, proc.Handle)
	}
	go func() { // sample queue depth gauges
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				m.QueueDepth.Set(int64(b.Depth()))
				m.DLQDepth.Set(int64(len(b.DLQ())))
			}
		}
	}()

	s := &server{pub: pub, b: b, proc: proc, st: st, m: m}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/events", s.ingest)
	mux.HandleFunc("GET /v1/devices/{id}/risk", s.risk)
	mux.HandleFunc("GET /v1/alerts", s.alerts)
	mux.HandleFunc("GET /v1/dlq", s.dlq)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	mux.Handle("GET /metrics", m.Reg.Handler())
	mux.Handle("/debug/pprof/", http.DefaultServeMux) // pprof handlers

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		srv.Shutdown(sctx)
	}()
	log.Printf("adshield-core listening on %s", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func (s *server) ingest(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var e events.Event
	if err := json.NewDecoder(r.Body).Decode(&e); err != nil {
		s.m.Outcomes.Inc("invalid")
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "malformed JSON"})
		return
	}
	if err := e.Validate(); err != nil {
		s.m.Outcomes.Inc("invalid")
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.pub.Publish(ctx, e); err != nil { // queue full => shed load
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "queue full, retry later"})
		return
	}
	s.m.Events.Inc(string(e.Type))
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "queued", "event_id": e.EventID})
}

func (s *server) risk(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	hist := s.st.History(id)
	if len(hist) == 0 {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown device"})
		return
	}
	alerts := s.proc.DeviceAlerts(id)
	if len(hist) > 50 {
		hist = hist[len(hist)-50:]
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"device_id": id, "risk_score": detect.RiskScore(alerts), "alerts": alerts, "recent_events": hist,
	})
}

func (s *server) alerts(w http.ResponseWriter, r *http.Request) {
	rule := r.URL.Query().Get("rule")
	out := []detect.Alert{}
	for _, a := range s.proc.Alerts() {
		if rule == "" || strings.EqualFold(a.Rule, rule) {
			out = append(out, a)
		}
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *server) dlq(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.b.DLQ())
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
