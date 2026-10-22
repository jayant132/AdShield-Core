//go:build !infra

package main

import (
	"context"

	"github.com/jayant132/adshield-core/internal/pipeline"
	"github.com/jayant132/adshield-core/internal/telemetry"
)

// initInfra is a no-op in the default build (in-memory queue and store).
func initInfra(context.Context, *pipeline.Processor, *telemetry.Metrics) (publisher, bool) {
	return nil, false
}
