package main

import (
	"context"
	"fmt"
	"time"

	"github.com/olamij3/sentrygrid/internal/detect"
	"github.com/olamij3/sentrygrid/internal/pool"
	"github.com/olamij3/sentrygrid/internal/sensor"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	profiles := map[string]sensor.Profile{
		"temp-01": {
			BaseValue:     22.0,
			NoiseStdDev:   0.5,
			AnomalyChance: 0.10,
			DropoutChance: 0.02,
			DriftPerTick:  0.3,
			TickInterval:  200 * time.Millisecond,
		},
		"temp-02": {
			BaseValue:     19.0,
			NoiseStdDev:   0.4,
			AnomalyChance: 0.08,
			DropoutChance: 0.02,
			DriftPerTick:  0.2,
			TickInterval:  300 * time.Millisecond,
		},
	}

	readings := sensor.StartDevices(ctx, profiles)
	registry := detect.NewRegistry()
	anomalies := pool.Run(ctx, readings, 4, registry)

	fmt.Println("SentryGrid running — watching for anomalies for 10 seconds...")
	fmt.Println()

	for a := range anomalies {
		fmt.Printf("🚨 ANOMALY  [%s]  device=%-8s  value=%.4f  method=%-8s  score=%.4f\n",
			a.Timestamp.Format("15:04:05.000"), a.DeviceID, a.Value, a.Method, a.Score)
	}

	fmt.Println("\nDone. All workers stopped cleanly.")
}