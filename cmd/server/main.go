package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/olamij3/sentrygrid/internal/detect"
	"github.com/olamij3/sentrygrid/internal/pool"
	"github.com/olamij3/sentrygrid/internal/sensor"
	"github.com/olamij3/sentrygrid/internal/store"
)

// tee fans one readings channel into two independent channels
func tee(ctx context.Context, in <-chan sensor.Reading) (<-chan sensor.Reading, <-chan sensor.Reading) {
	out1 := make(chan sensor.Reading, 256)
	out2 := make(chan sensor.Reading, 256)

	go func() {
		defer close(out1)
		defer close(out2)
		for {
			select {
			case <-ctx.Done():
				return
			case r, ok := <-in:
				if !ok {
					return
				}
				out1 <- r
				out2 <- r
			}
		}
	}()

	return out1, out2
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// open database
	db, err := store.Open("sentrygrid.db")
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	fmt.Println("Database opened: sentrygrid.db")

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

	// start devices
	readings := sensor.StartDevices(ctx, profiles)

	// tee: one copy for detection, one for storage
	forDetection, forStorage := tee(ctx, readings)

	// start anomaly detection
	registry := detect.NewRegistry()
	anomalies := pool.Run(ctx, forDetection, 4, registry)

	// tee anomalies: one for storage, one for printing
	anomalyCh1 := make(chan pool.Anomaly, 64)
	anomalyCh2 := make(chan pool.Anomaly, 64)
	go func() {
		defer close(anomalyCh1)
		defer close(anomalyCh2)
		for a := range anomalies {
			anomalyCh1 <- a
			anomalyCh2 <- a
		}
	}()

	// start database writer
	db.RunWriter(ctx, forStorage, anomalyCh1)

	fmt.Println("SentryGrid running — watching for anomalies for 10 seconds...")
	fmt.Println()

	for a := range anomalyCh2 {
		fmt.Printf("🚨 ANOMALY  [%s]  device=%-8s  value=%.4f  method=%-8s  score=%.4f\n",
			a.Timestamp.Format("15:04:05.000"), a.DeviceID, a.Value, a.Method, a.Score)
	}

	fmt.Println("\nDone. All data saved to sentrygrid.db")
}