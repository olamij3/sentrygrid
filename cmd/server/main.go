package main

import (
	"context"
	"fmt"
	"time"

	"github.com/olamij3/sentrygrid/internal/sensor"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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

	fmt.Println("SentryGrid starting — reading for 5 seconds...")
	fmt.Println()

	for r := range sensor.StartDevices(ctx, profiles) {
		fmt.Printf("[%s]  device=%-8s  value=%.4f\n",
			r.Timestamp.Format("15:04:05.000"), r.DeviceID, r.Value)
	}

	fmt.Println("\nDone. All devices stopped cleanly.")
}
