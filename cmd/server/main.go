package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/olamij3/sentrygrid/internal/api"
	"github.com/olamij3/sentrygrid/internal/detect"
	"github.com/olamij3/sentrygrid/internal/pool"
	"github.com/olamij3/sentrygrid/internal/sensor"
	"github.com/olamij3/sentrygrid/internal/store"
)

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

func teeAnomalies(ctx context.Context, in <-chan pool.Anomaly) (<-chan pool.Anomaly, <-chan pool.Anomaly) {
	out1 := make(chan pool.Anomaly, 64)
	out2 := make(chan pool.Anomaly, 64)
	go func() {
		defer close(out1)
		defer close(out2)
		for {
			select {
			case <-ctx.Done():
				return
			case a, ok := <-in:
				if !ok {
					return
				}
				out1 <- a
				out2 <- a
			}
		}
	}()
	return out1, out2
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
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

	// pipeline
	readings := sensor.StartDevices(ctx, profiles)
	forDetection, forStorage := tee(ctx, readings)
	registry := detect.NewRegistry()
	anomalies := pool.Run(ctx, forDetection, 4, registry)
	forStorage2, forBroadcast := teeAnomalies(ctx, anomalies)

	// database writer
	db.RunWriter(ctx, forStorage, forStorage2)

	// websocket hub
	hub := api.NewHub()
	hub.Broadcast(ctx, forBroadcast)

	// HTTP routes
	server := api.New(db)
	mux := server.Routes()
	mux.HandleFunc("GET /events", hub.ServeWS)

	fmt.Println("API running on http://localhost:9000")
	fmt.Println("Endpoints:")
	fmt.Println("  GET /api/health")
	fmt.Println("  GET /api/devices/{id}/readings")
	fmt.Println("  GET /api/anomalies")
	fmt.Println("  GET /events  (live anomaly stream)")
	fmt.Println()

	go func() {
		<-ctx.Done()
		fmt.Println("\nShutting down...")
	}()

	log.Fatal(http.ListenAndServe(":9000", mux))
}