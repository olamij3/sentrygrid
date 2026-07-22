package sensor

import (
	"context"
	"math/rand"
	"time"
)

func RunDevice(ctx context.Context, id string, p Profile, out chan<- Reading) {
	ticker := time.NewTicker(p.TickInterval)
	defer ticker.Stop()

	value := p.BaseValue
	driftActive := false

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if rand.Float64() < p.DropoutChance {
				continue
			}

			noise := rand.NormFloat64() * p.NoiseStdDev
			reading := value + noise

			if rand.Float64() < p.AnomalyChance {
				if rand.Float64() < 0.5 {
					reading += p.BaseValue * (0.3 + rand.Float64()*0.4)
				} else {
					driftActive = true
				}
			}

			if driftActive {
				value += p.DriftPerTick
				reading = value + noise
			}

			select {
			case out <- Reading{DeviceID: id, Timestamp: time.Now(), Value: reading}:
			case <-ctx.Done():
				return
			}
		}
	}
}

func StartDevices(ctx context.Context, profiles map[string]Profile) <-chan Reading {
	out := make(chan Reading, 256)
	done := make(chan struct{})

	for id, p := range profiles {
		go func(id string, p Profile) {
			RunDevice(ctx, id, p, out)
			done <- struct{}{}
		}(id, p)
	}

	go func() {
		for range profiles {
			<-done
		}
		close(out)
	}()

	return out
}
