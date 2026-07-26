package pool

import (
	"context"
	"sync"

	"github.com/olamij3/sentrygrid/internal/detect"
	"github.com/olamij3/sentrygrid/internal/sensor"
)

type Anomaly struct {
	sensor.Reading
	Method string
	Score  float64
}

func Run(ctx context.Context, in <-chan sensor.Reading, numWorkers int, reg *detect.Registry) <-chan Anomaly {
	results := make(chan Anomaly, 64)
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case r, ok := <-in:
					if !ok {
						return
					}
					z, e := reg.For(r.DeviceID)

					if anomalous, score := z.Check(r.Value); anomalous {
						send(ctx, results, Anomaly{Reading: r, Method: "zscore", Score: score})
					}
					if anomalous, dev := e.Check(r.Value); anomalous {
						send(ctx, results, Anomaly{Reading: r, Method: "ewma", Score: dev})
					}
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	return results
}

func send(ctx context.Context, out chan<- Anomaly, a Anomaly) {
	select {
	case out <- a:
	case <-ctx.Done():
	}
}