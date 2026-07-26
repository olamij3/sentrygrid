package detect

import (
	"math"
	"sync"
)

type EWMA struct {
	mu        sync.Mutex
	value     float64
	alpha     float64
	threshold float64
	primed    bool
}

func NewEWMA(alpha, threshold float64) *EWMA {
	return &EWMA{alpha: alpha, threshold: threshold}
}

func (e *EWMA) Check(value float64) (isAnomaly bool, deviation float64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.primed {
		e.value = value
		e.primed = true
		return false, 0
	}

	deviation = value - e.value
	e.value = e.alpha*value + (1-e.alpha)*e.value

	return math.Abs(deviation) > e.threshold, deviation
}