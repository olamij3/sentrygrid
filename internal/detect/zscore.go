package detect

import (
	"math"
	"sync"
)

type ZScore struct {
	mu        sync.Mutex
	window    []float64
	maxSize   int
	threshold float64
}

func NewZScore(maxSize int, threshold float64) *ZScore {
	return &ZScore{maxSize: maxSize, threshold: threshold}
}

func (z *ZScore) Check(value float64) (isAnomaly bool, score float64) {
	z.mu.Lock()
	defer z.mu.Unlock()

	if len(z.window) < 5 {
		z.window = append(z.window, value)
		return false, 0
	}

	mean, std := meanStdDev(z.window)
	if std == 0 {
		std = 1e-6
	}
	score = (value - mean) / std

	z.window = append(z.window, value)
	if len(z.window) > z.maxSize {
		z.window = z.window[1:]
	}

	return math.Abs(score) > z.threshold, score
}

func meanStdDev(xs []float64) (mean, std float64) {
	for _, x := range xs {
		mean += x
	}
	mean /= float64(len(xs))

	var sumSq float64
	for _, x := range xs {
		d := x - mean
		sumSq += d * d
	}
	std = math.Sqrt(sumSq / float64(len(xs)))
	return
}