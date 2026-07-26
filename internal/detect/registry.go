package detect

import "sync"

type Registry struct {
	mu     sync.Mutex
	zscore map[string]*ZScore
	ewma   map[string]*EWMA
}

func NewRegistry() *Registry {
	return &Registry{
		zscore: make(map[string]*ZScore),
		ewma:   make(map[string]*EWMA),
	}
}

func (r *Registry) For(deviceID string) (*ZScore, *EWMA) {
	r.mu.Lock()
	defer r.mu.Unlock()

	z, ok := r.zscore[deviceID]
	if !ok {
		z = NewZScore(30, 3.0)
		r.zscore[deviceID] = z
	}
	e, ok := r.ewma[deviceID]
	if !ok {
		e = NewEWMA(0.3, 4.0)
		r.ewma[deviceID] = e
	}
	return z, e
}