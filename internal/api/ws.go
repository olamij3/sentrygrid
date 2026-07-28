package api

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"github.com/olamij3/sentrygrid/internal/pool"
)

type Hub struct {
	mu      sync.Mutex
	clients map[chan []byte]struct{}
}

func NewHub() *Hub {
	return &Hub{
		clients: make(map[chan []byte]struct{}),
	}
}

func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
	// upgrade to websocket
	w.Header().Set("Access-Control-Allow-Origin", "*")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := make(chan []byte, 16)

	h.mu.Lock()
	h.clients[ch] = struct{}{}
	h.mu.Unlock()

	defer func() {
		h.mu.Lock()
		delete(h.clients, ch)
		h.mu.Unlock()
	}()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-ch:
			_, err := w.Write(append([]byte("data: "), append(msg, '\n', '\n')...))
			if err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (h *Hub) Broadcast(ctx context.Context, anomalies <-chan pool.Anomaly) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case a, ok := <-anomalies:
				if !ok {
					return
				}
				data, err := json.Marshal(a)
				if err != nil {
					log.Printf("broadcast marshal error: %v", err)
					continue
				}
				h.mu.Lock()
				for ch := range h.clients {
					select {
					case ch <- data:
					default:
						// client too slow, skip this message
					}
				}
				h.mu.Unlock()
			}
		}
	}()
}