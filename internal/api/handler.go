package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/olamij3/sentrygrid/internal/store"
)

type API struct {
	store *store.Store
}

func New(s *store.Store) *API {
	return &API{store: s}
}

func (a *API) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("GET /", http.FileServer(http.Dir("web")))
	mux.HandleFunc("GET /api/health", a.health)
	mux.HandleFunc("GET /api/devices/{id}/readings", a.deviceReadings)
	mux.HandleFunc("GET /api/anomalies", a.recentAnomalies)
	return mux
}

func (a *API) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *API) deviceReadings(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	limit := 100
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil {
			limit = n
		}
	}
	readings, err := a.store.RecentReadings(id, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, readings)
}

func (a *API) recentAnomalies(w http.ResponseWriter, r *http.Request) {
	anomalies, err := a.store.RecentAnomalies(50)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, anomalies)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
