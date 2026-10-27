package queue

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

type StatsReader interface {
	Stats(context.Context, string) (QueueStats, error)
}

type StatsHandler struct {
	reader StatsReader
}

func NewStatsHandler(reader StatsReader) *StatsHandler {
	return &StatsHandler{reader: reader}
}

func (h *StatsHandler) Get(w http.ResponseWriter, r *http.Request) {
	queueName := strings.TrimSpace(r.PathValue("queueName"))
	if queueName == "" {
		writeStatsError(w, http.StatusBadRequest, "queue name is required")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	stats, err := h.reader.Stats(ctx, queueName)
	if err != nil {
		writeStatsError(w, http.StatusInternalServerError, "failed to retrieve queue statistics")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(stats)
}

func writeStatsError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}
