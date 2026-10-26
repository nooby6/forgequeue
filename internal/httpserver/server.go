package httpserver

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/nooby6/forgequeue/internal/health"
	"github.com/nooby6/forgequeue/internal/job"
	"github.com/nooby6/forgequeue/internal/queue"
)

func New(addr string, hh *health.Handler, jh *job.Handler, qh *queue.StatsHandler, l *slog.Logger) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", hh.Live)
	mux.HandleFunc("GET /health/ready", hh.Ready)
	mux.HandleFunc("POST /jobs", jh.Create)
	mux.HandleFunc("GET /jobs/{id}", jh.Get)
	mux.HandleFunc("GET /queues/{queueName}/stats", qh.Get)
	return &http.Server{Addr: addr, Handler: loggingMiddleware(l, mux), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
}

func loggingMiddleware(l *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		l.Info("http_request", "method", r.Method, "path", r.URL.Path, "duration_ms", time.Since(start).Milliseconds())
	})
}
