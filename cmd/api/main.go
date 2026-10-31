package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nooby6/forgequeue/internal/config"
	"github.com/nooby6/forgequeue/internal/database"
	"github.com/nooby6/forgequeue/internal/health"
	"github.com/nooby6/forgequeue/internal/httpserver"
	"github.com/nooby6/forgequeue/internal/job"
	"github.com/nooby6/forgequeue/internal/queue"
	"github.com/nooby6/forgequeue/internal/worker"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := database.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("database_initialization_failed", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	repo := job.NewPostgresRepository(db)
	queueRuntime := queue.NewPostgresQueue(db)
	server := httpserver.New(
		cfg.Addr,
		&health.Handler{DB: db},
		job.NewHandler(job.NewService(repo)),
		queue.NewStatsHandler(queueRuntime),
		logger,
	)

	registry := worker.NewRegistry()
	if err := registry.Register("noop", func(ctx context.Context, payload json.RawMessage) error {
		logger.Info("noop_job_processed", "payload", string(payload))
		return nil
	}); err != nil {
		logger.Error("worker_registration_failed", "error", err)
		os.Exit(1)
	}

	workerID, _ := os.Hostname()
	if workerID == "" {
		workerID = "forgequeue-worker"
	}

	jobWorker := worker.New(queueRuntime, registry, logger, worker.Config{
		QueueName:       cfg.WorkerQueue,
		Concurrency:     cfg.WorkerConcurrency,
		PollInterval:    cfg.WorkerPollInterval,
		Lease:           cfg.WorkerLease,
		RetryBackoff:    cfg.WorkerRetryBackoff,
		ShutdownTimeout: cfg.WorkerShutdownTimeout,
		WorkerID:        workerID,
	})

	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		logger.Info("worker_started", "worker_id", workerID, "queue", cfg.WorkerQueue, "concurrency", cfg.WorkerConcurrency)
		jobWorker.Run(ctx)
		logger.Info("worker_stopped", "worker_id", workerID)
	}()

	go func() {
		logger.Info("server_started", "addr", cfg.Addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server_failed", "error", err)
			stop()
		}
	}()

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("server_shutdown_failed", "error", err)
	}

	select {
	case <-workerDone:
	case <-time.After(cfg.WorkerShutdownTimeout + time.Second):
		logger.Error("worker_shutdown_timeout", "timeout", cfg.WorkerShutdownTimeout)
	}
}
