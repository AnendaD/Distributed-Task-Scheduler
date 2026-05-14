package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"task-scheduler/internal/config"
	"task-scheduler/internal/jobs"
	"task-scheduler/internal/logger"
	"task-scheduler/internal/queue/redisstream"
	"task-scheduler/internal/storage/postgres"
	workersvc "task-scheduler/internal/worker"
)

func main() {
	cfg := config.Load()
	log := logger.New(cfg.LogLevel)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := postgres.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("connect postgres failed", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	redisClient := redisstream.NewClient(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	defer redisClient.Close()
	readyQueue := redisstream.New(redisClient, redisstream.DefaultStream, redisstream.DefaultGroup)

	repo := postgres.NewRepository(pool)
	jobService := jobs.New(repo, readyQueue, log)
	registry := jobs.NewRegistry()
	workersvc.RegisterHandlersWithOptions(registry, repo, log, cfg.TelegramBotToken, workersvc.HandlerOptions{
		JobCreator: jobService,
	})
	go func() {
		http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		http.ListenAndServe(":8080", nil)
	}()
	worker := workersvc.New(repo, readyQueue, registry, log, cfg.WorkerConcurrency, "")
	if err := worker.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("worker stopped with error", "error", err)
		os.Exit(1)
	}
}
