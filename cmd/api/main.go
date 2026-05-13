package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"task-scheduler/internal/config"
	httpserver "task-scheduler/internal/http"
	"task-scheduler/internal/jobs"
	_ "task-scheduler/internal/jobs"
	"task-scheduler/internal/logger"
	"task-scheduler/internal/producers/payment"
	"task-scheduler/internal/producers/telegram"
	"task-scheduler/internal/queue/redisstream"
	"task-scheduler/internal/storage/postgres"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func main() {
	cfg := config.Load()
	log := logger.New(cfg.LogLevel)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := postgres.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("failed to connect postgres", "error", err)
		os.Exit(1)
	}

	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Error("failed to ping postgres", "error", err)
		os.Exit(1)
	}

	repo := postgres.NewRepository(pool)
	redisClient := redisstream.NewClient(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	defer redisClient.Close()
	redisQueue := redisstream.New(redisClient, redisstream.DefaultStream, redisstream.DefaultGroup)

	if err := redisQueue.CreateConsumerGroup(ctx); err != nil {
		log.Error("failed to create consumers group", "error", err)
		os.Exit(1)
	}

	jobService := jobs.New(repo, redisQueue, log)
	router := NewRouter(log, jobService, cfg.TelegramWebhookSecret, cfg.PaymentWebhookSecret)
	server := httpserver.New(cfg.HTTPAddr, router, log)

	if err := server.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("api stoped with error", "error", err)
		os.Exit(1)
	}
}

func NewRouter(log *slog.Logger, jobService *jobs.Service, telegarmSecret, paymentSecret string) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)

	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	r.Post("/webhooks/telegram", telegram.NewHandler(jobService, telegarmSecret).ServeHTTP)
	r.Post("/webhooks/payment", payment.NewHandler(jobService, paymentSecret, log).ServeHTTP)
	log.Info("router initalized")
	return r
}
