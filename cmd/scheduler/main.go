package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"task-scheduler/internal/config"
	"task-scheduler/internal/jobs"
	"task-scheduler/internal/logger"
	cronproducer "task-scheduler/internal/producers/cron"
	"task-scheduler/internal/queue/redisstream"
	schedulersvc "task-scheduler/internal/scheduler"
	"task-scheduler/internal/storage/postgres"
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
	if err := readyQueue.CreateConsumerGroup(ctx); err != nil {
		log.Error("create redis consumer group failed", "error", err)
		os.Exit(1)
	}

	repo := postgres.NewRepository(pool)
	jobService := jobs.New(repo, readyQueue, log)
	cronCfg, err := config.LoadCronConfig(cfg.CronConfigPath)
	if err != nil {
		log.Error("load cron config failed", "path", cfg.CronConfigPath, "error", err)
		os.Exit(1)
	}
	cronProducer := cronproducer.NewProducer(jobService, log)
	if err := cronProducer.Register(cronCfg); err != nil {
		log.Error("register cron jobs failed", "error", err)
		os.Exit(1)
	}
	cronProducer.Start()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = cronProducer.Stop(shutdownCtx)
	}()
	go func() {
		http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		http.ListenAndServe(":8080", nil)
	}()
	scheduler := schedulersvc.New(repo, readyQueue, log, cfg.SchedulerInterval, cfg.SchedulerBatchSize)
	if err := scheduler.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("scheduler stopped with error", "error", err)
		os.Exit(1)
	}
}
