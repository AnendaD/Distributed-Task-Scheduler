package cron

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	_ "task-scheduler/internal/jobs"
	"time"

	"task-scheduler/internal/config"
	"task-scheduler/internal/jobs"

	robfigcron "github.com/robfig/cron/v3"
)

type Producer struct {
	cron    *robfigcron.Cron
	service *jobs.Service
	logger  *slog.Logger
}

func NewProducer(service *jobs.Service, logger *slog.Logger) *Producer {
	return &Producer{
		cron:    robfigcron.New(robfigcron.WithParser(robfigcron.NewParser(robfigcron.Minute | robfigcron.Hour | robfigcron.Dom | robfigcron.Month | robfigcron.Dow | robfigcron.Descriptor))),
		service: service,
		logger:  logger,
	}
}

func (p *Producer) Register(cfg config.CronConfig) error {
	for _, jobCfg := range cfg.Jobs {
		if err := jobs.ValidateType(jobCfg.Type); err != nil {
			return err
		}
		if _, err := p.cron.AddFunc(jobCfg.Schedule, func() {
			p.createJob(context.Background(), jobCfg, time.Now().UTC())
		}); err != nil {
			return err
		}
		p.logger.Info("registered cron job", "name", jobCfg.Name, "schedule", jobCfg.Schedule, "type", jobCfg.Type)
	}
	return nil
}

func (p *Producer) Start() {
	p.cron.Start()
}

func (p *Producer) Stop(ctx context.Context) error {
	stopCtx := p.cron.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-stopCtx.Done():
		return nil
	}
}

func (p *Producer) createJob(ctx context.Context, jobCfg config.CronJobConfig, scheduledAt time.Time) {
	payload, err := json.Marshal(jobCfg.Payload)
	if err != nil {
		p.logger.Error("marshal cron payload failed", "name", jobCfg.Name, "error", err)
		return
	}
	key := fmt.Sprintf("cron:%s:%s", jobCfg.Name, scheduledAt.Format(time.RFC3339))
	if _, _, err := p.service.CreateJob(ctx, jobs.CreateJobInput{
		Type:           jobCfg.Type,
		Payload:        payload,
		RunAt:          scheduledAt,
		MaxAttempts:    jobCfg.MaxAttempts,
		IdempotencyKey: &key,
		Producer:       "cron",
	}); err != nil {
		p.logger.Error("create cron job failed", "name", jobCfg.Name, "error", err)
		return
	}
	p.logger.Info("created cron job", "name", jobCfg.Name, "type", jobCfg.Type, "scheduled_at", scheduledAt)
}
