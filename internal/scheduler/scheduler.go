package scheduler

import (
	"context"
	"log/slog"
	"task-scheduler/internal/jobs"
	"time"
)

type Service struct {
	repo     jobs.Repository
	queue    jobs.ReadyQueue
	logger   *slog.Logger
	interval time.Duration
	batch    int
}

func New(repo jobs.Repository, queue jobs.ReadyQueue, logger *slog.Logger, interval time.Duration, batch int) *Service {
	if batch <= 0 {
		batch = 100
	}
	if interval <= 0 {
		interval = time.Second
	}
	return &Service{repo, queue, logger, interval, batch}
}

func (s *Service) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	s.logger.Info("scheduler started", "interval", s.interval, "batch size", s.batch)
	for {
		if err := s.tick(ctx); err != nil {
			s.logger.Error("scheduler tick failed", "error", err)
		}
		select {
		case <-ctx.Done():
			s.logger.Info("scheduler stopped")
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Service) Tick(ctx context.Context) error {
	return s.tick(ctx)
}

func (s *Service) tick(ctx context.Context) error {
	claimed, err := s.repo.ClaimDueJobs(ctx, s.batch)

	if err != nil {
		return err
	}

	if len(claimed) == 0 {
		return nil
	}

	for _, job := range claimed {
		if err := s.queue.Publish(ctx, job); err != nil {
			msg := err.Error()
			_ = s.repo.MarkScheduled(ctx, job.ID, job.RunAt, &msg)
			s.logger.Error("failed to publish due job", "job_id", job.ID.String(), "type", job.Type, "error", err)
			continue
		}
		s.logger.Debug("published due job", "job_id", job.ID.String(), "type", job.Type)
	}
	return nil
}
