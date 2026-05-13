package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"task-scheduler/internal/jobs"
	"task-scheduler/internal/queue"
	"time"

	"github.com/google/uuid"
)

type Queue interface {
	CreateConsumerGroup(ctx context.Context) error
	Read(ctx context.Context, consumer string, count int) ([]queue.Message, error)
	RecoverPending(ctx context.Context, consumer string, minIdleMillis int64, count int) ([]queue.Message, error)
	Ack(ctx context.Context, messageID string) error
}

type Service struct {
	repo        jobs.Repository
	queue       Queue
	registry    *jobs.Registry
	logger      *slog.Logger
	concurrency int
	consumerID  string
}

func New(repo jobs.Repository, queue Queue, registry *jobs.Registry, logger *slog.Logger, concurrency int, consumerID string) *Service {
	if concurrency <= 0 {
		concurrency = 5
	}
	if consumerID == "" {
		consumerID = "worker-" + uuid.NewString()
	}
	return &Service{repo: repo, queue: queue, registry: registry, logger: logger, concurrency: concurrency, consumerID: consumerID}
}

func (s *Service) Run(ctx context.Context) error {
	if err := s.queue.CreateConsumerGroup(ctx); err != nil {
		return err
	}
	s.logger.Info("worker pool started", "concurrency", s.concurrency, "consumer_id", s.consumerID)
	var wg sync.WaitGroup
	for i := 0; i < s.concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			s.loop(ctx, idx)
		}(i)
	}
	<-ctx.Done()
	wg.Wait()
	s.logger.Info("worker pool stopped")
	return ctx.Err()
}

func (s *Service) loop(ctx context.Context, idx int) {
	consumer := s.consumerID + "-" + uuid.NewString()[:8]
	for ctx.Err() == nil {
		pending, err := s.queue.RecoverPending(ctx, consumer, int64((30*time.Second)/time.Millisecond), 10)
		if err != nil {
			s.logger.Error("read queue failed", "error", err)
		}
		for _, msg := range pending {
			s.processMessage(ctx, msg, idx)
		}
		messages, err := s.queue.Read(ctx, consumer, 1)
		if err != nil {
			if !errors.Is(err, context.Canceled) && ctx.Err() == nil {
				s.logger.Error("read queue failed", "error", err)
			}
			continue
		}
		for _, msg := range messages {
			s.processMessage(ctx, msg, idx)
		}
	}
}

func (s *Service) processMessage(ctx context.Context, msg queue.Message, idx int) {
	jobID, err := uuid.Parse(msg.JobID)
	if err != nil {
		s.logger.Error("invalid queue job id", "error", err)
		_ = s.queue.Ack(ctx, msg.ID)
		return
	}
	job, err := s.repo.GetJob(ctx, jobID)
	if err != nil {
		s.logger.Error("load job failed", "error", err)
		return
	}
	if jobs.IsTerminal(job.Status) {
		_ = s.queue.Ack(ctx, msg.ID)
		return
	}
	if job.Status != jobs.StatusReady {
		s.logger.Warn("job is not ready, acking stale message", "job_id", job.ID.String(), "status", job.Status)
		_ = s.queue.Ack(ctx, msg.ID)
		return
	}
	if err := s.repo.MarkProcessing(ctx, job.ID); err != nil {
		s.logger.Error("mark processing failed", "error", err)
		return
	}
	attemptNumber := job.Attempts + 1
	attemptID, err := s.repo.StartAttempt(ctx, job.ID, attemptNumber)
	if err != nil {
		s.logger.Error("start attempt failed", "error", err)
		return
	}
	start := time.Now()
	err = s.registry.Handle(ctx, *job)
	duration := time.Since(start)
	if err == nil {
		if finishErr := s.repo.MarkSucceeded(ctx, job.ID); finishErr != nil {
			s.logger.Error("mark succeeded failed", "error", err)
			return
		}
		_ = s.repo.FinishAttempt(ctx, attemptID, string(jobs.StatusSucceeded), nil)
		if ackErr := s.queue.Ack(ctx, msg.ID); ackErr != nil {
			s.logger.Error("ack succeeded job failed", "error", err)
			return
		}
		s.logger.Info("job succeeded", "worker", idx, "job_id", job.ID.String(), "type", job.Type, "duration", duration.String())
		return
	}
	errText := err.Error()
	_ = s.repo.FinishAttempt(ctx, attemptID, string(jobs.StatusFailed), &errText)
	if attemptNumber < job.MaxAttempts {
		runAt := time.Now().UTC().Add(jobs.Backoff(attemptNumber))
		if retryErr := s.repo.ScheduleRetry(ctx, job.ID, runAt, attemptNumber, errText); retryErr != nil {
			s.logger.Error("schedule retry failed", "error", err)
			return
		}
		if ackErr := s.queue.Ack(ctx, msg.ID); ackErr != nil {
			s.logger.Error("ack retried job failed", "error", err)
			return
		}
		s.logger.Warn("job scheduled for retry", "job_id", job.ID.String(), "type", job.Type, "attempt", attemptNumber, "run_at", runAt, "error", err)
		return
	}
	if deadErr := s.repo.MarkDead(ctx, job.ID, attemptNumber, errText); deadErr != nil {
		s.logger.Error("mark dead failed", "error", err)
		return
	}
	if ackErr := s.queue.Ack(ctx, msg.ID); ackErr != nil {
		s.logger.Error("ack dead failed", "error", err)
		return
	}
	s.logger.Error("job moved to dead-letter state", "job_id", job.ID.String(), "type", job.Type, "attempt", attemptNumber, "error", err)
}
