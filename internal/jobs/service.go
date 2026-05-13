package jobs

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

type Service struct {
	repo   Repository
	queue  ReadyQueue
	logger *slog.Logger
	now    func() time.Time
}

func New(repo Repository, queue ReadyQueue, logger *slog.Logger) *Service {
	return &Service{
		repo:   repo,
		queue:  queue,
		logger: logger,
		now:    time.Now,
	}
}

func (s *Service) CreateJob(ctx context.Context, in CreateJobInput) (*Job, bool, error) {
	if err := ValidateType(in.Type); err != nil {
		return nil, false, ErrInvalidJobType
	}
	if err := ValidatePayload(in.Type, in.Payload); err != nil {
		return nil, false, ErrInvalidPayload
	}

	now := s.now().UTC()
	runAt := in.RunAt
	if runAt.IsZero() {
		runAt = now
	}
	maxAttempts := in.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	status := StatusScheduled
	if !runAt.After(now) {
		status = StatusReady
	}

	job := &Job{
		ID:             uuid.New(),
		Type:           in.Type,
		Payload:        in.Payload,
		RunAt:          runAt,
		MaxAttempts:    maxAttempts,
		IdempotencyKey: in.IdempotencyKey,
		Status:         status,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	created, existed, err := s.repo.CreateJob(ctx, job)
	if err != nil {
		return nil, false, err
	}
	if existed {
		return created, true, nil
	}

	if created.Status == StatusReady {
		if err := s.queue.Publish(ctx, *created); err != nil {
			msg := err.Error()
			_ = s.repo.MarkScheduled(ctx, created.ID, job.RunAt, &msg)
			created.Status = StatusScheduled
			created.LastError = &msg
		}
		return created, false, err
	}
	return created, false, nil
}

func (s *Service) GetJob(ctx context.Context, id uuid.UUID) (*Job, error) {
	return s.repo.GetJob(ctx, id)
}

func (s *Service) CancelJob(ctx context.Context, id uuid.UUID) error {
	return s.repo.CancelJob(ctx, id)
}

func (s *Service) RetryJob(ctx context.Context, id uuid.UUID) (*Job, error) {
	job, err := s.repo.GetJob(ctx, id)

	if err != nil {
		return nil, err
	}
	if job.Status != StatusCancelled && job.Status != StatusDead {
		return nil, ErrInvalidState
	}
	if err := s.repo.MarkScheduled(ctx, id, s.now().UTC(), nil); err != nil {
		return nil, err
	}
	return job, nil
}
