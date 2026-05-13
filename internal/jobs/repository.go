package jobs

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type Repository interface {
	CreateJob(ctx context.Context, job *Job) (*Job, bool, error)
	GetJob(ctx context.Context, id uuid.UUID) (*Job, error)
	ClaimDueJobs(ctx context.Context, limit int) ([]Job, error)
	SetStatus(ctx context.Context, id uuid.UUID, status Status) error
	MarkScheduled(ctx context.Context, id uuid.UUID, runAt time.Time, lastError *string) error
	MarkDead(ctx context.Context, id uuid.UUID, attempts int, lastError string) error
	MarkProcessing(ctx context.Context, id uuid.UUID) error
	MarkSucceeded(ctx context.Context, id uuid.UUID) error
	CancelJob(ctx context.Context, id uuid.UUID) error
	ScheduleRetry(ctx context.Context, id uuid.UUID, runAt time.Time, attemptNumber int, lastError string) error
	CleanupExpiredJobs(ctx context.Context, olderThanDays int) (int64, error)
	StartAttempt(ctx context.Context, jobID uuid.UUID, attempt int) (uuid.UUID, error)
	FinishAttempt(ctx context.Context, attemptID uuid.UUID, status string, lastError *string) error
}

type ReadyQueue interface {
	Publish(ctx context.Context, job Job) error
}
