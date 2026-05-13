package cron

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"task-scheduler/internal/config"
	"task-scheduler/internal/jobs"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestCronProducerCreatesJobWithIdempotencyKey(t *testing.T) {
	repo := &fakeRepo{}
	service := jobs.New(repo, &fakeQueue{}, nil)
	producer := NewProducer(service, slog.Default())
	scheduledAt := time.Date(2026, 5, 2, 9, 0, 0, 0, time.UTC)

	producer.createJob(context.Background(), config.CronJobConfig{
		Name:        "daily-test",
		Type:        jobs.TypeTelegramSendMessage,
		Payload:     map[string]interface{}{"chat_id": 1, "text": "hello"},
		MaxAttempts: 5,
	}, scheduledAt)

	require.Len(t, repo.created, 1)
	require.NotNil(t, repo.created[0].IdempotencyKey)
	require.Equal(t, "cron:daily-test:2026-05-02T09:00:00Z", *repo.created[0].IdempotencyKey)
}

type fakeQueue struct{}

func (q *fakeQueue) Publish(ctx context.Context, job jobs.Job) error { return nil }

type fakeRepo struct {
	created []jobs.Job
}

func (r *fakeRepo) CreateJob(ctx context.Context, job *jobs.Job) (*jobs.Job, bool, error) {
	r.created = append(r.created, *job)
	return job, false, nil
}
func (r *fakeRepo) GetJob(ctx context.Context, id uuid.UUID) (*jobs.Job, error) { return nil, nil }
func (r *fakeRepo) SetStatus(ctx context.Context, id uuid.UUID, status jobs.Status) error {
	return nil
}
func (r *fakeRepo) MarkScheduled(ctx context.Context, id uuid.UUID, runAt time.Time, lastError *string) error {
	return nil
}
func (r *fakeRepo) ClaimDueJobs(ctx context.Context, limit int) ([]jobs.Job, error) { return nil, nil }
func (r *fakeRepo) MarkProcessing(ctx context.Context, id uuid.UUID) error          { return nil }
func (r *fakeRepo) MarkSucceeded(ctx context.Context, id uuid.UUID) error           { return nil }
func (r *fakeRepo) ScheduleRetry(ctx context.Context, id uuid.UUID, runAt time.Time, attemptNumber int, lastError string) error {
	return nil
}
func (r *fakeRepo) MarkDead(ctx context.Context, id uuid.UUID, attempts int, lastError string) error {
	return nil
}
func (r *fakeRepo) CancelJob(ctx context.Context, id uuid.UUID) error { return nil }
func (r *fakeRepo) StartAttempt(ctx context.Context, jobID uuid.UUID, attemptNumber int) (uuid.UUID, error) {
	return uuid.New(), nil
}
func (r *fakeRepo) FinishAttempt(ctx context.Context, attemptID uuid.UUID, status string, errText *string) error {
	return nil
}
func (r *fakeRepo) CleanupExpiredJobs(ctx context.Context, olderThanDays int) (int64, error) {
	return 0, nil
}
