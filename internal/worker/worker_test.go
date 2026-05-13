package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"task-scheduler/internal/jobs"
	"task-scheduler/internal/queue"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestWorkerSchedulesRetryOnFailure(t *testing.T) {
	job := jobs.Job{
		ID:          uuid.New(),
		Type:        jobs.TypeTelegramProcessUpdate,
		Payload:     []byte(`{"ok":true}`),
		Status:      jobs.StatusReady,
		Attempts:    0,
		MaxAttempts: 2,
	}
	repo := &fakeRepo{job: job}
	q := &fakeQueue{}
	reg := jobs.NewRegistry()
	reg.Register(jobs.TypeTelegramProcessUpdate, jobs.HandleFunc(func(ctx context.Context, job jobs.Job) error {
		return errors.New("boom")
	}))
	svc := New(repo, q, reg, slog.Default(), 1, "test")

	svc.processMessage(context.Background(), queue.Message{ID: "1-0", JobID: job.ID.String(), JobType: job.Type}, 0)

	require.Equal(t, 1, repo.retryAttempts)
	require.True(t, repo.retryRunAt.After(time.Now()))
	require.Equal(t, []string{"1-0"}, q.acked)
}

func TestPaymentSimulatedFailureReturnsError(t *testing.T) {
	reg := jobs.NewRegistry()
	RegisterHandlersWithOptions(reg, &fakeRepo{}, slog.Default(), "", HandlerOptions{})
	payload := mustRawJSON(t, map[string]interface{}{
		"event_id": "evt_123",
		"type":     "payment.succeeded",
		"data": map[string]interface{}{
			"simulate_failure": true,
		},
	})

	err := reg.Handle(context.Background(), jobs.Job{ID: uuid.New(), Type: jobs.TypePaymentProcessSucceeded, Payload: payload})

	require.Error(t, err)
	require.Contains(t, err.Error(), "simulated")
}

type fakeQueue struct {
	acked []string
}

func (q *fakeQueue) CreateConsumerGroup(ctx context.Context) error { return nil }
func (q *fakeQueue) Read(ctx context.Context, consumer string, count int) ([]queue.Message, error) {
	return nil, nil
}
func (q *fakeQueue) RecoverPending(ctx context.Context, consumer string, minIdleMillis int64, count int) ([]queue.Message, error) {
	return nil, nil
}
func (q *fakeQueue) Ack(ctx context.Context, messageID string) error {
	q.acked = append(q.acked, messageID)
	return nil
}

type fakeCreator struct {
	created []jobs.CreateJobInput
}

func (c *fakeCreator) CreateJob(ctx context.Context, in jobs.CreateJobInput) (*jobs.Job, bool, error) {
	c.created = append(c.created, in)
	return &jobs.Job{ID: uuid.New(), Type: in.Type, Payload: in.Payload, Status: jobs.StatusReady}, false, nil
}

func mustRawJSON(t *testing.T, value interface{}) json.RawMessage {
	t.Helper()
	body, err := json.Marshal(value)
	require.NoError(t, err)
	return body
}

type fakeRepo struct {
	job           jobs.Job
	retryAttempts int
	retryRunAt    time.Time
}

func (r *fakeRepo) GetJob(ctx context.Context, id uuid.UUID) (*jobs.Job, error) {
	cp := r.job
	return &cp, nil
}
func (r *fakeRepo) MarkProcessing(ctx context.Context, id uuid.UUID) error { return nil }
func (r *fakeRepo) StartAttempt(ctx context.Context, jobID uuid.UUID, attemptNumber int) (uuid.UUID, error) {
	return uuid.New(), nil
}
func (r *fakeRepo) FinishAttempt(ctx context.Context, attemptID uuid.UUID, status string, errText *string) error {
	return nil
}
func (r *fakeRepo) ScheduleRetry(ctx context.Context, id uuid.UUID, runAt time.Time, attemptNumber int, lastError string) error {
	r.retryAttempts = attemptNumber
	r.retryRunAt = runAt
	return nil
}
func (r *fakeRepo) CreateJob(ctx context.Context, job *jobs.Job) (*jobs.Job, bool, error) {
	return job, false, nil
}
func (r *fakeRepo) SetStatus(ctx context.Context, id uuid.UUID, status jobs.Status) error { return nil }
func (r *fakeRepo) MarkScheduled(ctx context.Context, id uuid.UUID, runAt time.Time, lastError *string) error {
	return nil
}
func (r *fakeRepo) ClaimDueJobs(ctx context.Context, limit int) ([]jobs.Job, error) { return nil, nil }
func (r *fakeRepo) MarkSucceeded(ctx context.Context, id uuid.UUID) error           { return nil }
func (r *fakeRepo) MarkDead(ctx context.Context, id uuid.UUID, attempts int, lastError string) error {
	return nil
}
func (r *fakeRepo) CancelJob(ctx context.Context, id uuid.UUID) error { return nil }
func (r *fakeRepo) CleanupExpiredJobs(ctx context.Context, olderThanDays int) (int64, error) {
	return 0, nil
}
