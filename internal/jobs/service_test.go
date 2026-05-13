package jobs

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestServiceCreateImmediateJobPublishes(t *testing.T) {
	repo := newFakeRepo()
	queue := &fakeQueue{}
	svc := New(repo, queue, nil)

	job, existed, err := svc.CreateJob(context.Background(), CreateJobInput{
		Type:        TypeTelegramSendMessage,
		Payload:     mustJSON(t, map[string]interface{}{"chat_id": 1, "text": "hello"}),
		MaxAttempts: 5,
		Producer:    "test",
	})

	require.NoError(t, err)
	require.False(t, existed)
	require.Equal(t, StatusReady, job.Status)
	require.Len(t, queue.published, 1)
}

func TestServiceIdempotencyReturnsExistingJob(t *testing.T) {
	repo := newFakeRepo()
	queue := &fakeQueue{}
	svc := New(repo, queue, nil)
	key := "external:event-1"

	first, existed, err := svc.CreateJob(context.Background(), CreateJobInput{
		Type:           TypeTelegramSendMessage,
		Payload:        mustJSON(t, map[string]interface{}{"chat_id": 1, "text": "hello"}),
		IdempotencyKey: &key,
	})
	require.NoError(t, err)
	require.False(t, existed)
	second, existed, err := svc.CreateJob(context.Background(), CreateJobInput{
		Type:           TypeTelegramSendMessage,
		Payload:        mustJSON(t, map[string]interface{}{"chat_id": 1, "text": "hello again"}),
		IdempotencyKey: &key,
	})
	require.NoError(t, err)
	require.True(t, existed)
	require.Equal(t, first.ID, second.ID)
	require.Len(t, queue.published, 1)
}

func TestServiceCreateDelayedJobStaysScheduled(t *testing.T) {
	repo := newFakeRepo()
	queue := &fakeQueue{}
	svc := New(repo, queue, nil)
	runAt := time.Now().Add(time.Hour)

	job, _, err := svc.CreateJob(context.Background(), CreateJobInput{
		Type:    TypeReminderSend,
		Payload: mustJSON(t, map[string]interface{}{"chat_id": 1, "text": "later"}),
		RunAt:   runAt,
	})

	require.NoError(t, err)
	require.Equal(t, StatusScheduled, job.Status)
	require.Empty(t, queue.published)
}

func TestBackoff(t *testing.T) {
	require.Equal(t, 10*time.Second, Backoff(1))
	require.Equal(t, 30*time.Second, Backoff(2))
	require.Equal(t, 2*time.Minute, Backoff(3))
	require.Equal(t, 5*time.Minute, Backoff(4))
	require.Equal(t, 15*time.Minute, Backoff(5))
}

func mustJSON(t *testing.T, v interface{}) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

type fakeQueue struct {
	published []Job
	err       error
}

func (q *fakeQueue) Publish(ctx context.Context, job Job) error {
	if q.err != nil {
		return q.err
	}
	q.published = append(q.published, job)
	return nil
}

type fakeRepo struct {
	jobs map[uuid.UUID]*Job
	keys map[string]uuid.UUID
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{jobs: map[uuid.UUID]*Job{}, keys: map[string]uuid.UUID{}}
}

func (r *fakeRepo) CreateJob(ctx context.Context, job *Job) (*Job, bool, error) {
	if job.IdempotencyKey != nil {
		if id, ok := r.keys[*job.IdempotencyKey]; ok {
			return r.jobs[id], true, nil
		}
		r.keys[*job.IdempotencyKey] = job.ID
	}
	cp := *job
	r.jobs[job.ID] = &cp
	return &cp, false, nil
}

func (r *fakeRepo) GetJob(ctx context.Context, id uuid.UUID) (*Job, error) {
	job, ok := r.jobs[id]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *job
	return &cp, nil
}

func (r *fakeRepo) SetStatus(ctx context.Context, id uuid.UUID, status Status) error {
	r.jobs[id].Status = status
	return nil
}
func (r *fakeRepo) MarkScheduled(ctx context.Context, id uuid.UUID, runAt time.Time, lastError *string) error {
	r.jobs[id].Status = StatusScheduled
	r.jobs[id].RunAt = runAt
	r.jobs[id].LastError = lastError
	return nil
}
func (r *fakeRepo) ClaimDueJobs(ctx context.Context, limit int) ([]Job, error) { return nil, nil }
func (r *fakeRepo) MarkProcessing(ctx context.Context, id uuid.UUID) error     { return nil }
func (r *fakeRepo) MarkSucceeded(ctx context.Context, id uuid.UUID) error      { return nil }
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
