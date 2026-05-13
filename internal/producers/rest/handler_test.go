package rest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"task-scheduler/internal/jobs"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestCreateJobEndpoint(t *testing.T) {
	repo := &fakeRepo{}
	service := jobs.New(repo, &fakeQueue{}, nil)
	r := chi.NewRouter()
	NewHandler(service).Register(r)
	body := `{"type":"telegram.send_message","payload":{"chat_id":123,"text":"hello"},"max_attempts":5}`
	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(body))
	rec := httptest.NewRecorder()

	r.ServeHTTP(rec, req)

	require.Equal(t, http.StatusCreated, rec.Code)
	require.Len(t, repo.created, 1)
	require.Equal(t, jobs.TypeTelegramSendMessage, repo.created[0].Type)
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
func (r *fakeRepo) GetJob(ctx context.Context, id uuid.UUID) (*jobs.Job, error) {
	return nil, jobs.ErrNotFound
}
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
