package payment

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"task-scheduler/internal/jobs"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestPaymentWebhookValidSignatureCreatesJob(t *testing.T) {
	repo := newFakeRepo()
	handler := NewHandler(jobs.New(repo, &fakeQueue{}, nil), "secret", nil)
	body := paymentBody("evt_123", "payment.succeeded")
	rec := postPayment(handler, body, "secret")

	require.Equal(t, http.StatusAccepted, rec.Code)
	require.Len(t, repo.created, 1)
	require.Equal(t, jobs.TypePaymentProcessSucceeded, repo.created[0].Type)
	require.Equal(t, "payment:event:evt_123", *repo.created[0].IdempotencyKey)
	require.Equal(t, 5, repo.created[0].MaxAttempts)
}

func TestPaymentWebhookInvalidSignatureReturnsUnauthorized(t *testing.T) {
	repo := newFakeRepo()
	handler := NewHandler(jobs.New(repo, &fakeQueue{}, nil), "secret", nil)
	rec := postPayment(handler, paymentBody("evt_123", "payment.succeeded"), "wrong")

	require.Equal(t, http.StatusUnauthorized, rec.Code)
	require.Empty(t, repo.created)
}

func TestPaymentWebhookDuplicateEventReturnsExistingJob(t *testing.T) {
	repo := newFakeRepo()
	handler := NewHandler(jobs.New(repo, &fakeQueue{}, nil), "secret", nil)
	body := paymentBody("evt_123", "payment.succeeded")

	first := postPayment(handler, body, "secret")
	second := postPayment(handler, body, "secret")

	require.Equal(t, http.StatusAccepted, first.Code)
	require.Equal(t, http.StatusOK, second.Code)
	require.Len(t, repo.created, 1)
}

func TestPaymentEventTypesMapToJobTypes(t *testing.T) {
	cases := map[string]string{
		"payment.succeeded":          jobs.TypePaymentProcessSucceeded,
		"payment.failed":             jobs.TypePaymentProcessFailed,
		"payment.refund_requested":   jobs.TypePaymentProcessRefund,
		"payment.chargeback_created": jobs.TypePaymentProcessChargeback,
	}
	for eventType, expectedJobType := range cases {
		jobType, _, err := mapEvent(eventType)
		require.NoError(t, err)
		require.Equal(t, expectedJobType, jobType)
	}
}

func postPayment(handler http.Handler, body, secret string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/webhooks/payment", strings.NewReader(body))
	req.Header.Set("X-Payment-Signature", SignHMACSHA256([]byte(body), secret))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func paymentBody(eventID, eventType string) string {
	body, _ := json.Marshal(map[string]interface{}{
		"event_id":   eventID,
		"type":       eventType,
		"created_at": "2026-05-02T12:00:00Z",
		"data": map[string]interface{}{
			"payment_id": "pay_123",
			"order_id":   "order_456",
			"user_id":    "user_789",
			"amount":     1990,
			"currency":   "RUB",
		},
	})
	return string(body)
}

type fakeQueue struct{}

func (q *fakeQueue) Publish(ctx context.Context, job jobs.Job) error { return nil }

type fakeRepo struct {
	created []jobs.Job
	jobs    map[uuid.UUID]*jobs.Job
	keys    map[string]uuid.UUID
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{jobs: map[uuid.UUID]*jobs.Job{}, keys: map[string]uuid.UUID{}}
}

func (r *fakeRepo) CreateJob(ctx context.Context, job *jobs.Job) (*jobs.Job, bool, error) {
	if job.IdempotencyKey != nil {
		if id, ok := r.keys[*job.IdempotencyKey]; ok {
			return r.jobs[id], true, nil
		}
		r.keys[*job.IdempotencyKey] = job.ID
	}
	cp := *job
	r.jobs[job.ID] = &cp
	r.created = append(r.created, cp)
	return &cp, false, nil
}
func (r *fakeRepo) GetJob(ctx context.Context, id uuid.UUID) (*jobs.Job, error) {
	return r.jobs[id], nil
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
