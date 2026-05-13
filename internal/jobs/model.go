package jobs

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Status string

const (
	StatusProcessing Status = "processing"
	StatusReady      Status = "ready"
	StatusSucceeded  Status = "succeeded"
	StatusScheduled  Status = "scheduled"
	StatusDead       Status = "dead"
	StatusCancelled  Status = "cancelled"
	StatusFailed     Status = "failed"
)

const (
	TypeTelegramProcessUpdate = "telegram.process_update"
	TypeTelegramSendMessage   = "telegram.send_message"
	TypeReminderSend          = "reminder.send"
	TypeCleanupExpiredJobs    = "system.cleanup_expired_jobs"

	TypePaymentProcessSucceeded  = "payment.process_succeeded"
	TypePaymentProcessFailed     = "payment.process_failed"
	TypePaymentProcessRefund     = "payment.process_refund"
	TypePaymentProcessChargeback = "payment.process_chargeback"
)

var validTypes = map[string]struct{}{
	TypeReminderSend:          {},
	TypeTelegramSendMessage:   {},
	TypeTelegramProcessUpdate: {},
	TypeCleanupExpiredJobs:    {},

	TypePaymentProcessSucceeded:  {},
	TypePaymentProcessFailed:     {},
	TypePaymentProcessRefund:     {},
	TypePaymentProcessChargeback: {},
}

var (
	ErrInvalidJobType = errors.New("invalid job type")
	ErrInvalidPayload = errors.New("invalid job payload")
	ErrNotFound       = errors.New("job not found")
	ErrInvalidState   = errors.New("invalid job state")
)

type CreateJobInput struct {
	Type           string
	Payload        json.RawMessage
	RunAt          time.Time
	IdempotencyKey *string
	MaxAttempts    int
	Producer       string
}

type Attempt struct {
	ID            uuid.UUID  `json:"id"`
	JobID         uuid.UUID  `json:"job_id"`
	AttemptNumber int        `json:"attempt_number"`
	Status        string     `json:"status"`
	Error         *string    `json:"error,omitempty"`
	StarteddAt    time.Time  `json:"started_at"`
	FinishedAt    *time.Time `json:"finished_at,omitempty"`
}

type Job struct {
	ID             uuid.UUID       `json:"id"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	RunAt          time.Time       `json:"run_at"`
	Attempts       int             `json:"attempts"`
	MaxAttempts    int             `json:"max_attempts"`
	Status         Status          `json:"status"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
	LastError      *string         `json:"last_error,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	FinishedAt     *time.Time      `json:"finished_at,omitempty"`
}

func ValidateType(jobType string) error {
	if _, ok := validTypes[jobType]; !ok {
		return fmt.Errorf("%w: %s", ErrInvalidJobType, jobType)
	}
	return nil
}

func ValidatePayload(jobtype string, payload json.RawMessage) error {
	if len(payload) == 0 || !json.Valid(payload) {
		return fmt.Errorf("%w", ErrInvalidPayload)
	}
	var base map[string]interface{}
	if err := json.Unmarshal(payload, &base); err != nil {
		return fmt.Errorf("%w", ErrInvalidPayload)
	}
	switch jobtype {
	case TypeReminderSend, TypeTelegramSendMessage:
		if _, ok := base["chat_id"]; !ok {
			return fmt.Errorf("%w: missing chat_id", ErrInvalidPayload)
		}
		text, ok := base["text"].(string)
		if !ok || text == "" {
			return fmt.Errorf("%w: missing text", ErrInvalidPayload)
		}
	case TypeCleanupExpiredJobs:
		if _, ok := base["older_than_days"]; !ok {
			return fmt.Errorf("%w: missing older_than_days", ErrInvalidPayload)
		}
	}
	return nil
}

func Backoff(attempt int) time.Duration {
	switch attempt {
	case 1:
		return 10 * time.Second
	case 2:
		return 30 * time.Second
	case 3:
		return 2 * time.Minute
	case 4:
		return 5 * time.Minute
	default:
		return 15 * time.Minute
	}
}

func IsTerminal(status Status) bool {
	return status == StatusSucceeded || status == StatusDead || status == StatusCancelled
}
