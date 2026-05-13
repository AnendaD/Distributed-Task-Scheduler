package payment

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"task-scheduler/internal/jobs"
	"time"
)

const maxBodyBytes = 1 << 20

type Handler struct {
	service *jobs.Service
	secret  string
	logger  *slog.Logger
}

func NewHandler(service *jobs.Service, secret string, logger *slog.Logger) *Handler {
	return &Handler{service: service, secret: secret, logger: logger}
}

type Event struct {
	EventID   string          `json:"event_id"`
	Type      string          `json:"type"`
	CreatedAt time.Time       `json:"created_at"`
	Data      json.RawMessage `json:"data"`
}

type response struct {
	ID        string `json:"id,omitempty"`
	Status    string `json:"status"`
	Duplicate bool   `json:"duplicate,omitempty"`
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "read body failed")
		return
	}
	if !VerifyHexHMACSHA256(body, h.secret, r.Header.Get("X-Payment-Signature")) {
		h.writeError(w, http.StatusUnauthorized, "invalid payment signature")
		return
	}
	var event Event
	if err := json.Unmarshal(body, &event); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid payment event")
		return
	}
	if err := validateEvent(event); err != nil {
		h.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	jobType, maxAttempts, err := mapEvent(event.Type)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	key := fmt.Sprintf("payment:event:%s", event.EventID)
	job, existed, err := h.service.CreateJob(r.Context(), jobs.CreateJobInput{
		Type:           jobType,
		Payload:        json.RawMessage(body),
		MaxAttempts:    maxAttempts,
		IdempotencyKey: &key,
		Producer:       "payment",
	})
	if err != nil {
		if h.logger != nil {
			h.logger.Error("create payment job failed", "producer", "payment", "event", event.Type, "event_id", event.EventID, "error", err)
		}
		h.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	statusCode := http.StatusAccepted
	if existed {
		statusCode = http.StatusOK
	}
	if h.logger != nil {
		h.logger.Info("payment webhook created job", "producer", "payment", "event", event.Type, "event_id", event.EventID, "job_id", job.ID.String(), "duplicate", existed)
	}
	writeJSON(w, statusCode, response{ID: job.ID.String(), Status: string(job.Status), Duplicate: existed})
}

func mapEvent(eventType string) (string, int, error) {
	switch eventType {
	case "payment.succeeded":
		return jobs.TypePaymentProcessSucceeded, 5, nil
	case "payment.failed":
		return jobs.TypePaymentProcessFailed, 3, nil
	case "payment.refund_requested":
		return jobs.TypePaymentProcessRefund, 5, nil
	case "payment.chargeback_created":
		return jobs.TypePaymentProcessChargeback, 5, nil
	default:
		return "", 0, fmt.Errorf("unsupported payment event type %q", eventType)
	}
}

func validateEvent(event Event) error {
	if event.EventID == "" {
		return errors.New("missing event_id")
	}
	if event.Type == "" {
		return errors.New("missing type")
	}
	if event.CreatedAt.IsZero() {
		return errors.New("missing created_at")
	}
	if len(event.Data) == 0 || !json.Valid(event.Data) {
		return errors.New("missing data")
	}
	return nil
}

func SignHMACSHA256(body []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func VerifyHexHMACSHA256(body []byte, secret, signature string) bool {
	if secret == "" || signature == "" {
		return false
	}
	expected := SignHMACSHA256(body, secret)
	return hmac.Equal([]byte(expected), []byte(strings.ToLower(signature)))
}

func (h *Handler) writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, value interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
