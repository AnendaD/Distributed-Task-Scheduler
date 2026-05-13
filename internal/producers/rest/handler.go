package rest

import (
	"encoding/json"
	"errors"
	"net/http"
	"task-scheduler/internal/jobs"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

type Handler struct {
	service *jobs.Service
}

func NewHandler(service *jobs.Service) *Handler {
	return &Handler{service: service}
}

func (h *Handler) Register(r chi.Router) {
	r.Post("/jobs", h.createJob)
	r.Get("/jobs/{id}", h.getJob)
	r.Post("/jobs/{id}/cancel", h.cancelJob)
	r.Post("/jobs/{id}/retry", h.retryJob)
}

type createJobRequest struct {
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	RunAt          *time.Time      `json:"run_at"`
	MaxAttempts    int             `json:"max_attempts"`
	IdempotencyKey *string         `json:"idempotency_key"`
}

func (h *Handler) createJob(w http.ResponseWriter, r *http.Request) {
	var req createJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	var runAt time.Time
	if req.RunAt != nil {
		runAt = *req.RunAt
	}
	job, existed, err := h.service.CreateJob(r.Context(), jobs.CreateJobInput{
		Type:           req.Type,
		Payload:        req.Payload,
		RunAt:          runAt,
		MaxAttempts:    req.MaxAttempts,
		IdempotencyKey: req.IdempotencyKey,
		Producer:       "rest",
	})

	if err != nil {
		writeDomainError(w, err)
	}
	status := http.StatusCreated
	if existed {
		status = http.StatusOK
	}
	writeJSON(w, status, job)
}

func (h *Handler) getJob(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	job, err := h.service.GetJob(r.Context(), id)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (h *Handler) cancelJob(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	if err := h.service.CancelJob(r.Context(), id); err != nil {
		writeDomainError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) retryJob(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	job, err := h.service.RetryJob(r.Context(), id)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func parseID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return uuid.Nil, false
	}
	return id, true
}

func writeDomainError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, jobs.ErrInvalidJobType), errors.Is(err, jobs.ErrInvalidPayload), errors.Is(err, jobs.ErrInvalidState):
		writeError(w, http.StatusBadRequest, err)
	case errors.Is(err, jobs.ErrNotFound):
		writeError(w, http.StatusNotFound, err)
	default:
		writeError(w, http.StatusInternalServerError, err)
	}
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func writeJSON(w http.ResponseWriter, status int, value interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
