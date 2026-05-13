package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"task-scheduler/internal/jobs"
	"time"
)

type TelegramMessagePayload struct {
	ChatID interface{} `json:"chat_id"`
	Text   string      `json:"text"`
}

type CleanupPayload struct {
	OlderThanDays int `json:"older_than_days"`
}

type JobCreator interface {
	CreateJob(ctx context.Context, in jobs.CreateJobInput) (*jobs.Job, bool, error)
}

type HandlerOptions struct {
	JobCreator JobCreator
}

type TelegramSender struct {
	logger   *slog.Logger
	botToken string
	client   *http.Client
}

func RegisterHandlersWithOptions(reg *jobs.Registry, repo jobs.Repository, logger *slog.Logger, botToken string, opts HandlerOptions) {
	telegram := &TelegramSender{logger, botToken, &http.Client{Timeout: 10 * time.Second}}
	reg.Register(jobs.TypeTelegramSendMessage, telegram)
	reg.Register(jobs.TypeReminderSend, telegram)
	reg.Register(jobs.TypeTelegramProcessUpdate, jobs.HandleFunc(func(ctx context.Context, job jobs.Job) error {
		logger.Info("processed telegram update", "job_id", job.ID.String(), "playload", string(job.Payload))
		return nil
	}))
	reg.Register(jobs.TypeCleanupExpiredJobs, jobs.HandleFunc(func(ctx context.Context, job jobs.Job) error {
		payload, err := jobs.DecodePayload[CleanupPayload](job)
		if err != nil {
			return err
		}
		deleted, err := repo.CleanupExpiredJobs(ctx, payload.OlderThanDays)
		if err != nil {
			return err
		}
		logger.Info("cleanup expired jobs completed", "job_id", job.ID.String(), "deleted", deleted, "older_than_days", payload.OlderThanDays)
		return nil
	}))
	registerPaymentHandlers(reg, logger, opts.JobCreator)
}

func registerPaymentHandlers(reg *jobs.Registry, logger *slog.Logger, creator JobCreator) {
	handler := jobs.HandleFunc(func(ctx context.Context, job jobs.Job) error {
		if payloadHasSimulatedFailure(job.Payload) {
			return fmt.Errorf("simulated transient payment failure")
		}
		logger.Info("processed payment job", "job_id", job.ID.String(), "type", job.Type, "payload", string(job.Payload))
		if job.Type == jobs.TypePaymentProcessSucceeded && creator != nil {
			if chatID, ok := payloadChatID(job.Payload); ok {
				return createTelegramFollowUp(ctx, creator, chatID, "Payment succeeded", "payment:telegram:"+job.ID.String())
			}
		}
		return nil
	})
	reg.Register(jobs.TypePaymentProcessSucceeded, handler)
	reg.Register(jobs.TypePaymentProcessFailed, handler)
	reg.Register(jobs.TypePaymentProcessRefund, handler)
	reg.Register(jobs.TypePaymentProcessChargeback, handler)
}

func payloadHasSimulatedFailure(payload json.RawMessage) bool {
	var body map[string]interface{}
	if err := json.Unmarshal(payload, &body); err != nil {
		return false
	}
	if value, ok := body["simulate_failure"].(bool); ok && value {
		return true
	}
	data, ok := body["data"].(map[string]interface{})
	if !ok {
		return false
	}
	value, ok := data["simulate_failure"].(bool)
	return ok && value
}

func payloadChatID(payload json.RawMessage) (interface{}, bool) {
	var body map[string]interface{}
	if err := json.Unmarshal(payload, &body); err != nil {
		return nil, false
	}
	if chatID, ok := body["chat_id"]; ok {
		return chatID, true
	}
	data, ok := body["data"].(map[string]interface{})
	if !ok {
		return nil, false
	}
	chatID, ok := data["chat_id"]
	return chatID, ok
}

func createTelegramFollowUp(ctx context.Context, creator JobCreator, chatID interface{}, text, key string) error {
	payload, err := json.Marshal(map[string]interface{}{"chat_id": chatID, "text": text})
	if err != nil {
		return err
	}
	_, _, err = creator.CreateJob(ctx, jobs.CreateJobInput{
		Type:           jobs.TypeTelegramSendMessage,
		Payload:        payload,
		MaxAttempts:    3,
		IdempotencyKey: &key,
		Producer:       "worker",
	})
	return err
}

func (s *TelegramSender) Handle(ctx context.Context, job jobs.Job) error {
	payload, err := jobs.DecodePayload[TelegramMessagePayload](job)
	if err != nil {
		return err
	}
	if payload.Text == "" {
		return fmt.Errorf("telegram message text is empty")
	}
	if s.botToken == "" {
		s.logger.Info("telegram token is missing")
		return nil
	}
	body, err := json.Marshal(map[string]interface{}{
		"chat_id": payload.ChatID,
		"text":    payload.Text,
	})

	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.telegram.org/bot"+s.botToken+"/sendMessage", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("telegram api returned %s: %s", resp.Status, string(b))
	}
	return nil
}
