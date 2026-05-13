package telegram

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"task-scheduler/internal/jobs"
	"time"
)

type Handler struct {
	service *jobs.Service
	secret  string
}

type Update struct {
	UpdateID int64 `json:"update_id"`
	Message  *struct {
		Chat struct {
			ID int64 `json:"id"`
		} `json:"chat"`
		Text string `json:"text"`
	} `json:"message"`
}

func NewHandler(service *jobs.Service, secret string) *Handler {
	return &Handler{service, secret}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.secret != "" && r.Header.Get("X-Telegram-Bot-Api-Secret-Token") != h.secret {
		http.Error(w, "invalid telegram secret", http.StatusUnauthorized)
		return
	}

	var update Update
	if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
		http.Error(w, "invalid telegram update", http.StatusBadRequest)
		return
	}

	key := fmt.Sprintf("telegram:update:%d", update.UpdateID)
	runAt := time.Now().UTC()
	text := strings.TrimSpace(update.Message.Text)
	chatID := update.Message.Chat.ID
	jobType := jobs.TypeTelegramProcessUpdate
	maxAttempts := 3
	payload, err := json.Marshal(map[string]interface{}{"update": update})
	if err != nil {
		http.Error(w, "invalid telegram update", http.StatusBadRequest)
		return
	}
	if strings.HasPrefix(text, "/remind") {
		newTime, message, err := parseReminder(text, runAt)
		if err != nil {
			jobType = jobs.TypeTelegramSendMessage
			payload, _ = json.Marshal(map[string]interface{}{"chat_id": chatID, "text": "Usage: /remind <10m|1h|RFC3339 time> <text>"})
		} else {
			jobType = jobs.TypeReminderSend
			runAt = newTime
			payload, _ = json.Marshal(map[string]interface{}{"chat_id": chatID, "text": message})
			maxAttempts = 5
		}
	} else if strings.HasPrefix(text, "/status") {
		jobType = jobs.TypeTelegramSendMessage
		payload, _ = json.Marshal(map[string]interface{}{"chat_id": chatID, "text": "Scheduler is running. Use REST API /jobs/{id} for detailed status."})
	} else {
		jobType = jobs.TypeTelegramSendMessage
		payload, _ = json.Marshal(map[string]interface{}{"chat_id": chatID, "text": "Supported commands: /remind <delay> <text>"})
	}

	job, existed, err := h.service.CreateJob(r.Context(), jobs.CreateJobInput{
		Type:           jobType,
		Payload:        payload,
		RunAt:          runAt,
		MaxAttempts:    maxAttempts,
		IdempotencyKey: &key,
		Producer:       "telegram",
	})

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	status := http.StatusAccepted

	if existed {
		status = http.StatusOK
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(job)
}

func parseReminder(text string, now time.Time) (time.Time, string, error) {
	parts := strings.Fields(text)
	if len(parts) < 3 {
		return time.Time{}, "", fmt.Errorf("missing arguments")
	}

	when := parts[1]
	message := strings.Join(parts[2:], " ")
	if duration, err := time.ParseDuration(when); err == nil {
		return now.Add(duration), message, nil
	}

	if date, err := time.Parse(time.RFC3339, when); err == nil {
		return date, message, nil
	}

	return time.Time{}, "", fmt.Errorf("invalid reminder")
}
