package jobs

import (
	"context"
	"encoding/json"
	"fmt"
)

type Handler interface {
	Handle(ctx context.Context, job Job) error
}

type HandleFunc func(ctx context.Context, job Job) error

func (f HandleFunc) Handle(ctx context.Context, job Job) error {
	return f(ctx, job)
}

type Registry struct {
	handlers map[string]Handler
}

func NewRegistry() *Registry {
	return &Registry{map[string]Handler{}}
}

func (r *Registry) Register(jobType string, handler Handler) {
	r.handlers[jobType] = handler
}

func (r *Registry) Handle(ctx context.Context, job Job) error {
	handler, ok := r.handlers[job.Type]
	if !ok {
		return fmt.Errorf("no handler for job type %s", job.Type)
	}
	return handler.Handle(ctx, job)
}

func DecodePayload[T any](job Job) (T, error) {
	var payload T
	err := json.Unmarshal(job.Payload, &payload)
	return payload, err
}
