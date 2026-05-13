package queue

import (
	"context"
	"task-scheduler/internal/jobs"
)

type Message struct {
	ID      string
	JobID   string
	JobType string
}

type ReadyQueue interface {
	CreateConsumerGroup(ctx context.Context) error
	Publish(ctx context.Context, job jobs.Job) error
	Read(ctx context.Context, consumer string, count int) ([]Message, error)
	Ack(ctx context.Context, messageID string) error
	RecoverPending(ctx context.Context, consumer string, minIdleMills int64, count int) ([]Message, error)
}
