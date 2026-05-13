package redisstream

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"task-scheduler/internal/jobs"
	"task-scheduler/internal/queue"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	DefaultStream = "jobs:ready"
	DefaultGroup  = "workers"
)

type Queue struct {
	client *redis.Client
	stream string
	group  string
}

func NewClient(addr, password string, db int) *redis.Client {
	return redis.NewClient(&redis.Options{Addr: addr, Password: password, DB: db})
}

func New(client *redis.Client, stream, group string) *Queue {
	if stream == "" {
		stream = DefaultStream
	}
	if group == "" {
		group = DefaultGroup
	}
	return &Queue{client, stream, group}
}

func (q *Queue) CreateConsumerGroup(ctx context.Context) error {
	err := q.client.XGroupCreateMkStream(ctx, q.stream, q.group, "0").Err()
	if err != nil && strings.Contains(err.Error(), "BUSYGROUP") {
		return nil
	}
	return err
}

func (q *Queue) Publish(ctx context.Context, job jobs.Job) error {
	return q.client.XAdd(ctx, &redis.XAddArgs{
		Stream: q.stream,
		Values: map[string]interface{}{
			"job_id":   job.ID.String(),
			"job_type": job.Type,
		},
	}).Err()
}

func (q *Queue) Read(ctx context.Context, consumer string, count int) ([]queue.Message, error) {
	streams, err := q.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    q.group,
		Consumer: consumer,
		Streams:  []string{q.stream, ">"},
		Count:    int64(count),
		Block:    5 * time.Second,
	}).Result()

	if errors.Is(err, redis.Nil) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	return toMessages(streams), nil
}

func (q *Queue) RecoverPending(ctx context.Context, consumer string, minIdleMills int64, count int) ([]queue.Message, error) {
	msgs, _, err := q.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Group:    q.group,
		Consumer: consumer,
		Stream:   q.stream,
		Count:    int64(count),
		MinIdle:  time.Duration(minIdleMills) * time.Millisecond,
		Start:    "0-0",
	}).Result()

	if errors.Is(err, redis.Nil) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	messages := []queue.Message{}
	for _, msg := range msgs {
		messages = append(messages, convertMessage(msg))
	}
	return messages, nil
}

func (q *Queue) Ack(ctx context.Context, messageID string) error {
	return q.client.XAck(ctx, q.stream, q.group, messageID).Err()
}

func toMessages(streams []redis.XStream) []queue.Message {
	out := []queue.Message{}
	for _, stream := range streams {
		for _, msg := range stream.Messages {
			out = append(out, convertMessage(msg))
		}
	}
	return out
}

func convertMessage(msg redis.XMessage) queue.Message {
	return queue.Message{
		ID:      msg.ID,
		JobID:   fmt.Sprint(msg.Values["job_id"]),
		JobType: fmt.Sprint(msg.Values["job_type"]),
	}
}
