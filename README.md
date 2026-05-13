# distributed-task-scheduler

This project is a distributed background task scheduler written in Go. It supports multiple producers such as REST API, Telegram webhooks, payment webhooks and cron schedules. Jobs are persisted in PostgreSQL, scheduled by run_at, published to Redis Streams, processed by worker pools, retried with exponential backoff and moved to a dead-letter state after max attempts.

## Architecture

```text
REST API / Telegram / Cron / Payment Webhook 
        ↓
Producer Adapter Layer
        ↓
JobService
        ↓
PostgreSQL
        ↓
Scheduler
        ↓
Redis Streams: jobs:ready
        ↓
Worker Pool
        ↓
Success / Retry / Dead-Letter
```

## Producers

- REST API Producer: `POST /jobs`
- Telegram Webhook Producer: `POST /webhooks/telegram`
- Payment Webhook Mock Producer: `POST /webhooks/payment`
- Cron Producer: configured by `configs/cron.yaml`

All producers create jobs only. They do not execute work directly.

## Job Types

- `telegram.process_update`
- `telegram.send_message`
- `reminder.send`
- `system.cleanup_expired_jobs`
- `payment.process_succeeded`
- `payment.process_failed`
- `payment.process_refund`
- `payment.process_chargeback`

## Local Setup

```bash
make up
make migrate
```

For local development without Docker services for app binaries:

```bash
make run-api
make run-scheduler
make run-worker
```

Run tests:

```bash
make test
```

## Environment Variables

| Variable | Example |
| --- | --- |
| `APP_ENV` | `local` |
| `DATABASE_URL` | `postgres://scheduler:scheduler@localhost:5432/scheduler?sslmode=disable` |
| `REDIS_ADDR` | `localhost:6379` |
| `REDIS_PASSWORD` | empty |
| `REDIS_DB` | `0` |
| `TELEGRAM_BOT_TOKEN` | optional |
| `TELEGRAM_WEBHOOK_SECRET` | `dev-secret` |
| `PAYMENT_WEBHOOK_SECRET` | `payment-dev-secret` |
| `SCHEDULER_INTERVAL` | `1s` |
| `SCHEDULER_BATCH_SIZE` | `100` |
| `WORKER_CONCURRENCY` | `15` |
| `LOG_LEVEL` | `debug` |

## REST API

Create an immediate or delayed job:

```bash
curl -X POST http://localhost:8081/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "type": "telegram.send_message",
    "payload": {
      "chat_id": 123456,
      "text": "hello"
    },
    "run_at": "2026-05-02T18:00:00Z",
    "max_attempts": 5,
    "idempotency_key": "external-service:event-123"
  }'
```

Other endpoints:

- `GET /jobs/{id}`
- `POST /jobs/{id}/cancel`
- `POST /jobs/{id}/retry`
- `GET /healthz`

## Telegram Webhook

```bash
curl -X POST http://localhost:8081/webhooks/telegram \
  -H "Content-Type: application/json" \
  -H "X-Telegram-Bot-Api-Secret-Token: dev-secret" \
  -d '{
    "update_id": 1001,
    "message": {
      "chat": {"id": 123456},
      "text": "/remind 10m drink water"
    }
  }'
```

Supported MVP commands:

- `/remind 10m drink water`
- `/remind 1h call manager`
- `/remind 2026-05-02T18:00:00Z check something`

Telegram update idempotency key format:

```text
telegram:update:<update_id>
```

## Payment Webhook Mock Producer

This producer simulates a payment provider sending events. It demonstrates idempotency and reliable webhook processing without real money or a real payment integration.

Supported events:

- `payment.succeeded` -> `payment.process_succeeded`, max attempts `5`
- `payment.failed` -> `payment.process_failed`, max attempts `3`
- `payment.refund_requested` -> `payment.process_refund`, max attempts `5`
- `payment.chargeback_created` -> `payment.process_chargeback`, max attempts `5`

Example:

```bash
body='{"event_id":"evt_123","type":"payment.succeeded","created_at":"2026-05-02T12:00:00Z","data":{"payment_id":"pay_123","order_id":"order_456","user_id":"user_789","amount":1990,"currency":"RUB"}}'
sig=$(printf '%s' "$body" | openssl dgst -sha256 -hmac payment-dev-secret -hex | awk '{print $2}')

curl -X POST http://localhost:8081/webhooks/payment \
  -H "Content-Type: application/json" \
  -H "X-Payment-Signature: $sig" \
  -d "$body"
```

Payment idempotency key format:

```text
payment:event:<event_id>
```

Payment worker handlers log MVP processing. `payment.process_succeeded` creates a follow-up `telegram.send_message` job when `chat_id` is present in the payload. Any payment payload containing `"simulate_failure": true` triggers a transient failure to exercise retry behavior.

## Security And Signature Validation

Payment webhooks verify HMAC-SHA256 over the raw request body and compare signatures with constant-time comparison.

- Payment header: `X-Payment-Signature`
- Payment secret: `PAYMENT_WEBHOOK_SECRET`

## Producer Idempotency

Idempotency is enforced by the existing PostgreSQL-backed `JobService.CreateJob` flow. Producers only choose stable keys:

- REST: caller-supplied `idempotency_key`
- Telegram: `telegram:update:<update_id>`
- Cron: `cron:<job_name>:<scheduled_timestamp>`
- Payment: `payment:event:<event_id>`

## Cron Config

Cron jobs live in [configs/cron.yaml](configs/cron.yaml):

```yaml
cron_jobs:
  - name: cleanup-expired-jobs
    schedule: "0 * * * *"
    type: "system.cleanup_expired_jobs"
    payload:
      older_than_days: 30
    max_attempts: 3
```

Cron producer idempotency key format:

```text
cron:<job_name>:<scheduled_timestamp>
```

## Scheduling And Delivery

Immediate jobs are created as `ready` and published to Redis Streams. Delayed jobs are stored as `scheduled`.

The scheduler periodically scans PostgreSQL for:

```sql
status = 'scheduled' AND run_at <= now()
```

It uses `FOR UPDATE SKIP LOCKED`, changes jobs to `ready`, and publishes messages to Redis stream `jobs:ready`.

Redis consumer group:

```text
workers
```

Message fields:

- `job_id`
- `job_type`

## Retries And Dead-Letter State

Workers execute jobs with at-least-once semantics. On failure:

- attempts are incremented
- if attempts remain, job returns to `scheduled`
- `run_at` is set using exponential backoff
- when max attempts are exhausted, job becomes `dead`

Backoff schedule:

- attempt 1: 10 seconds
- attempt 2: 30 seconds
- attempt 3: 2 minutes
- attempt 4: 5 minutes
- attempt 5+: 15 minutes

## Tests

The repository includes focused tests for:

- JobService create job behavior
- idempotency behavior
- scheduler due job enqueueing
- retry/backoff logic
- Telegram webhook reminder parsing
- cron job creation
- REST API job creation
- Payment webhook signature, duplicate and mapping behavior
Run:

```bash
go test ./...
```

## Load Tests

See [loadtests/result.md](loadtests/result.md). This is a placeholder for future k6, vegeta or hey scenarios that stress immediate jobs, delayed jobs, retries and worker throughput.
