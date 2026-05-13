APP_NAME := task-scheduler
DATABASE_URL ?= postgres://scheduler:scheduler@localhost:5432/scheduler?sslmode=disable

.PHONY: up down migrate run-api run-scheduler run-worker test tidy build

up:
	docker compose up --build

down:
	docker compose down

migrate:
	docker compose run --rm migrate

run-api:
	go run ./cmd/api

run-scheduler:
	go run ./cmd/scheduler

run-worker:
	go run ./cmd/worker

test:
	go test ./...

tidy:
	go mod tidy

build:
	go build ./cmd/api ./cmd/scheduler ./cmd/worker
