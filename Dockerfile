FROM golang:1.25-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG SERVICE=api
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/app ./cmd/${SERVICE}

RUN wget -O /out/migrate.tar.gz https://github.com/golang-migrate/migrate/releases/download/v4.18.2/migrate.linux-amd64.tar.gz \
    && tar -xzf /out/migrate.tar.gz -C /out \
    && chmod +x /out/migrate

FROM alpine:3.22

RUN apk add --no-cache ca-certificates tzdata
RUN adduser -D -H appuser
WORKDIR /app
COPY --from=builder /out/app /app/app
COPY --from=builder /out/migrate /app/migrate
COPY configs /app/configs
COPY migrations /app/migrations
COPY scripts/entrypoint.sh /app/entrypoint.sh

RUN chmod +x /app/entrypoint.sh
RUN chown -R appuser /app
USER appuser

EXPOSE 8080
ENTRYPOINT ["/app/entrypoint.sh"]