FROM golang:1.25-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG SERVICE=api
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/app ./cmd/${SERVICE}

FROM alpine:3.22

RUN adduser -D -H appuser
USER appuser
WORKDIR /app
COPY --from=builder /out/app /app/app
COPY configs /app/configs

EXPOSE 8080 9090 9093
ENTRYPOINT ["/app/app"]
