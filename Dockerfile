FROM golang:1.24-alpine AS builder

WORKDIR /app

# Copy dependency files first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .
COPY .env /app/.env

# Build the application from cmd/streamserver directory
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/stream-server ./cmd/stream_server
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/data-connector ./cmd/data_connector
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/mongo-sub ./cmd/mongo_sub
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/telegram-bot ./cmd/telegram_bot
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/graphql-server ./cmd/graphql
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/etl ./cmd/etl

# Final stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy all binaries
COPY --from=builder /app/bin/ /app/bin/

# Expose port if your server uses one
EXPOSE 8080

CMD ["./stream-server"]