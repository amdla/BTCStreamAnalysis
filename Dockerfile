FROM golang:1.24-alpine AS builder

WORKDIR /app

# Copy dependency files first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .
COPY .env /app/.env

# Build the application from cmd/stream_server directory
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/stream-server ./cmd/stream_server

# Final stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy binary
COPY --from=builder /app/bin/stream-server .

# Expose port if your server uses one
EXPOSE 8080

CMD ["./stream-server"]