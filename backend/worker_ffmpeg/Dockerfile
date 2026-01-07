# Build Stage
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copy Go module files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY cmd/ ./cmd/
COPY common/ ./common/

# Build the binaries
RUN go build -o scanner ./cmd/scanner/main.go
RUN go build -o worker ./cmd/worker/main.go

# Runtime Stage
FROM alpine:latest

WORKDIR /app

# Install CA certificates and ffmpeg
RUN apk add --no-cache ca-certificates ffmpeg

# Copy binaries from builder
COPY --from=builder /app/scanner /usr/local/bin/ffmpeg_scanner
COPY --from=builder /app/worker /usr/local/bin/ffmpeg_worker

# Environment variables
ENV BACKEND_API_URL="http://host.docker.internal:8080/api"
ENV REDIS_URL="redis://localhost:6379/0"
ENV MAX_THREADS="1"

# By default run worker, can be overridden by entrypoint/command in k8s
CMD ["ffmpeg_worker"]