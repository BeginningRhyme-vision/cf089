# Build Stage
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copy Go module files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY *.go ./

# Build the binary
RUN go build -o worker_ffmpeg main.go

# Runtime Stage
FROM alpine:latest

WORKDIR /app

# Install CA certificates and ffmpeg
RUN apk add --no-cache ca-certificates ffmpeg

# Copy binary from builder
COPY --from=builder /app/worker_ffmpeg /usr/local/bin/worker_ffmpeg

# Environment variables
ENV BACKEND_API_URL="http://host.docker.internal:8080/api"

CMD ["worker_ffmpeg"]
