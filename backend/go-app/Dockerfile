# Dockerfile for backend/go-app
# Context: backend/go-app
# Usage: docker build -t unbound-backend .
# Note: You must provide config.yaml at runtime (e.g., via volume mount to /app/config.yaml)

# Build Stage
FROM golang:1.25-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Arguments for LanceDB version
# Check https://github.com/lancedb/lancedb-go/releases for available versions/assets
ARG LANCEDB_VERSION=0.1.2
ARG TARGETARCH=amd64

# Install header file
# We prefer the one from the downloaded zip if available, but fallback/override with local project header
# to ensure compatibility with the Go code.
# Assuming build context is 'backend/go-app', 'include/lancedb.h' should be available.
COPY include/lancedb.h /usr/local/include/
COPY lib/liblancedb_go.so /usr/local/lib/

# Setup env for CGO
ENV CGO_ENABLED=1
ENV CGO_CFLAGS="-I/usr/local/include"
ENV CGO_LDFLAGS="-L/usr/local/lib -Wl,-rpath,/usr/local/lib -llancedb_go"

# Copy module files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build
RUN go build -o main .

# Runtime Stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy binary
COPY --from=builder /app/main .

# Copy native library
COPY --from=builder /usr/local/lib/liblancedb_go.so /usr/lib/

# Config is expected at /app/config.yaml or via volume mount
# CMD ["./main"]
ENTRYPOINT ["./main"]
