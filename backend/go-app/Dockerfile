# Dockerfile for backend/go-app
# Context: backend/go-app
# Usage: docker build -t unbound-backend .
# Note: You must provide config.yaml at runtime (e.g., via volume mount to /app/config.yaml)

# Stage 0: Get Go Toolchain
# Use a valid Go version to retrieve the toolchain
FROM golang:1.23-bookworm AS toolchain

# Build Stage
# Use Ubuntu 24.04 to ensure we have GLIBC >= 2.38 (it has 2.39) required by liblancedb_go.so
FROM ubuntu:24.04 AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    wget \
    build-essential \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy Go toolchain from the official image
COPY --from=toolchain /usr/local/go /usr/local/go
ENV PATH="/usr/local/go/bin:${PATH}"

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
RUN wget https://release.unboundfuture.ai/unbound-admin/liblancedb_go.so && mv liblancedb_go.so /usr/local/lib/

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
# Use Ubuntu 24.04 for runtime as well to match glibc version
FROM ubuntu:24.04

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy binary
COPY --from=builder /app/main .

# Copy native library
# The application will look for shared libs in standard paths like /usr/lib
COPY --from=builder /usr/local/lib/liblancedb_go.so /usr/lib/

# Config is expected at /app/config.yaml or via volume mount
# CMD ["./main"]
ENTRYPOINT ["./main"]