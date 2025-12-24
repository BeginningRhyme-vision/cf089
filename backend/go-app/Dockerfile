# Dockerfile for backend/go-app
# Context: backend/go-app
# Usage: docker build -t unbound-backend .

# Stage 0: Get Go Toolchain
FROM golang:1.25-bookworm AS toolchain

# Build Stage
FROM ubuntu:24.04 AS builder

# Install build dependencies
# We only need ca-certificates and standard tools, no C/C++ compiler needed for CGO_ENABLED=0
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy Go toolchain
COPY --from=toolchain /usr/local/go /usr/local/go
ENV PATH="/usr/local/go/bin:${PATH}"

WORKDIR /app

# Disable CGO for a pure Go build
ENV CGO_ENABLED=0

# Copy module files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build
RUN go build -o main .

# Runtime Stage
FROM ubuntu:24.04

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy binary
COPY --from=builder /app/main .

# Config is expected at /app/config.yaml or via volume mount
ENTRYPOINT ["./main"]
