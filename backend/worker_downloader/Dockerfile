# Build Stage
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copy shared packages
COPY pkg/proxypool pkg/proxypool

WORKDIR /app/worker_downloader

# Copy Go module files
COPY worker_downloader/go.mod worker_downloader/go.sum ./
RUN go mod download

# Copy source code
COPY worker_downloader/downloader.go .

# Build the Go binary
RUN go build -o downloader downloader.go

# Runtime Stage
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
# ffmpeg is required for yt-dlp to merge audio/video
# ca-certificates for HTTPS
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Copy Go binary from builder
COPY --from=builder /app/worker_downloader/downloader /usr/local/bin/downloader

# Copy Python requirements and install
COPY worker_downloader/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy Python script
COPY worker_downloader/get_yt_metadata.py .

# Environment variables (defaults)
ENV BACKEND_API_URL="http://host.docker.internal:8080/api"

# Default command
# Users should override this to run either the Go downloader or Python metadata fetcher
# e.g., CMD ["downloader"] or CMD ["python", "get_yt_metadata.py"]
CMD ["python", "get_yt_metadata.py"]
