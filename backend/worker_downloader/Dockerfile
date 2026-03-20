# Build Stage
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copy Go module files
COPY go.mod go.sum ./
COPY third_party ./third_party
RUN go mod download

# Copy source code
COPY downloader.go .

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
COPY --from=builder /app/downloader /usr/local/bin/downloader

COPY assets/GeoLite2-Country.mmdb /app/assets/GeoLite2-Country.mmdb

# Copy Python requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy Python script
COPY get_yt_metadata.py .

# Environment variables (defaults)
ENV BACKEND_API_URL="http://host.docker.internal:8080/api"
ENV GEOIP_COUNTRY_MMDB_PATH="/app/assets/GeoLite2-Country.mmdb"

# Default command
# Users should override this to run either the Go downloader or Python metadata fetcher
# e.g., CMD ["downloader"] or CMD ["python", "get_yt_metadata.py"]
CMD ["python", "get_yt_metadata.py"]
