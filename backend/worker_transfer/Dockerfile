# Build Stage
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copy Go module files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY *.go ./

# Build the binaries
RUN go build -o scanner .
RUN go build -o transfer .

# Runtime Stage
FROM alpine:latest

WORKDIR /app

# Install CA certificates for HTTPS
RUN apk add --no-cache ca-certificates

# Copy binaries from builder
COPY --from=builder /app/scanner /usr/local/bin/scanner
COPY --from=builder /app/transfer /usr/local/bin/transfer

# Environment variables (defaults)
ENV BACKEND_API_URL="http://host.docker.internal:8080/api"

# Default command
# Users should override this to run either the scanner or transfer worker
# e.g., CMD ["scanner"] or CMD ["transfer"]
CMD ["scanner"]
