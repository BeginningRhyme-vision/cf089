# Use the official Golang image to create a build artifact.
# This Dockerfile expects to be run from the project root context.
FROM golang:1.25-bookworm AS builder

WORKDIR /app

# Copy the dependency module and the source code
COPY backend/pkg/ backend/pkg/
COPY backend/proxy_tester/ backend/proxy_tester/
COPY config.yaml config.yaml

# Build the binary
WORKDIR /app/backend/proxy_tester
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -v -o proxy_tester .

# Use a Docker multi-stage build to create a lean production image.
FROM alpine:3.19
RUN apk add --no-cache ca-certificates

WORKDIR /root/

# Copy the binary and config to the production image
COPY --from=builder /app/backend/proxy_tester/proxy_tester .
COPY --from=builder /app/config.yaml .

# Run the web service on container startup.
CMD ["./proxy_tester"]