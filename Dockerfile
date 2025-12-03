# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Copy go mod files and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY cmd/ cmd/
COPY internal/ internal/
COPY pkg/ pkg/

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -o kafka-mqtt-bridge ./cmd/bridge

# Runtime stage
FROM scratch

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/kafka-mqtt-bridge .

# Default command
ENTRYPOINT ["./kafka-mqtt-bridge"]
CMD ["-config", "/config/bridge-config.yaml"]
