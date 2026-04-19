FROM golang:1.25-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /build

COPY go.mod go.sum ./
# COPY taskfile.* ./

RUN go mod download
# RUN go install github.com/go-task/task/v3/cmd/task@latest

COPY cmd/main.go ./cmd/
COPY internal/ ./internal/

ARG VERSION
ARG COMMIT_HASH
ARG ENV
ARG BUILD_TIME
ARG AUTHOR

# Build static binary
RUN CGO_ENABLED=0 \
    go build \
    -ldflags="-w -s -X 'main.Version=${VERSION}' -X 'main.BuildTime=${BUILD_TIME}' -X 'main.CommitHash=${COMMIT_HASH}' -X 'main.Env=${ENV}' -X 'main.Author=${AUTHOR}'" \
    -o relay ./cmd/main.go
RUN ls -lh /build/relay


# Final stage: Pure distroless - NO DOCKER CLI
FROM gcr.io/distroless/static-debian12:nonroot
LABEL \
    org.opencontainers.image.title="Relay" \
    org.opencontainers.image.description="relay" \
    org.opencontainers.image.source="https://github.com/rndmcodeguy20/relay"
# Copy only CA certificates and your binary
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /build/relay /server/relay

USER nonroot
WORKDIR /server

EXPOSE 5972
ENTRYPOINT ["./relay"]