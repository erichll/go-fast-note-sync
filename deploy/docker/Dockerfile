FROM golang:1.24.5-alpine AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o go-fast-note-sync ./cmd/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
RUN addgroup -S appgroup && \
    adduser -S -D -h /home/appuser -G appgroup appuser && \
    mkdir -p /home/appuser && \
    chown appuser:appgroup /home/appuser
COPY --from=builder --chown=appuser:appgroup /build/go-fast-note-sync /usr/local/bin/go-fast-note-sync
ENV HOME=/home/appuser
USER appuser
WORKDIR /home/appuser
ENTRYPOINT ["go-fast-note-sync"]
CMD ["start"]
