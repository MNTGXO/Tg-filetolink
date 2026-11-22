# syntax=docker/dockerfile:1

# ---------- build stage ----------
FROM golang:1.22-alpine AS builder

# The official golang image pins GOTOOLCHAIN=local; "auto" lets Go fetch a newer
# toolchain if the latest gotd/td release requires one.
ENV GOTOOLCHAIN=auto \
    CGO_ENABLED=0 \
    GOOS=linux

RUN apk add --no-cache git ca-certificates

WORKDIR /src
COPY go.mod go.sum* ./
COPY *.go start.mp4 ./

# Resolves the latest gotd/td and writes go.sum if it is not committed yet.
RUN go mod tidy && \
    go build -trimpath -ldflags="-s -w" -o /out/tgstream .

# ---------- runtime stage ----------
# distroless/static already ships CA certificates and tzdata.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/tgstream /tgstream

# Make the GC reclaim aggressively before Koyeb's 512 MB OOM limit.
ENV GOMEMLIMIT=400MiB \
    PORT=8000

EXPOSE 8000
USER nonroot:nonroot
ENTRYPOINT ["/tgstream"]
