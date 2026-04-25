# syntax=docker/dockerfile:1.7

# Multi-stage build. CGO_ENABLED=0 so the final image can be a
# minimal scratch-ish distro; matches .goreleaser.yml's binary
# build flags so `docker build` and a release archive produce
# byte-identical binaries for the same commit.
FROM golang:1.26-alpine AS builder

WORKDIR /src

# Dependency layer first so an unchanged go.mod/go.sum hits cache.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

# Version is stamped into structures.Version; the release pipeline
# overrides this via `--build-arg VERSION=v1.2.3` but `docker build .`
# alone produces a "dev" build, which is honest when running from
# a working copy.
ARG VERSION=dev
ARG COMMIT=unknown
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 \
    go build \
      -trimpath \
      -ldflags="-s -w -X github.com/hypergnomon/hypergnomon/structures.Version=${VERSION}" \
      -o /out/hypergnomon \
      ./cmd/hypergnomon

# Also build the bundled wstest CLI so operators can smoke-test
# the subscribe API against a running container without pulling a
# separate image.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 \
    go build -trimpath -ldflags="-s -w" -o /out/wstest ./cmd/wstest

# ---- runtime ----------------------------------------------------
FROM alpine:3.20

# ca-certificates so TLS to remote daemons and the public GnomonSC
# registry works. tini as PID 1 so SIGTERM from `docker stop`
# propagates to the Go process — otherwise scan flushes are skipped
# at teardown.
RUN apk add --no-cache ca-certificates tini && \
    addgroup -S hg && adduser -S -G hg hg

# gnomondb is the default --db-dir. A mounted volume here lets the
# operator persist the index across container restarts.
RUN mkdir -p /data && chown hg:hg /data
VOLUME ["/data"]
WORKDIR /data

COPY --from=builder /out/hypergnomon /usr/local/bin/hypergnomon
COPY --from=builder /out/wstest      /usr/local/bin/wstest

USER hg

# 8082 = HTTP API, 9190 = WS JSON-RPC. These are the shipped defaults
# from cmd/hypergnomon/main.go; expose both so `docker run -p …` works
# without needing to override flags.
EXPOSE 8082 9190

ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/hypergnomon"]
CMD ["--daemon-rpc-address=127.0.0.1:10102", \
     "--api-address=0.0.0.0:8082", \
     "--ws-address=0.0.0.0:9190", \
     "--db-dir=/data/gnomondb", \
     "--fastsync"]
