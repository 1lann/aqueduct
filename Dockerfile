# syntax=docker/dockerfile:1

# The builder always runs on the machine's own architecture and cross-compiles
# for the target, which needs no emulation because there is no cgo here.
FROM --platform=$BUILDPLATFORM golang:1.23 AS builder

WORKDIR /src

# Dependencies change far less often than the code, so they get a layer of
# their own that survives an ordinary commit.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .

ARG TARGETOS TARGETARCH
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/aqueduct .

# Execution container
FROM gcr.io/distroless/static:nonroot

COPY --from=builder /out/aqueduct /aqueduct

ENTRYPOINT ["/aqueduct"]
