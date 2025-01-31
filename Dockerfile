# syntax=docker/dockerfile:1
FROM golang:1.23 AS builder

# caching layer
RUN echo "2023-01-03T01:33:49Z" && cd / && git clone https://github.com/1lann/aqueduct && \
    cd aqueduct && GOPROXY=https://proxy.golang.org,direct CGO_ENABLED=0 go build .

WORKDIR /app

COPY . .

RUN CGO_ENABLED=0 go build -o aqueduct .

# Execution container
FROM gcr.io/distroless/static:nonroot

COPY --from=builder /app/aqueduct /aqueduct

ENTRYPOINT ["/aqueduct"]
