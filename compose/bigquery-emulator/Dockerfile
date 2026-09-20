ARG GO_VERSION=1.25
ARG DEBIAN_VERSION=bookworm

FROM golang:${GO_VERSION}-${DEBIAN_VERSION} AS builder
ARG BIGQUERY_EMULATOR_VERSION=v0.8.1

RUN git clone --depth 1 --branch "${BIGQUERY_EMULATOR_VERSION}" https://github.com/goccy/bigquery-emulator.git /src
WORKDIR /src
COPY patches /patches
RUN git apply --verbose /patches/*.patch
RUN go test -v -run BigQueryCompat ./internal/contentdata/ ./server/ > /tmp/test.log 2>&1 || { cat /tmp/test.log; exit 1; }; \
    [ "$(grep -c '^--- PASS' /tmp/test.log)" = 4 ]
RUN CGO_ENABLED=0 go build -o /go/bin/bigquery-emulator ./cmd/bigquery-emulator

FROM debian:${DEBIAN_VERSION}-slim
COPY --from=builder /go/bin/bigquery-emulator /bin/bigquery-emulator
ENTRYPOINT ["/bin/bigquery-emulator"]
