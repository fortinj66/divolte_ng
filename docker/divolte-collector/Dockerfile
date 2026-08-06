# Builder stage stays on the pinned Go toolchain (see README - go.mod
# requires 1.19 and several deps are only verified against that version),
# independent of the runtime base image below.
FROM docker.io/library/golang:1.19-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/divolte-collector ./cmd/divolte-collector

# fedora-minimal instead of the full fedora image - the app only needs
# ca-certificates and a place to run a static binary, and the full image's
# large base layer was hitting a gateway size/timeout limit on chunked
# uploads to some internal registries that pulls never exercise.
FROM quay.io/fedora/fedora-minimal:44
RUN microdnf install -y ca-certificates && microdnf clean all \
    && useradd -r -u 1000 -m -d /app -s /sbin/nologin divolte

WORKDIR /app
COPY --from=builder /out/divolte-collector ./divolte-collector
COPY configs ./configs

USER divolte
EXPOSE 8290 8291
ENTRYPOINT ["/app/divolte-collector"]
CMD ["-config", "configs/server.yaml"]
