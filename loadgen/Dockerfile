# Load-generator image: sweepscale + fanoutscale + dsload + render binaries.
# Built from the loadgen module context: docker build -f loadgen/Dockerfile loadgen
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/sweepscale ./cmd/sweepscale \
 && CGO_ENABLED=0 go build -o /out/dsload ./cmd/dsload \
 && CGO_ENABLED=0 go build -o /out/render ./cmd/render \
 && CGO_ENABLED=0 go build -o /out/fanoutscale ./cmd/fanoutscale

FROM alpine:3.20
RUN adduser -D -u 10001 loadgen
COPY --from=build /out/sweepscale /out/dsload /out/render /out/fanoutscale /usr/local/bin/
USER loadgen
# No ENTRYPOINT: the Kubernetes Job sets command: ["sweepscale"|"fanoutscale", ...].
