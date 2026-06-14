# chronicle server image — self-contained multi-stage build so Cloud Build
# produces a linux/amd64 image (no QEMU on an arm Mac). Build context is the
# repo root: gcloud builds submit --config loadtest/cloudbuild.yaml .
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/chronicle ./cmd/chronicle

FROM alpine:3.20
RUN adduser -D -u 10001 chronicle
COPY --from=build /out/chronicle /usr/local/bin/chronicle
USER chronicle
EXPOSE 4437 9090
ENTRYPOINT ["chronicle"]
