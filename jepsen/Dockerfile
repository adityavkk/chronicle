# Minimal image around a host-built static chronicle binary. Building the binary
# on the host (where GOPROXY/certs are configured) keeps the image build offline
# and fast. Build context is the jepsen/ directory.
FROM alpine:3.20
RUN adduser -D -u 10001 chronicle
COPY bin/chronicle-linux /usr/local/bin/chronicle
USER chronicle
EXPOSE 4437
ENTRYPOINT ["/usr/local/bin/chronicle"]
