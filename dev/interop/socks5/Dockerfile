# Dante SOCKS5 server for the interop `proxy` scenario: no authentication,
# CONNECT and UDP ASSOCIATE allowed, port 1080, on the internal `swarm`
# network only. Debian's packaged dante-server on the same pinned base as the
# fbt runtime image (multi-arch index digest, see README.md).
FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc
RUN apt-get update \
    && apt-get install -y --no-install-recommends dante-server \
    && rm -rf /var/lib/apt/lists/*
COPY danted.conf /etc/danted.conf
# Port 1080 needs no privilege: run as Debian's `nobody`.
USER nobody
EXPOSE 1080/tcp
# Logs every connection and UDP association to stderr (compose logs).
CMD ["danted", "-f", "/etc/danted.conf"]
