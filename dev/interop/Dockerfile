# fbt-example for the interop harness. Build context: the repository root.
# Images pinned by multi-arch index digest (see README.md).

FROM rust:1.99-bookworm@sha256:e6f2cff35d0f309acf8433b4f05878ed77092602c5065cc81801177da0bb3a9c AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p fbt-example \
    && cp target/release/fbt-example /usr/local/bin/fbt-example

FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc
# curl and jq drive the qBittorrent WebUI API from inside the compose network.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl jq \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local/bin/fbt-example /usr/local/bin/fbt-example
# Non-root by default; compose overrides the uid/gid with the caller's.
RUN useradd --system --uid 10001 --no-create-home fbt
USER fbt
CMD ["sleep", "infinity"]
