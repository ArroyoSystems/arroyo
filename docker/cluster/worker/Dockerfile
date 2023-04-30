FROM rust:1-slim-bookworm as builder
WORKDIR /build_dir

COPY docker/cluster/worker/copy_artifacts .
RUN cargo build --release

FROM debian:bookworm-slim as runner
WORKDIR /app
RUN apt-get update && \
    apt-get -y install openssl ca-certificates

COPY --from=builder /build_dir/target/release/copy_artifacts /usr/local/bin
COPY docker/cluster/worker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENV PRODUCTION=true

ENTRYPOINT [ "/entrypoint.sh" ]
