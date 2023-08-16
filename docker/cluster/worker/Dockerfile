FROM rust:1-slim-bookworm as builder
ARG GIT_SHA

WORKDIR /build_dir

COPY docker/cluster/worker/copy_artifacts .

# set git sha
ENV VERGEN_GIT_SHA=$GIT_SHA
RUN cargo build --release

FROM debian:bookworm-slim as runner
WORKDIR /app
RUN apt-get update && \
    apt-get -y install openssl ca-certificates libsasl2-dev

COPY --from=builder /build_dir/target/release/copy_artifacts /usr/local/bin
COPY docker/cluster/worker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENV PRODUCTION=true

ENTRYPOINT [ "/entrypoint.sh" ]
