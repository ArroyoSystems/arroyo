FROM rust:1-slim-bookworm as builder
ARG GIT_SHA

RUN apt-get update && \
    apt-get -y install libssl-dev libsasl2-dev openssl pkg-config

WORKDIR /build_dir

COPY arroyo-types arroyo-types
COPY arroyo-worker arroyo-worker
COPY arroyo-rpc arroyo-rpc
COPY arroyo-macro arroyo-macro
COPY arroyo-formats arroyo-formats
COPY arroyo-server-common arroyo-server-common
COPY arroyo-state arroyo-state
COPY arroyo-metrics arroyo-metrics
COPY arroyo-compiler-service arroyo-compiler-service
COPY arroyo-connectors arroyo-connectors
COPY arroyo-api arroyo-api
COPY arroyo-datastream arroyo-datastream
COPY arroyo-node arroyo-node
COPY arroyo-openapi arroyo-openapi
COPY arroyo-operator arroyo-operator
COPY arroyo-df arroyo-df
COPY arroyo-sql-macro arroyo-sql-macro
COPY arroyo-sql-testing arroyo-sql-testing
COPY arroyo-controller arroyo-controller
COPY arroyo-console arroyo-console
COPY arroyo-storage arroyo-storage
COPY copy-artifacts copy-artifacts
COPY integ integ
COPY docker/refinery.toml refinery.toml
COPY connector-schemas connector-schemas
COPY arroyo-openapi arroyo-openapi
COPY arroyo-bin arroyo-bin
COPY arroyo arroyo
COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock

# set git sha
ENV VERGEN_GIT_SHA=$GIT_SHA
RUN cargo build --release --package copy-artifacts

FROM debian:bookworm-slim as runner
WORKDIR /app
RUN apt-get update && \
    apt-get -y install openssl ca-certificates libsasl2-dev

COPY --from=builder /build_dir/target/release/copy-artifacts /usr/local/bin
COPY docker/cluster/worker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENV PRODUCTION=true

ENTRYPOINT [ "/entrypoint.sh" ]
