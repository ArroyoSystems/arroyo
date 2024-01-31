FROM rust:1-bookworm AS builder
WORKDIR /app
ARG PROTO_ARCH
ARG GIT_SHA

RUN apt-get update && \
    apt-get -y install curl pkg-config unzip build-essential libssl-dev openssl \
    cmake clang wget postgresql postgresql-client supervisor python3 python-is-python3 sudo bash libsasl2-dev && \
    cargo install refinery_cli && \
    cargo install wasm-pack && \
    wget https://github.com/protocolbuffers/protobuf/releases/download/v21.8/protoc-21.8-linux-${PROTO_ARCH}.zip && \
    unzip protoc*.zip && \
    mv bin/protoc /usr/local/bin

# Install node & pnpm
RUN mkdir -p /etc/apt/keyrings && \
    curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg && \
    echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_18.x nodistro main" |  \
      sudo tee /etc/apt/sources.list.d/nodesource.list && \
    apt-get update && apt-get install -y nodejs && \
    curl -fsSL https://get.pnpm.io/install.sh | env PNPM_VERSION=8.10.4 SHELL=$(which bash) bash -


COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock

COPY arroyo-types arroyo-types
COPY arroyo-worker arroyo-worker
COPY arroyo-rpc arroyo-rpc
COPY arroyo-formats arroyo-formats
COPY arroyo-macro arroyo-macro
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
COPY arroyo-openapi arroyo-openap
COPY arroyo-storage arroyo-storage
COPY docker/refinery.toml refinery.toml
COPY connector-schemas connector-schemas
COPY arroyo-bin arroyo-bin
COPY arroyo arroyo

COPY copy-artifacts copy-artifacts
COPY integ integ

# set git hash
ENV VERGEN_GIT_SHA=$GIT_SHA
RUN service postgresql start && \
    sudo -u postgres psql -c "CREATE USER arroyo WITH PASSWORD 'arroyo' SUPERUSER;" && \
    sudo -u postgres createdb arroyo && \
    refinery migrate -c refinery.toml -p arroyo-api/migrations && \
    CARGO_PROFILE_RELEASE_DEBUG=false cargo build --release --bin arroyo-api --bin arroyo-controller --all-features && \
    bash -c "cd arroyo-console && source ~/.bashrc && /root/.local/share/pnpm/pnpm install && /root/.local/share/pnpm/pnpm build" && \
    cp "$(which refinery)" /refinery

FROM debian:bookworm-slim AS runner
WORKDIR /app
RUN apt-get update && \
    apt-get -y install libssl-dev openssl supervisor libsasl2-dev
COPY --from=builder /app/target/release/arroyo-api /app/target/release/arroyo-controller ./
COPY --from=builder /app/arroyo-console/dist ./dist
COPY --from=builder /app/arroyo-api/migrations /migrations
COPY --from=builder /refinery /usr/local/bin/refinery

COPY docker/cluster/services/supervisord.conf /supervisord.conf
COPY docker/cluster/services/entrypoint.sh /entrypoint.sh

ENV PRODUCTION=true \
    ASSET_DIR="/app/dist"
RUN chmod +x /entrypoint.sh

ENTRYPOINT [ "/entrypoint.sh" ]
