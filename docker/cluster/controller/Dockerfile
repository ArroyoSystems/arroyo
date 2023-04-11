FROM rust:1-bookworm AS builder
WORKDIR app
ARG PROTO_ARCH

RUN apt-get update && \
    apt-get -y install curl pkg-config unzip build-essential libssl-dev openssl \
    cmake clang wget postgresql postgresql-client supervisor python3 python-is-python3 sudo bash && \
    cargo install refinery_cli && \
    cargo install wasm-pack && \
    wget https://github.com/protocolbuffers/protobuf/releases/download/v21.8/protoc-21.8-linux-${PROTO_ARCH}.zip && \
    unzip protoc*.zip && \
    mv bin/protoc /usr/local/bin && \
    curl -fsSL https://deb.nodesource.com/setup_19.x | sudo -E bash - && \
    sudo apt-get install -y nodejs && \
    curl -fsSL https://get.pnpm.io/install.sh | SHELL=$(which bash) bash -

COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock

COPY arroyo-types arroyo-types
COPY arroyo-worker arroyo-worker
COPY arroyo-rpc arroyo-rpc
COPY arroyo-macro arroyo-macro
COPY arroyo-server-common arroyo-server-common
COPY arroyo-state arroyo-state
COPY arroyo-metrics arroyo-metrics
COPY arroyo-compiler-service arroyo-compiler-service

COPY arroyo-api arroyo-api
COPY arroyo-datastream arroyo-datastream
COPY arroyo-node arroyo-node
COPY arroyo-sql arroyo-sql
COPY arroyo-sql-macro arroyo-sql-macro
COPY arroyo-sql-testing arroyo-sql-testing
COPY arroyo-controller arroyo-controller
COPY arroyo-console arroyo-console
COPY docker/refinery.toml refinery.toml

RUN service postgresql start && \
    sudo -u postgres psql -c "CREATE USER arroyo WITH PASSWORD 'arroyo' SUPERUSER;" && \
    sudo -u postgres createdb arroyo && \
    refinery migrate -c refinery.toml -p arroyo-api/migrations && \
    CARGO_PROFILE_RELEASE_DEBUG=false cargo build --release --bin arroyo-api --bin arroyo-controller && \
    bash -c "cd arroyo-console && source ~/.bashrc && /root/.local/share/pnpm/pnpm install && /root/.local/share/pnpm/pnpm build"

FROM debian:bookworm-slim AS runner
WORKDIR app
RUN apt-get update && \
    apt-get -y install libssl-dev openssl supervisor
COPY --from=builder /app/target/release/arroyo-api /app/target/release/arroyo-controller /app/arroyo-console/dist ./
COPY docker/cluster/controller/supervisord.conf /supervisord.conf

ENTRYPOINT [ "/usr/bin/supervisord", "-c", "/supervisord.conf" ]
