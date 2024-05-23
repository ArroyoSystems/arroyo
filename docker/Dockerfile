# syntax=docker/dockerfile:1.4

FROM rust:1-bookworm AS builder
WORKDIR /app
ARG GIT_SHA
ARG TARGETPLATFORM
ARG PROFILE=release

RUN apt-get update && \
    apt-get -y install curl pkg-config unzip build-essential libssl-dev openssl \
    cmake clang wget postgresql postgresql-client supervisor python3 python-is-python3 sudo bash libsasl2-dev && \
    cargo install refinery_cli

# Install node & pnpm
RUN mkdir -p /etc/apt/keyrings && \
    curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg && \
    echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_18.x nodistro main" |  \
      sudo tee /etc/apt/sources.list.d/nodesource.list && \
    apt-get update && apt-get install -y nodejs && \
    curl -fsSL https://get.pnpm.io/install.sh | env PNPM_VERSION=9.0.6 SHELL=$(which bash) bash -

COPY docker/install_deps.sh /install_deps.sh
RUN sh /install_deps.sh

COPY webui/pnpm-lock.yaml webui/pnpm-lock.yaml
RUN bash -c "cd webui && source ~/.bashrc && /root/.local/share/pnpm/pnpm fetch"

COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock

COPY crates crates
COPY webui webui

# set git hash
ENV VERGEN_GIT_SHA=$GIT_SHA
RUN bash -c "cd webui && source ~/.bashrc && /root/.local/share/pnpm/pnpm install && /root/.local/share/pnpm/pnpm build"

COPY docker/refinery.toml refinery.toml

RUN --mount=type=cache,target=/usr/local/cargo/registry,id=${TARGETPLATFORM}-${PROFILE} \
      --mount=type=cache,target=/app/target,id=${TARGETPLATFORM}-${PROFILE} \
    service postgresql start && \
    sudo -u postgres psql -c "CREATE USER arroyo WITH PASSWORD 'arroyo' SUPERUSER;" && \
    sudo -u postgres createdb arroyo && \
    refinery migrate -c refinery.toml -p crates/arroyo-api/migrations && \
    CARGO_PROFILE_RELEASE_DEBUG=false cargo build --profile ${PROFILE} --bin arroyo-bin --all-features && \
    mv target/release/arroyo-bin /arroyo-bin

FROM debian:bookworm-slim AS arroyo-single
WORKDIR /app
RUN apt-get update && \
    apt-get -y install libssl-dev openssl supervisor libsasl2-dev curl postgresql postgresql-client sudo pkg-config

COPY --from=builder /arroyo-bin ./

COPY docker/supervisord.conf /supervisord.conf
COPY docker/entrypoint.sh /entrypoint.sh

ENV PRODUCTION=true \
    ASSET_DIR="/app/dist" \
    INSTALL_RUSTC=true \
    INSTALL_CLANG=true

RUN chmod +x /entrypoint.sh

EXPOSE 8000
ENTRYPOINT [ "/entrypoint.sh" ]

FROM debian:bookworm-slim as arroyo
WORKDIR /app

RUN apt-get update && \
    apt-get -y install libsasl2-2 ca-certificates curl pkg-config

COPY --from=builder /arroyo-bin ./

ENV PRODUCTION=true \
    ASSET_DIR="/app/dist" \
    INSTALL_RUSTC=true \
    INSTALL_CLANG=true

EXPOSE 8000
ENTRYPOINT [ "/app/arroyo-bin" ]

FROM rust:slim-bookworm AS arroyo-full
WORKDIR /app
RUN apt-get update && \
    apt-get -y install libsasl2-2 ca-certificates curl pkg-config

COPY --from=builder /arroyo-bin ./

ENV PRODUCTION=true \
    ASSET_DIR="/app/dist"

EXPOSE 8000
ENTRYPOINT [ "/app/arroyo-bin" ]
