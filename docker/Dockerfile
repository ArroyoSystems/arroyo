# syntax=docker/dockerfile:1.4

FROM rust:1-bookworm AS builder
WORKDIR /app
ARG GIT_SHA
ARG TARGETPLATFORM
ARG PROFILE=release

RUN apt-get update && \
    apt-get -y install curl pkg-config unzip build-essential libssl-dev openssl \
    cmake clang wget postgresql postgresql-client supervisor sudo bash libsasl2-dev && \
    cargo install refinery_cli

# Install Python (manually, as no bookworm package for 3.12)


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
    CARGO_NET_GIT_FETCH_WITH_CLI=true CARGO_PROFILE_RELEASE_DEBUG=false cargo build --profile ${PROFILE} --bin arroyo && \
    mv target/*/arroyo /arroyo


FROM debian:bookworm-slim as arroyo
WORKDIR /app

RUN apt-get update && \
    apt-get -y install libsasl2-2 ca-certificates curl pkg-config && \
    mkdir /config

COPY --from=builder /arroyo ./
COPY --from=builder /usr/local/bin/protoc /usr/local/bin/protoc
COPY --from=builder /app/include/google /usr/local/include/google
COPY --from=builder /app/python /python

RUN cp -r /python/bin/* /usr/local/bin/ && \
    cp -r /python/include/* /usr/local/include/ && \
    cp -r /python/lib/* /usr/local/lib/ && \
    cp -r /python/share/* /usr/local/share/ && \
    rm -rf /python && \
    ldconfig

ENV INSTALL_RUSTC=true \
    INSTALL_CLANG=true \
    ARROYO__API__RUN_HTTP_PORT=5115

EXPOSE 5115
ENTRYPOINT [ "/app/arroyo" ]
CMD ["cluster"]

FROM rust:slim-bookworm AS arroyo-full
WORKDIR /app
RUN apt-get update && \
    apt-get -y install libsasl2-2 ca-certificates curl pkg-config

ENV ARROYO__API__RUN_HTTP_PORT=5115

COPY --from=builder /arroyo ./
COPY --from=builder /usr/local/bin/protoc /usr/local/bin/protoc
COPY --from=builder /app/include/google /usr/local/include/google
COPY --from=builder /app/python /python

RUN cp -r /python/bin/* /usr/local/bin/ && \
    cp -r /python/include/* /usr/local/include/ && \
    cp -r /python/lib/* /usr/local/lib/ && \
    cp -r /python/share/* /usr/local/share/ && \
    rm -rf /python && \
    ldconfig
  

EXPOSE 5115
ENTRYPOINT [ "/app/arroyo" ]
CMD ["cluster"]