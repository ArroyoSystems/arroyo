FROM rust:1-bookworm-slim AS builder
WORKDIR /opt/arroyo

RUN apt-get update
RUN apt-get -y install curl pkg-config unzip build-essential libssl-dev openssl cmake clang wget

# Install mold
RUN wget https://github.com/rui314/mold/releases/download/v1.5.1/mold-1.5.1-x86_64-linux.tar.gz && \
    tar xvfz mold*.tar.gz && \
    mv mold*-linux/bin/* /usr/bin && \
    mv mold*-linux/libexec/* /usr/libexec && \
    rm -rf mold*

# Install rust
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y

ENV CARGO_HOME=/root/.cargo
ENV PATH=$CARGO_HOME/bin:$PATH

COPY ops/query-compiler/cargo_config.toml $CARGO_HOME/config

# RUN cargo install sccache
RUN cargo install wasm-pack

RUN wget https://github.com/apple/foundationdb/releases/download/7.1.23/foundationdb-clients_7.1.23-1_amd64.deb
RUN dpkg -i foundationdb-clients_7.1.23-1_amd64.deb

RUN wget https://github.com/protocolbuffers/protobuf/releases/download/v21.8/protoc-21.8-linux-x86_64.zip
RUN unzip protoc*.zip
RUN mv bin/protoc /usr/local/bin

RUN mkdir -p query-compiler/src/

# this is dumb but I can't figure out another way to accomplish this in docker...
COPY arroyo-types query-compiler/src/arroyo-types
COPY arroyo-worker query-compiler/src/arroyo-worker
COPY arroyo-rpc query-compiler/src/arroyo-rpc
COPY arroyo-macro query-compiler/src/arroyo-macro
COPY arroyo-server-common query-compiler/src/arroyo-server-common
COPY arroyo-state query-compiler/src/arroyo-state
COPY arroyo-metrics query-compiler/src/arroyo-metrics
COPY arroyo-compiler-service query-compiler/src/arroyo-compiler-service

COPY arroyo-api query-compiler/src/arroyo-api
COPY arroyo-datastream query-compiler/src/arroyo-datastream
COPY arroyo-demo query-compiler/src/arroyo-demo
COPY arroyo-node query-compiler/src/arroyo-node
COPY arroyo-bench query-compiler/src/arroyo-bench
COPY arroyo-program query-compiler/src/arroyo-program
COPY arroyo-sql query-compiler/src/arroyo-sql
COPY arroyo-controller query-compiler/src/arroyo-controller
COPY arroyo-testing query-compiler/src/arroyo-testing
COPY deployer query-compiler/src/deployer

COPY Cargo.toml query-compiler/src
COPY Cargo.lock query-compiler/src

# build the compiler service
RUN cd query-compiler/src && cargo build --release -p arroyo-compiler-service && mv target/release/arroyo-compiler-service /usr/bin

COPY ops/query-compiler/build_base pipeline/
RUN cd pipeline && cargo build --release && cargo build --release

RUN cd pipeline/wasm-fns && wasm-pack build
ENV BUILD_DIR=/opt/arroyo/pipeline
ENV IDLE_SHUTDOWN_MS=600000
EXPOSE 9000
ENTRYPOINT /usr/bin/arroyo-compiler-service
