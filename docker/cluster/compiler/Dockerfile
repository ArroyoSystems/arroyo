FROM rust:1-slim-bookworm
WORKDIR /opt/arroyo

ARG PROTO_ARCH
ARG MOLD_ARCH
ARG GIT_SHA

RUN apt-get update && \
    apt-get -y install curl pkg-config unzip build-essential libssl-dev openssl cmake clang wget && \
    # Install mold
    wget https://github.com/rui314/mold/releases/download/v1.11.0/mold-1.11.0-${MOLD_ARCH}-linux.tar.gz && \
    tar xvfz mold*.tar.gz && \
    mv mold*-linux/bin/* /usr/bin && \
    mv mold*-linux/libexec/* /usr/libexec && \
    rm -rf mold* && \
    # Install protoc
    wget https://github.com/protocolbuffers/protobuf/releases/download/v21.8/protoc-21.8-linux-${PROTO_ARCH}.zip && \
    unzip protoc*.zip && \
    mv bin/protoc /usr/local/bin && \
    cargo install wasm-pack

COPY docker/config.toml $CARGO_HOME/config

RUN mkdir -p query-compiler/src/

# copy the source code
COPY arroyo-types src/arroyo-types
COPY arroyo-worker src/arroyo-worker
COPY arroyo-rpc src/arroyo-rpc
COPY arroyo-macro src/arroyo-macro
COPY arroyo-server-common src/arroyo-server-common
COPY arroyo-state src/arroyo-state
COPY arroyo-metrics src/arroyo-metrics
COPY arroyo-compiler-service src/arroyo-compiler-service
COPY arroyo-connectors src/arroyo-connectors
COPY arroyo-api src/arroyo-api
COPY arroyo-datastream src/arroyo-datastream
COPY arroyo-node src/arroyo-node
COPY arroyo-sql src/arroyo-sql
COPY arroyo-sql-macro src/arroyo-sql-macro
COPY arroyo-sql-testing src/arroyo-sql-testing
COPY arroyo-controller src/arroyo-controller
COPY arroyo-console src/arroyo-console
COPY connector-schemas src/connector-schemas

COPY integ src/integ

COPY Cargo.toml src/Cargo.toml
COPY Cargo.lock src/Cargo.lock

# set git hash
ENV VERGEN_GIT_SHA=$GIT_SHA
# build the compiler service
RUN cd src && cargo build --release -p arroyo-compiler-service && mv target/release/arroyo-compiler-service /usr/bin

# Copy and build the base pipeline
COPY docker/build_base pipeline/
RUN cd pipeline && cargo build --release && cargo build --release && \
    cd wasm-fns && wasm-pack build

ENV BUILD_DIR=/opt/arroyo/pipeline
EXPOSE 9000

ENTRYPOINT [ "/usr/bin/arroyo-compiler-service" ]
CMD [ "start" ]
