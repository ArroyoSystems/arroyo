FROM ubuntu:22.04
ENV TZ=Etc/UTC
ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /opt/arroyo/src
ARG MOLD_ARCH
ARG PROTO_ARCH

RUN apt-get update
RUN apt-get -y install curl pkg-config unzip build-essential libssl-dev openssl \
    cmake clang wget postgresql postgresql-client supervisor python3 python-is-python3 sudo bash

RUN wget https://github.com/rui314/mold/releases/download/v1.11.0/mold-1.11.0-${MOLD_ARCH}-linux.tar.gz && \
    tar xvfz mold*.tar.gz && \
    mv mold*-linux/bin/* /usr/bin && \
    mv mold*-linux/libexec/* /usr/libexec && \
    rm -rf mold*

RUN wget https://github.com/protocolbuffers/protobuf/releases/download/v21.8/protoc-21.8-linux-${PROTO_ARCH}.zip && \
    unzip protoc*.zip && \
    mv bin/protoc /usr/local/bin

# Install Rust
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y

ENV CARGO_HOME=/root/.cargo
ENV PATH=$CARGO_HOME/bin:$PATH

COPY docker/single/Cargo.toml $CARGO_HOME/config

RUN cargo install wasm-pack

# Set up DB
RUN cargo install refinery_cli

# Install pnpm
RUN  curl -fsSL https://deb.nodesource.com/setup_19.x | sudo -E bash - && \
    sudo apt-get install -y nodejs && \
    curl -fsSL https://get.pnpm.io/install.sh | SHELL=$(which bash) bash -

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
COPY docker/single/refinery.toml refinery.toml

COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock

RUN service postgresql start && \
    sudo -u postgres psql -c "CREATE USER arroyo WITH PASSWORD 'arroyo' SUPERUSER;" && \
    sudo -u postgres createdb arroyo && \
    refinery migrate -c refinery.toml -p arroyo-api/migrations

# Build Arroyo
RUN service postgresql start && \
    cargo build --release -p arroyo-compiler-service -p arroyo-controller -p arroyo-api && \
    mv target/release/arroyo-* /usr/bin && \
    rm -rf target/release && \
    service postgresql stop

RUN bash -c "cd arroyo-console && source ~/.bashrc && /root/.local/share/pnpm/pnpm install && /root/.local/share/pnpm/pnpm build"

WORKDIR /opt/arroyo/build

COPY docker/single/build_base pipeline/
RUN cd pipeline && cargo build --release && cargo build --release
RUN cd pipeline/wasm-fns && wasm-pack build
COPY docker/single/supervisord.conf /opt/arroyo/src/docker/single/supervisord.conf
EXPOSE 8000 8001 9000 9190 9191
RUN mkdir -p /tmp/arroyo/build
CMD ["/usr/bin/supervisord", "-c", "/opt/arroyo/src/docker/single/supervisord.conf"]
