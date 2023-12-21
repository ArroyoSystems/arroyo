FROM ubuntu:22.04
ENV TZ=Etc/UTC
ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /opt/arroyo/src
ARG MOLD_ARCH
ARG PROTO_ARCH
ARG PROM_ARCH
ARG GIT_SHA

RUN apt-get update
RUN apt-get -y install git curl pkg-config unzip build-essential libssl-dev openssl libsasl2-dev \
    cmake clang wget postgresql postgresql-client supervisor python3 python-is-python3 sudo bash gpg

RUN wget https://github.com/rui314/mold/releases/download/v1.11.0/mold-1.11.0-${MOLD_ARCH}-linux.tar.gz && \
    tar xvfz mold*.tar.gz && \
    mv mold*-linux/bin/* /usr/bin && \
    mv mold*-linux/libexec/* /usr/libexec && \
    rm -rf mold*

RUN wget https://github.com/protocolbuffers/protobuf/releases/download/v21.8/protoc-21.8-linux-${PROTO_ARCH}.zip && \
    unzip protoc*.zip && \
    mv bin/protoc /usr/local/bin

# Install prometheus and pushgateway
RUN wget https://github.com/prometheus/prometheus/releases/download/v2.43.0/prometheus-2.43.0.linux-${PROM_ARCH}.tar.gz && \
    tar xvfz prometheus*.tar.gz && \
    mv prometheus*/prometheus /usr/local/bin && \
    wget https://github.com/prometheus/pushgateway/releases/download/v1.5.1/pushgateway-1.5.1.linux-${PROM_ARCH}.tar.gz && \
    tar xvfz pushgateway*.tar.gz && \
    mv pushgateway*/pushgateway /usr/local/bin && \
    rm -rf prometheus* pushgateway*
COPY docker/single/prometheus.yml /etc/prometheus/prometheus.yml

# Install Rust
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y

ENV CARGO_HOME=/root/.cargo
ENV PATH=$CARGO_HOME/bin:$PATH

COPY docker/config.toml $CARGO_HOME/config

RUN cargo install wasm-pack

# Set up DB
RUN cargo install refinery_cli

# Install node & pnpm
RUN mkdir -p /etc/apt/keyrings && \
    curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg && \
    echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_18.x nodistro main" |  \
      sudo tee /etc/apt/sources.list.d/nodesource.list && \
    apt-get update && apt-get install -y nodejs && \
    curl -fsSL https://get.pnpm.io/install.sh | env PNPM_VERSION=8.10.4 SHELL=$(which bash) bash -

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
COPY arroyo arroyo

COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock

RUN service postgresql start && \
    sudo -u postgres psql -c "CREATE USER arroyo WITH PASSWORD 'arroyo' SUPERUSER;" && \
    sudo -u postgres createdb arroyo && \
    refinery migrate -c refinery.toml -p arroyo-api/migrations

# set git hash
ENV VERGEN_GIT_SHA=$GIT_SHA

# Build Arroyo
RUN service postgresql start && \
    cargo build --release --all-features -p arroyo-compiler-service -p arroyo-controller -p arroyo-api && \
    mv target/release/arroyo-* /usr/bin && \
    rm -rf target/release && \
    service postgresql stop

RUN bash -c "cd arroyo-console && source ~/.bashrc && /root/.local/share/pnpm/pnpm install && /root/.local/share/pnpm/pnpm build"

RUN rm -rf .git

WORKDIR /opt/arroyo/build

COPY docker/build_base pipeline/
RUN cd pipeline && cargo build --release && cargo build --release
RUN cd pipeline/wasm-fns && wasm-pack build
COPY docker/single/supervisord.conf /opt/arroyo/src/docker/single/supervisord.conf
COPY docker/single/entrypoint.sh /entrypoint.sh
EXPOSE 8000 9000 9190 9191
RUN mkdir -p /tmp/arroyo/build
ENTRYPOINT ["/entrypoint.sh"]
