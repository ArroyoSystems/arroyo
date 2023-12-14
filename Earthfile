VERSION --global-cache 0.7


IMPORT github.com/earthly/lib/rust AS rust


install:
  FROM rust:1-bookworm
  RUN rustup component add clippy rustfmt
  ARG TARGETARCH

  RUN apt-get update && \
      apt-get -y install curl git pkg-config unzip build-essential libssl-dev libsasl2-dev openssl cmake clang wget \
      postgresql default-jdk nodejs sudo

  RUN wget -qO- https://get.pnpm.io/install.sh | ENV="$HOME/.dashrc" SHELL="$(which dash)" dash -  && \
    . "$HOME/.dashrc" && \
    pnpm install -g @openapitools/openapi-generator-cli && \
    openapi-generator-cli version

  RUN if [ "$TARGETARCH" = "amd64" ]; then \
      wget https://github.com/protocolbuffers/protobuf/releases/download/v21.8/protoc-21.8-linux-x86_64.zip -O protoc.zip; \
    elif [ "$TARGETARCH" = "arm64" ]; then \
      wget https://github.com/protocolbuffers/protobuf/releases/download/v21.8/protoc-21.8-linux-aarch_64.zip -O protoc.zip; \
    else \
      echo "Unsupported architecture: $TARGETARCH"; \
      exit 1; \
    fi

  RUN cargo install refinery_cli --no-default-features --features postgresql
  RUN unzip protoc*.zip && mv bin/protoc /usr/local/bin

  # RUN cargo install wasm-pack

  # Call +INIT before copying the source file to avoid installing function depencies every time source code changes
  # This parametrization will be used in future calls to functions of the library
  DO rust+INIT --keep_fingerprints=true

source:
  FROM +install
  COPY --keep-ts Cargo.toml Cargo.lock ./
  COPY --keep-ts --dir arroyo-types arroyo-worker arroyo-rpc arroyo-macro arroyo-server-common \
    arroyo-state arroyo-metrics arroyo-compiler-service arroyo-connectors arroyo-api \
    arroyo-datastream arroyo-node arroyo-openapi arroyo-sql arroyo-sql-macro arroyo-sql-testing \
    arroyo-controller arroyo-console arroyo-storage arroyo-formats connector-schemas arroyo \
    copy-artifacts integ ./

lint:
  FROM +source
  RUN cargo fmt -- --check

db:
  FROM +lint
  ENV DATABASE_URL="postgres://arroyo:arroyo@localhost:5432/arroyo"
  RUN service postgresql start && \
     sudo -u postgres psql -c "CREATE USER arroyo WITH PASSWORD 'arroyo' SUPERUSER;" && \
     sudo -u postgres createdb arroyo && \
     /usr/local/cargo/bin/refinery migrate -e DATABASE_URL -p arroyo-api/migrations


build:
  FROM +db
  ENV DATABASE_USER=arroyo
  ENV DATABASE_PASSWORD=arroyo
  ENV DATABASE_HOST=localhost
  ENV DATABASE_NAME=arroyo
  CMD service postgresql start && bash
  DO rust+CARGO --args="build --release" --output="release/[^/\.]+"
  SAVE ARTIFACT ./target/release/ target AS LOCAL artifact/target
