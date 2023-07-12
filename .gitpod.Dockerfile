FROM gitpod/workspace-postgres:2023-05-08-21-16-55

RUN sudo apt-get update 
RUN sudo apt-get install -y pkg-config build-essential libssl-dev openssl cmake curl
 
RUN wget https://github.com/protocolbuffers/protobuf/releases/download/v21.8/protoc-21.8-linux-x86_64.zip \
    && unzip protoc-21.8-linux-x86_64.zip \
    && sudo mv bin/protoc /usr/local/bin

RUN cargo install wasm-pack && cargo install refinery_cli

