#!/bin/env bash

set -e

# Install arch specific build dependencies
echo env
if [ "$(uname -m)" = "x86_64" ]; then
  export PROTO_ARCH="x86_64";
  export MOLD_ARCH="x86_64";
  export PROM_ARCH="amd64";
elif [ "$(uname -m)" = "aarch64" ]; then
  export PROTO_ARCH="aarch_64";
  export MOLD_ARCH="aarch64";
  export PROM_ARCH="arm64";
else
  echo "Unsupported architecture: $(uname -m)"
  exit 1;
fi

if [ "$1" = "build" ]; then
    # Install mold
    curl -OL https://github.com/rui314/mold/releases/download/v1.11.0/mold-1.11.0-${MOLD_ARCH}-linux.tar.gz
    tar xvfz mold*.tar.gz
    mv mold*-linux/bin/* /usr/bin
    mv mold*-linux/libexec/* /usr/libexec
    rm -rf mold*

    # Install protoc
    curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v21.8/protoc-21.8-linux-${PROTO_ARCH}.zip
    unzip protoc*.zip
    mv bin/protoc /usr/local/bin
    rm -rf protoc*.zip
elif [ "$1" = "run" ]; then
    # Install prometheus and pushgateway
    curl -OL https://github.com/prometheus/prometheus/releases/download/v2.49.1/prometheus-2.49.1.linux-${PROM_ARCH}.tar.gz
    tar xvfz prometheus*.tar.gz
    mv prometheus*/prometheus /usr/local/bin
    curl -OL https://github.com/prometheus/pushgateway/releases/download/v1.5.1/pushgateway-1.5.1.linux-${PROM_ARCH}.tar.gz
    tar xvfz pushgateway*.tar.gz
    mv pushgateway*/pushgateway /usr/local/bin
    rm -rf prometheus* pushgateway*
else
    echo "Invalid argument: $1"
    exit 1;
fi
