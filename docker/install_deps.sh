#!/bin/env bash

set -e

# Install arch specific build dependencies
echo env
if [ "$(uname -m)" = "x86_64" ]; then
  export PROTO_ARCH="x86_64";
  export MOLD_ARCH="x86_64";
elif [ "$(uname -m)" = "aarch64" ]; then
  export PROTO_ARCH="aarch_64";
  export MOLD_ARCH="aarch64";
else
  echo "Unsupported architecture: $(uname -m)"
  exit 1;
fi

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
